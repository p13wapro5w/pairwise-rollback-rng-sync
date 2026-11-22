"""Deterministic rollback RNG and per-frame state synchronization.

Peer-to-peer fighting games run the same simulation on every client.  As soon
as a remote input arrives late the local simulation is rolled back and
re-simulated.  That only works when both peers derive *exactly* the same
random numbers for the same frame and when both hash the resulting simulation
state in exactly the same way.

This module contains the primitives used by such a rollback loop:

* :func:`deterministic_rng` / :func:`seed_channel` / :func:`rng_stream_split`
  split a single match seed into frame-addressable values without any hidden
  mutable state, so a rollback can re-derive a frame's randomness.
* :func:`state_hash` and :func:`checksum_window` produce stable, per-frame
  hashes.  ``blake2b`` is used instead of :func:`hash` because the built-in
  hash is salted per process (``PYTHONHASHSEED``) and would differ between
  peers.
* :func:`diverge_detect`, :func:`replay_verify`, :func:`hash_tolerance` and
  :func:`desync_policy` compare those hashes.
* :func:`resync` and :func:`snapshot_restore` perform the forced resync that
  snaps a diverged client back onto the authoritative state.
* :func:`frame_advance`, :func:`rollback_depth`, :func:`input_order` and
  :func:`hit_order` keep frame counters, rollback limits and simultaneous
  input/hit resolution deterministic.

Every helper is a pure function: the same arguments always produce the same
result on every peer, in every process and after any number of rollbacks.
"""

from __future__ import annotations

import copy
import hashlib
import struct
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

__all__ = [
    "MAX_ROLLBACK_FRAMES",
    "checksum_window",
    "desync_policy",
    "deterministic_rng",
    "diverge_detect",
    "frame_advance",
    "hash_tolerance",
    "hit_order",
    "input_order",
    "replay_verify",
    "resync",
    "rng_stream_split",
    "rollback_depth",
    "seed_channel",
    "snapshot_restore",
    "state_hash",
]

#: Rollback is cheap up to a point; past this many frames a full resync is
#: cheaper and avoids letting the two simulations drift apart.
MAX_ROLLBACK_FRAMES = 7

_MASK64 = (1 << 64) - 1
_GOLDEN_GAMMA = 0x9E3779B97F4A7C15

# Domain-separation tags: the same seed must not reuse a value across roles.
_RNG_TAG = b"rollback/rng/v1"
_CHANNEL_TAG = b"rollback/channel/v1"
_STREAM_TAG = b"rollback/stream/v1"
_HASH_TAG = b"rollback/state/v1"


# ---------------------------------------------------------------------------
# Deterministic hashing helpers
# ---------------------------------------------------------------------------


def _canonical_bytes(value: Any) -> bytes:
    """Encode ``value`` into an unambiguous, process-independent byte string.

    Unlike ``repr``, this never contains memory addresses, and ints, floats,
    bools and strings are type-tagged so that e.g. ``1``, ``True`` and ``"1"``
    cannot collide.
    """

    if value is None:
        return b"n"
    if isinstance(value, bool):
        return b"b1" if value else b"b0"
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii")
    if isinstance(value, float):
        # ``-0.0`` and ``0.0`` compare equal but are distinct bit patterns;
        # canonicalise so both peers agree, and keep NaN stable too.
        if value == 0.0:
            value = 0.0
        return b"f" + struct.pack(">d", value)
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return b"s" + struct.pack(">Q", len(raw)) + raw
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return b"y" + struct.pack(">Q", len(raw)) + raw
    if isinstance(value, Mapping):
        items = [(_canonical_bytes(k), _canonical_bytes(v)) for k, v in value.items()]
        items.sort(key=lambda pair: pair[0])
        out = bytearray(b"d" + struct.pack(">Q", len(items)))
        for key, val in items:
            out += struct.pack(">Q", len(key)) + key
            out += struct.pack(">Q", len(val)) + val
        return bytes(out)
    if isinstance(value, (set, frozenset)):
        encoded = sorted(_canonical_bytes(item) for item in value)
        out = bytearray(b"S" + struct.pack(">Q", len(encoded)))
        for item in encoded:
            out += struct.pack(">Q", len(item)) + item
        return bytes(out)
    if isinstance(value, Sequence):
        out = bytearray(b"l" + struct.pack(">Q", len(value)))
        for item in value:
            encoded = _canonical_bytes(item)
            out += struct.pack(">Q", len(encoded)) + encoded
        return bytes(out)

    raise TypeError(
        "state_hash cannot deterministically encode "
        f"{type(value).__name__!r}; pass primitives, containers or bytes"
    )


def _digest(*parts: Any) -> int:
    """Return a stable unsigned 64-bit digest of ``parts``."""

    hasher = hashlib.blake2b(digest_size=8, person=_HASH_TAG[:16])
    for part in parts:
        encoded = _canonical_bytes(part)
        hasher.update(struct.pack(">Q", len(encoded)))
        hasher.update(encoded)
    return int.from_bytes(hasher.digest(), "big")


def _splitmix64(value: int) -> int:
    """One SplitMix64 step: fast, well-distributed and deterministic."""

    value = (value + _GOLDEN_GAMMA) & _MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _MASK64
    return value ^ (value >> 31)


def _require_int(name: str, value: Any, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _as_tuple(values: Sequence[Any]) -> tuple[Any, ...]:
    """Accept either ``f(a, b)`` or ``f([a, b])`` without eating tuples."""

    if len(values) == 1 and not isinstance(values[0], Mapping):
        candidate = values[0]
        if isinstance(candidate, Iterable) and not isinstance(
            candidate, (str, bytes, bytearray)
        ):
            return tuple(candidate)
    return tuple(values)


# ---------------------------------------------------------------------------
# Deterministic RNG
# ---------------------------------------------------------------------------


def deterministic_rng(seed: int, frame: int, stream: int = 0) -> int:
    """Return the random value for ``frame`` of ``stream`` under ``seed``.

    The result depends only on the arguments, never on how many times this
    function was called before.  Replaying frame 120 after a rollback therefore
    yields the same value as the first simulation of frame 120, which is the
    core requirement for deterministic rollback.
    """

    seed = _require_int("seed", seed)
    frame = _require_int("frame", frame, minimum=0)
    stream = _require_int("stream", stream, minimum=0)

    state = _digest(_RNG_TAG, seed, stream, frame)

    # Two rounds of mixing remove the structure of the frame counter so that
    # adjacent frames and adjacent streams are not correlated.
    state = _splitmix64(state ^ (stream << 32))
    state = _splitmix64(state ^ frame)
    return state & _MASK64


def seed_channel(seed: int, channel: int) -> int:
    """Derive an independent root seed for ``channel`` from the match seed.

    Gameplay, cosmetic effects and network jitter must not consume each
    other's numbers; giving every channel its own root seed keeps a rollback
    on one channel from shifting the values seen by another.
    """

    seed = _require_int("seed", seed)
    channel = _require_int("channel", channel, minimum=0)

    mixed = _digest(_CHANNEL_TAG, seed, channel)
    return _splitmix64(mixed ^ (channel * _GOLDEN_GAMMA)) & _MASK64


def rng_stream_split(seed: int, streams: int) -> tuple[int, ...]:
    """Split ``seed`` into ``streams`` independent, deterministic stream seeds.

    The streams are pairwise distinct (this is checked) so that two subsystems
    can never accidentally share the same sequence, which would reintroduce
    the desync the split is meant to prevent.
    """

    seed = _require_int("seed", seed)
    streams = _require_int("streams", streams, minimum=0)

    derived: list[int] = []
    used: set[int] = set()
    for index in range(streams):
        candidate = _digest(_STREAM_TAG, seed, index)
        candidate = _splitmix64(candidate ^ (index * _GOLDEN_GAMMA))
        # Practically unreachable, but never hand out a duplicate stream.
        attempt = 0
        while candidate in used:
            attempt += 1
            candidate = _splitmix64(candidate ^ attempt)
        used.add(candidate)
        derived.append(candidate & _MASK64)
    return tuple(derived)


# ---------------------------------------------------------------------------
# Per-frame state hashes
# ---------------------------------------------------------------------------


def state_hash(frame: int, *state_parts: Any) -> int:
    """Hash the simulation state for ``frame``.

    ``state_parts`` are canonicalised with :func:`_canonical_bytes`, so
    mapping insertion order and set ordering cannot make two peers disagree.
    The frame number is part of the hash: a state that is correct but counted
    at the wrong frame is still a desync.
    """

    frame = _require_int("frame", frame, minimum=0)
    return _digest(_HASH_TAG, frame, state_parts)


def checksum_window(window: int, frame_count: int) -> tuple[int, int]:
    """Return the ``[start, end)`` frame range of the most recent checksums.

    ``window`` is clamped to the frames that actually exist, so a window
    larger than the current frame count (for example during the first few
    frames of a match) simply covers the whole match so far instead of
    reading past the start of the buffer.
    """

    window = _require_int("window", window, minimum=0)
    frame_count = _require_int("frame_count", frame_count, minimum=0)

    end = frame_count
    start = max(0, end - window)
    return (start, end)


# ---------------------------------------------------------------------------
# Divergence detection and forced resync
# ---------------------------------------------------------------------------


def diverge_detect(local_hash: int, remote_hash: int) -> bool:
    """Return ``True`` when the two peers hashed the same frame differently."""

    _require_int("local_hash", local_hash, minimum=0)
    _require_int("remote_hash", remote_hash, minimum=0)
    return local_hash != remote_hash


def replay_verify(local_hash: int, replay_hash: int) -> bool:
    """Return ``True`` when a replayed frame reproduces the recorded hash.

    A rollback re-simulates frames; this is the check that the re-simulation
    landed on exactly the state the original run had.
    """

    _require_int("local_hash", local_hash, minimum=0)
    _require_int("replay_hash", replay_hash, minimum=0)
    return local_hash == replay_hash


def hash_tolerance(tolerance: int, difference: int) -> bool:
    """Return whether ``difference`` is small enough to accept.

    ``tolerance`` is an absolute, non-negative bound; it defaults to exact
    matching when it is ``0``.  Divergence handling must not silently accept
    a large gap just because the hashes are "close".
    """

    tolerance = _require_int("tolerance", tolerance, minimum=0)
    difference = _require_int("difference", difference)
    return abs(difference) <= tolerance


def desync_policy(local_hash: int, remote_hash: int, *, depth: int = 0) -> str:
    """Choose the recovery action after comparing two frame hashes.

    Returns one of ``"continue"``, ``"resync"`` or ``"rollback"``.  Deep
    divergence can no longer be repaired by replaying inputs, so it escalates
    to a forced resync; shallow divergence is handled by a rollback.
    """

    _require_int("local_hash", local_hash, minimum=0)
    _require_int("remote_hash", remote_hash, minimum=0)
    depth = _require_int("depth", depth, minimum=0)

    if local_hash == remote_hash:
        return "continue"
    if depth > MAX_ROLLBACK_FRAMES:
        return "resync"
    return "rollback"


def resync(local_state: Any, remote_state: Any, *, frame: int | None = None) -> dict[str, Any]:
    """Force ``local_state`` onto the authoritative ``remote_state``.

    The remote peer's state wins.  The returned mapping records both sides so
    a caller can log the divergence, and carries a deep copy of the adopted
    state so subsequent simulation cannot alias the original snapshot.
    """

    if frame is not None:
        frame = _require_int("frame", frame, minimum=0)

    return {
        "action": "resync",
        "frame": frame,
        "adopted": copy.deepcopy(remote_state),
        "discarded": copy.deepcopy(local_state),
        "local_matches_remote": _safe_equal(local_state, remote_state),
    }


def _safe_equal(left: Any, right: Any) -> bool:
    """Equality that never raises, used only for diagnostics."""

    try:
        return bool(left == right)
    except Exception:  # pragma: no cover - diagnostic path only
        return False


def snapshot_restore(snapshot: Any, frame: int | None = None) -> Any:
    """Return an isolated copy of ``snapshot`` for re-simulation.

    Copying matters: restoring by reference would let the rolled-back
    simulation mutate the snapshot it was supposed to be restored *to*, and
    the next rollback would then replay already-modified state.

    When a mapping snapshot carries a ``frame`` key and an explicit ``frame``
    is supplied, the two must agree; restoring the wrong frame's snapshot is
    an immediate desync.
    """

    if frame is not None:
        frame = _require_int("frame", frame, minimum=0)
        if isinstance(snapshot, Mapping) and "frame" in snapshot:
            snapshot_frame = snapshot["frame"]
            if isinstance(snapshot_frame, int) and not isinstance(snapshot_frame, bool):
                if snapshot_frame != frame:
                    raise ValueError(
                        f"snapshot is for frame {snapshot_frame}, not frame {frame}"
                    )

    return copy.deepcopy(snapshot)


# ---------------------------------------------------------------------------
# Frame counters and deterministic ordering
# ---------------------------------------------------------------------------


def frame_advance(frame: int, steps: int = 1) -> int:
    """Advance (or, for negative ``steps``, roll back) the frame counter."""

    frame = _require_int("frame", frame)
    steps = _require_int("steps", steps)
    advanced = frame + steps
    if advanced < 0:
        raise ValueError(f"frame cannot advance to a negative value: {advanced}")
    return advanced


def rollback_depth(depth: int, max_depth: int = MAX_ROLLBACK_FRAMES) -> dict[str, Any]:
    """Classify how far a rollback has to rewind.

    Rollback stays cheap only for a handful of frames.  Once ``depth`` exceeds
    ``max_depth`` the simulation has drifted too far (character positions
    diverge, hit ordering flips) and a forced resync is cheaper and safer than
    replaying every frame.
    """

    depth = _require_int("depth", depth, minimum=0)
    max_depth = _require_int("max_depth", max_depth, minimum=0)

    within_limit = depth <= max_depth
    return {
        "depth": depth,
        "max_depth": max_depth,
        "within_limit": within_limit,
        "clamped_depth": min(depth, max_depth),
        "overflow": max(0, depth - max_depth),
        "requires_resync": not within_limit,
    }


def _field(item: Mapping[Any, Any], *names: str, default: int = 0) -> int:
    """Read the first integer-valued field among ``names``.

    Missing, boolean or non-integer values fall back to ``default`` so a
    malformed record cannot make peers sort it differently.
    """

    for name in names:
        value = item.get(name, default)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return default


def _record_key(item: Any, *, hit: bool) -> tuple[Any, ...]:
    """Deterministic sort key for an input or hit record.

    The gameplay fields come first and the canonical encoding is the final
    tie-breaker, so the ordering is total and never depends on mapping
    iteration order or on when the record arrived from the network.

    Inputs are ordered by the player who supplied them; hits are ordered by
    descending priority (a heavier hit resolves first) and only then by the
    attacker.
    """

    if isinstance(item, Mapping):
        frame = _field(item, "frame")
        if hit:
            return (
                0,
                frame,
                -_field(item, "priority"),
                _field(item, "attacker", "player"),
                _field(item, "sequence", "seq"),
                _canonical_bytes(item),
            )
        return (
            0,
            frame,
            _field(item, "player"),
            _field(item, "sequence", "seq"),
            _canonical_bytes(item),
        )

    # Non-mapping records have no gameplay fields; order them after mapping
    # records and use the canonical encoding so the key stays total.
    return (1, _canonical_bytes(item))


def input_order(*inputs: Any) -> tuple[Any, ...]:
    """Return simultaneous inputs in a canonical, peer-identical order.

    Inputs can be passed variadically (``input_order(a, b)``) or as a single
    iterable.  Sorting by ``(frame, player, sequence)`` means a rollback
    replay resolves the same pair of inputs in the same order even if they
    arrived from the network in a different order.
    """

    items = _as_tuple(inputs)
    return tuple(sorted(items, key=lambda item: _record_key(item, hit=False)))


def hit_order(*hits: Any) -> tuple[Any, ...]:
    """Return overlapping hit events in a canonical, peer-identical order.

    Hit order decides damage, trades and counter-hits, so it must be a pure
    function of the events themselves and never of dictionary iteration order
    or arrival time.  Higher ``priority`` resolves first; remaining ties are
    broken by attacker, sequence and finally the canonical encoding.
    """

    items = _as_tuple(hits)
    return tuple(sorted(items, key=lambda item: _record_key(item, hit=True)))
