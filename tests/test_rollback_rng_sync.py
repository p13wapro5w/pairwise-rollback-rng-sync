import pytest
import gameplay.rollback_rng_sync as mod

CASES = [
    ("deterministic_rng", (42, 7)),
    ("state_hash", (1, 2)),
    ("diverge_detect", (1, 2)),
    ("resync", (1, 2)),
    ("frame_advance", (7, 2)),
    ("seed_channel", (1, 2)),
    ("input_order", (1, 2)),
    ("rollback_depth", (8, 4)),
    ("replay_verify", (1, 2)),
    ("rng_stream_split", (1, 2)),
    ("hash_tolerance", (0, 2)),
    ("desync_policy", (1, 2)),
    ("snapshot_restore", (1, 2)),
    ("hit_order", (1, 2)),
    ("checksum_window", (3, 2)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
