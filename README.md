# pairwise-rollback-rng-sync

## 领域

游戏、图形与多媒体

## 主题

回滚随机同步

## 说明

确定性 RNG、逐帧状态哈希与分歧重同步。

初始状态下 `gameplay/rollback_rng_sync.py` 中所有函数抛出 `NotImplementedError`，测试全部失败。
修复实现后运行：

```bash
python -m pytest -q
```

期望全部通过。
