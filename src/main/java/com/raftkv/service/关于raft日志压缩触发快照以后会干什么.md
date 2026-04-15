在你的 `RaftKVService` 配置中，设置了 `setSnapshotLogIndexMargin(10000)` 和 `setSnapshotIntervalSecs(3600)`。这意味着当满足“日志达到 1 万条”或“时间过去 1 小时”任一条件时，SOFAJRaft 会触发快照。

触发快照以后，整个系统会按顺序发生以下 **四个核心动作**：

### 1. 执行状态机保存 (`onSnapshotSave`)
这是你代码里定义的逻辑。
*   **数据打包：** JRaft 会调用 `KVStoreStateMachine.onSnapshotSave`。你的代码会锁住 `kvStore`，把内存里的所有 KV 数据和幂等请求缓存 (`processedRequests`) 序列化成一个 `machine-data.json` 文件。
*   **元数据关联：** JRaft 会自动记录这个快照对应的是 **哪一条日志索引（lastIncludedIndex）** 和 **哪一个任期（lastIncludedTerm）**。
*   **通知框架：** 当你调用 `done.run(Status.OK())` 时，JRaft 知道快照已经安全落盘。

### 2. 日志截断 (Log Truncation) —— **最重要的变化**
快照完成后，Raft 就不再需要那些旧日志了。
*   **物理删除：** JRaft 会检查磁盘上的 `log` 目录。由于 Index 1 到 `lastIncludedIndex` 的所有改动都已经反映在刚才生成的快照文件里了，JRaft 会**物理删除**这些旧的 WAL 日志文件。
*   **磁盘释放：** 这是防止磁盘被无限增长的日志撑爆的关键步骤。

### 3. 加速节点重启恢复
如果你现在重启节点，流程发生了质变：
*   **旧逻辑（无快照）：** 节点启动 $\rightarrow$ 从第 1 条日志开始重放 $\rightarrow$ 执行 10 万次 `onApply` $\rightarrow$ 恢复状态。这可能耗时几分钟。
*   **新逻辑（有快照）：** 节点启动 $\rightarrow$ 调用 `onSnapshotLoad` $\rightarrow$ 一次性把 `machine-data.json` 加载进内存 $\rightarrow$ **直接跳过前 10 万条日志** $\rightarrow$ 只重放快照之后产生的一丁点日志。启动通常在秒级完成。

### 4. 辅助追赶落后节点 (InstallSnapshot)
假设你的集群里有一个节点宕机了很久（比如断网了 3 天），此时：
*   **Leader 的日志已经删了：** Leader 因为做了快照，已经把 3 天前的旧日志删了，没法通过 `AppendEntries`（发日志）来同步。
*   **发送快照：** Leader 会发现这个 Follower 落后太多，于是通过网络把整个快照文件发给它（这就是 `InstallSnapshot` RPC）。
*   **Follower 恢复：** Follower 收到快照后，调用 `onSnapshotLoad` 瞬间追上进度，然后再开始同步剩下的实时日志。

---

### 总结：快照之后你的磁盘长什么样？

| 目录 | 快照前 | 快照后 |
| :--- | :--- | :--- |
| `log/` | 堆积了大量文件（例如 10G） | **大幅缩小**，只剩最新的日志（例如几百MB） |
| `snapshot/` | 可能为空或旧快照 | 出现一个新的文件夹，包含 `machine-data.json` |
| `raft_meta/` | 记录 lastApplied=100000 | 记录 lastIncludedIndex=100000 |

**一句话总结：触发快照后，你的内存数据被固化到了文件里，冗长的 WAL 日志被清空，系统完成了“瘦身”，并获得了一份快速重放的“存档”。**