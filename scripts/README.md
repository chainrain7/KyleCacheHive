# scripts/

集群回归测试与诊断脚本。全部在 **Linux** 下运行（项目依赖 muduo）。

## regress.sh —— 集群回归测试

```bash
mkdir -p build && cd build && cmake .. && make -j$(nproc) && cd ..   # 先编译出 bin/
bash scripts/regress.sh            # 跑全部（约 8~10 分钟，其中 t8 固定占 50 秒、t3 要写到 200KB 快照）
bash scripts/regress.sh t6 t4      # 只跑指定用例
```

脚本会把 `bin/` 里的可执行文件拷到 `WORK` 目录（默认 `~/kvcachehive-regress`）再运行，
`WORK` 请放在原生 Linux 文件系统上，不要放到 `/mnt/*` 这类跨系统挂载盘。

| 用例 | 验证内容 | 通过标准 |
|---|---|---|
| t1 / t2 / t3 | kill -9 当前 leader → 故障转移 → 持续写入直到快照达到 1KB / 50KB / 200KB → 重启全集群 | 旧 leader 追平：三节点 `SAME(snapshot)=True` 且 `SAME(nlogs)=True`，`PASS (10/10)`，`parse_error=0` |
| t4 | kill -STOP 冻结 leader（模拟分区）→ 触发快照 → 解冻 | 旧 leader 解冻后退位并在 1~2 秒内追平 |
| t5 | 500 轮顺序写读、混合压测、kill -9 全集群后重启恢复 | `success=500`、`fail=0`、`PASS (10/10)` |
| t6 | 压测进行中 kill -9 leader | 压测正常结束、`fail=0`，`max_ms` 约等于一次选举超时 |
| t7 | 1KB / 64KB / 300KB 的值 put + get | `[BIGVALUE] PASS` |
| t8 | RPC 框架独立冒烟（rpcExample） | `success=10/10` |
| t9 | 15 秒纯读前后三个节点的 RSS | `rss_delta_kb` 为个位数到低两位数的 MB 级以下增长（修复前 leader 是 +61000） |

说明：

- t1~t4 用「一直写到快照达标」而不是固定时长，因为写入速率取决于磁盘 fsync 延迟（见 `fsync_probe.py`），固定时长在不同机器上不一定触发快照。
- t3 的快照超过单个 TCP 段（64KB），用来验证 RPC 分帧；去掉分帧后该用例会失败（旧 leader 停在 `snapIdx=0`，并出现 `parse error`）。
- 测试会 `pkill -9 raftCoreRun` 等进程名，不要和其它正在运行的同名程序混用。

## readstate.py —— 解析持久化文件

`persist{节点号}.txt` 的格式是 `[8 字节 raftstate 长度][raftstate][snapshot]`，raftstate 是 boost text archive。

```bash
python3 scripts/readstate.py persist0.txt persist1.txt persist2.txt            # 逐个打印 term / votedFor / snapIdx / 日志条数
python3 scripts/readstate.py persist0.txt persist1.txt persist2.txt --leader   # 三个节点都投了同一个节点时，该节点即 leader
python3 scripts/readstate.py persist0.txt persist1.txt persist2.txt --same     # 三个节点的快照、日志条数是否一致
python3 scripts/readstate.py persist0.txt persist1.txt --atleast 200000        # 快照是否都达到指定字节数
```

## fsync_probe.py —— 磁盘 fsync 延迟

```bash
python3 scripts/fsync_probe.py
```

按持久化层的落盘模式（写临时文件 → fsync → rename → fsync 目录）测一次落盘的耗时。
**写路径的性能数字必须在同一个 fsync 延迟下比较**：每次写要经过两轮落盘，
fsync 中位数从 1.8ms 变到 5.9ms 时，纯写 P50 会从约 4ms 变到约 11ms，这不是代码变化造成的。
