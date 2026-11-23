# KyleCacheHive

基于 Raft 共识算法的分布式键值存储系统（C++20）。多节点组成集群对外表现为一个强一致的 KV 数据库，少数节点故障不影响整体可用性；读请求走 ReadIndex 线性一致读，写请求经 pipelined 日志复制达成多数派提交。

> 本项目基于开源项目 KVstorageBaseRaft-cpp 深度改造，改造范围与来源见文末「项目来源」。

```
client(clerk) ──Get/Put/Append──▶ KvServer(RPC) ──状态机──▶ SkipList
                                     │ applyCh
                                   Raft ◀──AppendEntries/RequestVote/InstallSnapshot──▶ Raft
                                     │                                          (peer 间 RPC, protobuf)
                                  Persister（单文件原子持久化：raftstate + snapshot）
```

## 核心特性

**共识层（Raft）**
- Leader 选举 + 日志复制 + 心跳，选举超时 300~500ms 随机化
- **PreVote 预投票**：两阶段选举（预投票不增 term 不落盘），被分区/反复重启的节点不会用膨胀 term 干扰健康集群
- **ReadIndex 线性一致读**（raft thesis §6.4）：读请求不写 raft 日志——leader 心跳确认多数派身份后按 commitIndex 位点读本地状态机，纯读吞吐提升约 85 倍
- **Pipelined 日志复制**：新日志提交后立即复制（不等心跳 tick）+ ack 链式续发 + per-peer 在途管理（epoch 失效旧任期返回）；提交判定为 matchIndex 多数派扫描
- **Leader no-op 条目**（etcd/TiKV 实践）：当选即追加空操作，保证新 term 能间接提交历史日志
- 日志压缩与 InstallSnapshot 快照同步（单次 AE 携带条数上限 256）

**RPC 层**
- protobuf 协议 + varint 长度头 + muduo 网络库多线程服务端
- 长连接复用、per-channel 互斥、**收发超时**（SO_RCVTIMEO/SNDTIMEO）与部分写处理

**存储层**
- 跳表状态机（共享锁 + thread_local 随机层数引擎）
- 单文件原子持久化：`[8B raftstate长度][raftstate][snapshot]` 成对原子落盘（临时文件 + fsync + rename），崩溃不会出现"截断日志 + 旧快照"的不一致对
- clientId + requestId 幂等去重，保证客户端重试不重复执行

## 性能

3 节点集群（WSL2 回环），4 并发客户端线程，key 空间 100，日志关闭；混合负载（80% 读 / 20% 写）15s × 3 轮取中位，纯读 / 纯写为 10s 单轮：

| 负载 | QPS | P50 | P99 |
|---|---|---|---|
| 混合 80/20 | **1,299** | 3.1 ms | 9.5 ms |
| 纯读（ReadIndex） | **14,328** | 0.26 ms | 0.62 ms |
| 纯写（共识提交） | **362** | 10.9 ms | 15.8 ms |

作为对照，原项目实现（读写均走 raft 日志共识）在同条件下混合负载吞吐约 169 QPS——ReadIndex 使读吞吐提升约 85 倍，pipeline 复制使混合负载提升 7.7 倍。

## 可靠性验证（均为可复现实验）

- **崩溃恢复**：`kill -9` 全集群 → 重启 → 已确认写入 10/10 完整恢复（依赖单文件原子持久化 + leader no-op 条目间接提交历史日志）
- **分区模拟**：`kill -STOP` 冻结单节点约 10s → 其余节点持续服务（RPC 超时重试）→ 被隔离节点 term 不膨胀（PreVote 生效）→ 解冻后数据完整
- **读不写日志**：100 个纯 Get 前后持久化文件字节不变
- 500 轮顺序写读回归全过，压测零失败、零异常退出

## 快速开始

依赖：Linux（muduo/boost/protobuf，仅 Linux 编译）、CMake ≥ 3.22、C++20。

```bash
mkdir -p build && cd build && cmake .. && make -j$(nproc)   # 调试日志: cmake -DKVRAFT_DEBUG=ON ..
cp -r ../bin ~/run && cd ~/run && rm -f persist*.txt

./raftCoreRun -n 3 -f test.conf                    # 终端1：启动 3 节点集群（fork 子进程）
./benchMain -t 4 -d 15 -g 80 -k 100 -f test.conf   # 终端2：混合负载压测（QPS/P50/P99）

# 崩溃恢复演示
./kvRecoverTest put
pkill -9 raftCoreRun && ./raftCoreRun -n 3 -f test.conf
./kvRecoverTest get   # expect: PASS (10/10)
```

## 目录结构

```
├── src/
│   ├── raftCore/      # Raft 共识 + KvServer 状态机层 + 持久化
│   ├── raftRpcPro/    # raft 节点间 RPC 的 protobuf 定义
│   ├── raftClerk/     # 客户端（自动寻主 + 重试 + 幂等）
│   ├── rpc/           # RPC 框架（protobuf + muduo，varint 头 + 超时 + 长连接）
│   ├── skipList/      # 跳表 KV 引擎（状态机存储）
│   ├── fiber/         # ucontext 协程库（ticker 定时任务）
│   └── common/        # 配置、工具、阻塞队列
├── example/
│   ├── raftCoreExample/   # 集群启动器 / 压测 / 崩溃恢复验证客户端
│   └── rpcExample/        # RPC 框架独立示例
├── docs/ROADMAP.md    # 后续优化方向（已定位到文件:行号）
└── bin/               # 编译产物
```

## 设计取舍与已知限制

- RPC 为同步调用（无异步/流式），响应单次 recv 上限 4KB——完整协议分帧在 ROADMAP
- 跳表为整表共享锁，细粒度/无锁读在 ROADMAP
- 未实现成员变更与 Leader Lease（ROADMAP 中均已定位落点）
- 服务发现为配置文件 + 启动同步，非注册中心方案

## 项目来源

本项目基于开源项目 [KVstorageBaseRaft-cpp](https://github.com/youngyangyang04/KVstorageBaseRaft-cpp)（作者 youngyangyang04）深度改造而来。原项目提供了 Raft 共识、RPC 框架、跳表与协程库的基础实现；本项目在其之上完成的工作包括：

- 修复 12 处正确性缺陷（持久化读回失败与双文件非原子导致的丢数据、快照边界崩溃分支、RPC 并发帧交错、use-after-free、clientId 重复导致的丢写等），并将持久化层重写为单文件原子配对格式
- 实现 PreVote 预投票、ReadIndex 线性一致读、写路径 pipeline 复制、leader no-op 条目
- RPC 收发超时与部分写处理；applier 由固定轮询改为条件变量通知
- 新增压测工具 `benchMain` 与崩溃恢复验证工具 `kvRecoverTest`

上文性能与可靠性数据均为本项目改造后的实测结果，对照基线为原项目实现。原项目未附带开源许可证，本项目仅用于个人学习与作品展示。

详细优化路线见 [docs/ROADMAP.md](docs/ROADMAP.md)。
