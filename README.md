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
- **PreVote 预投票**：两阶段选举。候选人先用「下一个任期」试探多数派，拿到多数派预投票才真正增加任期、落盘并发起选举，被分区或反复重启的节点不会用膨胀的 term 干扰健康集群。预投票记录与正式投票记录（votedFor）相互独立，所以 leader 宕机后其余节点仍然选得出新 leader
- **ReadIndex 线性一致读**（raft thesis §6.4）：读请求不写 raft 日志。leader 心跳确认多数派身份后按 commitIndex 位点读本地状态机，单次读不再需要一轮日志共识
- **Pipelined 日志复制**：新日志提交后立即复制（不等心跳 tick）+ ack 链式续发 + per-peer 在途管理（epoch 失效旧任期返回）；提交判定为 matchIndex 多数派扫描
- **Leader no-op 条目**（etcd/TiKV 实践）：当选即追加空操作，保证新 term 能间接提交历史日志
- 日志压缩与 InstallSnapshot 快照同步：单次 AE 最多携带 256 条；落后的节点自动改走快照，同一节点同时至多一个快照在途，快照装完立即接着复制后续日志

**RPC 层**
- protobuf + muduo 网络库多线程服务端
- **协议分帧**：请求、响应都带 4 字节长度前缀，按长度收满再解析，支持超过单个 TCP 段的大消息（已验证 300KB 的值、207KB 的快照）
- 长连接复用、per-channel 互斥、收发超时（SO_RCVTIMEO/SNDTIMEO）与部分写处理

**存储层**
- 跳表状态机（共享锁 + thread_local 随机层数引擎）
- 单文件原子持久化：`[8B raftstate长度][raftstate][snapshot]` 成对原子落盘（临时文件 + fsync + rename），崩溃不会出现「截断日志 + 旧快照」的不一致对
- clientId + requestId 幂等去重，保证客户端重试不重复执行

## 性能

3 节点集群（WSL2 Ubuntu 22.04，回环网络），4 并发客户端线程，key 空间 100（已预先写满），日志关闭，每种负载 3 轮 × 10 秒取中位。`bash scripts/bench.sh` 可复现。测试时磁盘一次落盘（含两次 fsync）中位 1.7ms。

| 负载 | QPS | P50 | P99 |
|---|---|---|---|
| 混合 80% 读 / 20% 写 | **1,285** | 3.09 ms | 9.50 ms |
| 纯读（ReadIndex） | **13,407** | 0.28 ms | 0.68 ms |
| 纯写（共识提交） | **370** | 10.7 ms | 14.8 ms |

作为对照，原项目实现的读写都走 raft 日志共识，在同一台机器上混合负载吞吐约 169 QPS（2026-11-20 测得）：混合负载约 7.6 倍，纯读按此推算约 80 倍。

纯写的延迟基本由磁盘决定：每次写要同步落盘两轮，P50 约为一次落盘耗时的 2 倍。磁盘忙的时候（fsync 中位 5.9ms）同样的代码纯写延迟会到单线程 11ms、双线程 21ms，所以写性能数字要连同 fsync 延迟一起看。

## 可靠性验证

以下各项都由 `scripts/regress.sh` 覆盖，在 Linux 上一条命令即可复现（判读标准见 [scripts/README.md](scripts/README.md)）：

- **leader 故障转移**：压测进行中 `kill -9` leader，其余节点选出新 leader，写入最长中断 0.4~0.5 秒（约一个选举超时），零失败
- **掉队节点通过快照追上**：`kill -9` leader → 故障转移后继续写入并触发快照 → 重启全集群，旧 leader 通过快照加后续日志追平。快照 2KB / 58KB / 207KB 三档均验证，三个节点的快照与日志完全一致
- **分区愈合**：`kill -STOP` 冻结 leader 期间触发快照，解冻后旧 leader 退位并在 1 秒内追平
- **崩溃恢复**：`kill -9` 全集群 → 重启，已确认写入 10/10 完整恢复（单文件原子持久化 + leader no-op 条目间接提交历史日志）
- **大值**：1KB / 64KB / 300KB 的值 put 后 get 一致
- **内存**：15 秒 16 万次纯读，leader 常驻内存增长约 2MB（修复每个 RPC 泄漏请求、响应消息之前约 61MB）
- 500 轮顺序写读回归全过；压测零失败

## 快速开始

依赖：Linux（muduo/boost/protobuf，仅 Linux 编译）、CMake ≥ 3.22、C++20。

```bash
mkdir -p build && cd build && cmake .. && make -j$(nproc)   # 调试日志: cmake -DKVRAFT_DEBUG=ON ..
cd .. && bash scripts/regress.sh                            # 集群回归测试（约 8~10 分钟）
bash scripts/bench.sh                                       # 性能测试

# 手动跑一遍：把 bin 拷到原生 Linux 文件系统下的目录再执行
cp -r bin ~/run && cd ~/run && rm -f persist*.txt
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
│   ├── rpc/           # RPC 框架（protobuf + muduo，分帧 + 超时 + 长连接）
│   ├── skipList/      # 跳表 KV 引擎（状态机存储）
│   ├── fiber/         # ucontext 协程库（ticker 定时任务）
│   └── common/        # 配置、工具、阻塞队列
├── example/
│   ├── raftCoreExample/   # 集群启动器 / 压测 / 崩溃恢复与大值验证客户端
│   └── rpcExample/        # RPC 框架独立示例
├── scripts/           # 集群回归测试、性能测试、持久化文件解析、fsync 探针
├── docs/ROADMAP.md    # 后续优化方向
└── bin/               # 编译产物
```

## 设计取舍与已知限制

- RPC 为同步调用（无异步/流式）；handler 在 muduo 的 IO 线程里同步阻塞等待共识完成，单节点的并发处理能力受 IO 线程数限制
- 每次写都同步落盘，而且 leader 是持着 raft 锁落的：整份 raft state 全量重写加两次 fsync，写吞吐的上限约为「1 / 持锁落盘耗时」
- 跳表为整表共享锁
- 未实现成员变更与 Leader Lease
- 服务发现为配置文件 + 启动同步，非注册中心方案

后续方向见 [docs/ROADMAP.md](docs/ROADMAP.md)。

## 项目来源

本项目基于开源项目 [KVstorageBaseRaft-cpp](https://github.com/youngyangyang04/KVstorageBaseRaft-cpp)（作者 youngyangyang04）深度改造而来。原项目提供了 Raft 共识、RPC 框架、跳表与协程库的基础实现；本项目在其之上完成的工作包括：

**正确性修复**
- 持久化读回失败与双文件非原子导致的丢数据、快照边界崩溃分支、RPC 并发帧交错、use-after-free、clientId 重复导致的丢写等 12 处缺陷；持久化层重写为单文件原子配对格式
- RPC 协议分帧：原协议没有长度前缀，超过约 64KB 的快照传不过去，字段全为默认值的空响应客户端收不到，单个值最大只能约 4KB
- 服务端每个 RPC 泄漏请求、响应两个消息对象（15 秒纯读 leader 内存增长约 61MB）

**功能与性能**
- PreVote 预投票、ReadIndex 线性一致读、写路径 pipeline 复制、leader no-op 条目
- RPC 收发超时与部分写处理；applier 由固定轮询改为条件变量通知
- 快照同步的在途保护与「快照装完立即续发日志」

**测试工具**
- `benchMain` 压测、`kvRecoverTest` 崩溃恢复验证、`kvBigValueTest` 大值验证
- `scripts/regress.sh` 集群回归（故障转移、快照追赶、分区愈合、大值、内存）与 `scripts/bench.sh` 性能测试

上文性能与可靠性数据均为本项目改造后的实测结果。原项目未附带开源许可证，本项目仅用于个人学习与作品展示。
