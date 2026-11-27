# ROADMAP — 后续优化方向（本轮未做，均有分析落脚点）

> 已完成：
> - 第一轮（2026-11-20）：12 项正确性修复 + PreVote + ReadIndex
> - 第二轮（2026-11-20）：RPC 收发超时 + 写路径 pipeline + no-op leader entry + applier 条件变量 + 快照阈值修正
> - 第三轮（2026-11-27）：快照同步与 PreVote 故障转移的回归修复、**RPC 协议分帧**（两个方向）、每请求内存泄漏修复、`scripts/regress.sh` 集群回归 + `scripts/bench.sh` 性能测试
>
> 以下方向经代码分析已定位到具体位置，作为后续迭代路线。

## 一、Raft 协议继续深化

### 1. 成员变更（单节点增删 / joint consensus）
- 现状：`m_peers` 在 `init` 后固定（raft.cpp init），多数派按初始集群规模硬算
- 方案：单服务器成员变更（单步）实现简单、面试讲得清；proto 增加 AddServer/RemoveServer RPC
- 价值：简历加分项，体现对配置变更安全性的理解

### 2. Leader Lease / CheckQuorum
- 现状：ReadIndex 每次读都发心跳确认（100ms 窗口），租约化后可省掉确认轮次
- 方案：leader 记录多数派最近确认时间，租约期内直接服务读；配合 resign-on-lease-expire
- 价值：纯读 QPS 可再提升（当前约 13.4k，每个读要给每个 follower 起一个线程并发一轮心跳确认，主要耗在确认轮往返上）

## 二、RPC 框架

### 1. handler 分发到业务线程池
- 现状：`RpcProvider::HandleFrame` 在 muduo 的 IO 线程里直接同步调用 handler（rpcprovider.cpp），而 `KvServer::Get/PutAppend` 会阻塞等待共识完成（最长 500ms）。一个 handler 阻塞期间，落在同一个 IO 线程上的其它连接都得不到处理；节点解冻时这种排队会让第一个快照请求的应答迟到、超过对端 1 秒超时
- 方案：IO 线程只做收发和解析，handler 投递到业务线程池执行（muduo 自带 ThreadPool），响应通过 `runInLoop` 回到连接所在的 IO 线程发送

### 2. 协程化 RPC（让自带 fiber 库真正落地）
- 现状：心跳/选举/快照/pipeline AE fan-out 是 `std::thread + detach`（每 peer 每轮一个线程，有在途上限但仍是线程），自研协程库只用于 ticker 定时
- 方案：RPC 收发跑进 IOManager 协程（hook connect/read），fan-out 从线程改为协程并发
- 价值：简历上"自研协程库 + 协程化 RPC"形成闭环故事

## 三、存储引擎

### 1. 跳表并发
- 现状：整表一把 `std::mutex _mtx`（skipList.h），读不加锁有数据竞争隐患（上层 kvServer 锁兜底）
- 方案：`std::shared_mutex` 读写锁 → 每节点 mark-delete + epoch 回收（对标 Redis/LevelDB memtable）
- 顺带：层数概率参数化（p=0.25）、析构改迭代（现递归有栈溢出风险）

### 2. 序列化
- 现状：跳表 dump/日志 Op 全走 boost text archive（文本格式，体积大、慢）
- 方案：换 protobuf 或自定义二进制；快照体积可直接砍半以上

### 3. 落盘移出 raft 锁 + group commit / WAL
- 现状：每写一条都把整份 raft state 重新序列化、全量重写文件、fsync 两次，leader 是持着 `m_mtx` 做的（raft.cpp `Raft::persist`），follower 收到日志后也是落盘完才回包
- 实测：一次落盘（含两次 fsync）中位 1.7ms 时纯写 P50 约 10.7ms、370 QPS；fsync 中位 5.9ms 时单线程约 11ms、双线程约 21ms。写 P50 基本等于两轮落盘，持锁落盘还挡住了同一时期的提交和 apply
- 方案：日志改成追加写（写放大从「日志长度」降到 O(1)）；落盘移出 raft 锁；把一段时间内的多次写合并成一次 fsync（group commit），fsync 策略可配（always / interval / never）

## 四、测试与可观测性

### 1. RPC 故障注入 + 混沌测试
- 现状：无网络延迟/丢包/乱序注入能力。现有的故障手段只有 `kill -9`（进程崩溃）和 `kill -STOP`（整个进程冻结，见 `scripts/regress.sh` 的 t4）
- 方案：raftRpcUtil 层加可控代理（延迟/丢弃/分区开关），写 6.824 风格 tester（crash-restart、one-third 分区、乱序 RPC）
- 价值：有了它才能做 Jepsen 级验证叙事

### 2. 单元测试与 CI
- 现状：集群级回归已有 `scripts/regress.sh`（故障转移、快照追赶、分区愈合、大值、内存 9 个场景），但没有接进 CTest，也没有 CI；`test/` 目录仍是零散的手动试验代码
- 方案：gtest 接入构建（跳表、Persister、Raft 状态机单测），regress.sh 接成 CTest；GitHub Actions 里构建 muduo 后跑单测 + 回归；TSAN/ASAN（`raft.cpp` 的 electionTimeOutTicker / leaderHearBeatTicker 仍在无锁读 `m_status`）

### 3. 可观测性
- 现状：DPrintf 是同步 printf，无 metrics
- 方案：异步分级日志（spdlog 或自研队列——common/util.h 的 LockQueue 本来就是为日志队列设计的，注释还写着 like go chan）；选举次数/commit 速率/RPC RTT 直方图暴露

## 五、工程化小项

- CMake 重写：去掉全部 aux_source_directory 与变量错位（src/skipList 的 CMakeLists 把源文件 set 成 src_raftCore 等），Debug 固定改为可配，加 -Wall -Wextra
- 清理死代码：kvServer.h 的 m_kvDB 旧状态机、fiber 库的 Semaphore 空声明、两套 defer 重复实现
- 启动去 sleep：kvServer 构造的 sleep(6)/sleep(n-me) 改为配置文件就绪协议或重试连接
- 原项目遗留：ticker 协程无锁读 m_status（raft.cpp electionTimeOutTicker/leaderHearBeatTicker）→ 改 std::atomic
- 每次 RPC 一个 detached 线程：选举、心跳、快照、读确认都是现起线程（raft.cpp 里 7 处），改成固定线程池或协程
- `Raft::Snapshot` 与 InstallSnapshot 接收端直接调 `Persister::Save`，绕过了 `persist()` 的状态签名去重，下一次 `persist()` 会多做一次无谓的全量写盘
- InstallSnapshot 接收端只和快照边界比较，没有和 lastApplied 比较；当前实现靠「加载快照时不清理跳表」避免了状态机回退，语义上不够干净
