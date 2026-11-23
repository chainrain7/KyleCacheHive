# ROADMAP — 后续优化方向（本轮未做，均有分析落脚点）

> 2026-11-20 已完成：第一轮（12 项正确性修复 + PreVote + ReadIndex）+ 第二轮（**RPC 收发超时** + **写路径 pipeline + no-op leader entry** + applier 条件变量 + 快照阈值修正）。
> 以下方向经代码分析已定位到具体文件:行号，作为后续迭代路线。

## 一、Raft 协议继续深化

### 1. 成员变更（单节点增删 / joint consensus）
- 现状：`m_peers` 在 `init` 后固定（raft.cpp init），多数派按初始集群规模硬算
- 方案：单服务器成员变更（单步）实现简单、面试讲得清；proto 增加 AddServer/RemoveServer RPC
- 价值：简历加分项，体现对配置变更安全性的理解

### 2. Leader Lease / CheckQuorum
- 现状：ReadIndex 每次读都发心跳确认（100ms 窗口），租约化后可省掉确认轮次
- 方案：leader 记录多数派最近确认时间，租约期内直接服务读；配合 resign-on-lease-expire
- 价值：纯读 QPS 可再提升（当前 14.3k 主要耗在确认轮 RTT）

## 二、RPC 框架

### 1. 协议分帧
- 现状：服务端 `retrieveAllAsString` 一次解析（半包丢请求，rpcprovider.cpp OnMessage）；客户端单次 recv 4096 字节上限（mprpcchannel.cpp）
- 方案：muduo Buffer 循环解析 + 客户端按长度前缀循环 recv；响应也加长度前缀

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

### 3. WAL 与 fsync 策略
- 现状：Raft persist 与 KV 快照共用一个文件全量重写（正确性优先的方案）
- 方案：KV 层独立 WAL 追加写 + 定期 compaction；fsync 策略可配（always/interval/never）

## 四、测试与可观测性

### 1. RPC 故障注入 + 混沌测试
- 现状：无网络分区/延迟/丢包注入能力（STOP 模拟会冻死客户端依赖 RPC 超时，见 RPC#1）
- 方案：raftRpcUtil 层加可控代理（延迟/丢弃/分区开关），写 6.824 风格 tester（crash-restart、one-third 分区、乱序 RPC）
- 价值：有了它才能做 Jepsen 级验证叙事

### 2. 单元测试
- 现状：test/ 目录是手动试验代码，未接入 CMake
- 方案：gtest 接入构建，跳表/raft 各模块单测；TSAN/ASAN CI

### 3. 可观测性
- 现状：DPrintf 是同步 printf，无 metrics
- 方案：异步分级日志（spdlog 或自研队列——common/util.h 的 LockQueue 本来就是为日志队列设计的，注释还写着 like go chan）；选举次数/commit 速率/RPC RTT 直方图暴露

## 五、工程化小项

- CMake 重写：去掉全部 aux_source_directory 与变量错位（src/skipList 的 CMakeLists 把源文件 set 成 src_raftCore 等），Debug 固定改为可配，加 -Wall -Wextra
- 清理死代码：kvServer.h 的 m_kvDB 旧状态机、fiber 库的 Semaphore 空声明、两套 defer 重复实现
- 启动去 sleep：kvServer 构造的 sleep(6)/sleep(n-me) 改为配置文件就绪协议或重试连接
- 原项目遗留：ticker 协程无锁读 m_status（raft.cpp electionTimeOutTicker/leaderHearBeatTicker）→ 改 std::atomic
