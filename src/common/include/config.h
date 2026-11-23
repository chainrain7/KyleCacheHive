//
//

#ifndef CONFIG_H
#define CONFIG_H

// 调试日志开关：默认关闭，cmake -DKVRAFT_DEBUG=ON 打开（对应 -DKVRAFT_DEBUG 编译宏）
// 基准测试必须在日志关闭状态下运行
#ifndef KVRAFT_DEBUG
#define KVRAFT_DEBUG 0
#endif
const bool Debug = KVRAFT_DEBUG;

const int debugMul = 1;  // 时间单位：time.Millisecond，不同网络环境rpc速度不同，因此需要乘以一个系数
const int HeartBeatTimeout = 25 * debugMul;  // 心跳时间一般要比选举超时小一个数量级
const int ApplyInterval = 10 * debugMul;     //

const int minRandomizedElectionTime = 300 * debugMul;  // ms
const int maxRandomizedElectionTime = 500 * debugMul;  // ms

const int CONSENSUS_TIMEOUT = 500 * debugMul;  // ms

// 单次 AppendEntries 携带日志条数上限：原实现每次心跳全量重发剩余日志，
// 日志积压时单包体积和网络开销失控
const int MaxEntriesPerAppendEntries = 256;

// RPC 收发超时（SO_RCVTIMEO/SO_SNDTIMEO，毫秒）：
// 原实现 recv 无限期阻塞，对端 STOP/卡死时调用线程永久挂起（分区模拟实测死锁）
const int RpcRecvTimeoutMs = 1000;

// 协程相关设置

const int FIBER_THREAD_NUM = 1;              // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false;  // 是否使用caller_thread执行调度任务

#endif  // CONFIG_H
