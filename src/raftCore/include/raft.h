#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include "ApplyMsg.h"
#include "Persister.h"
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
#include "config.h"
#include "monsoon.h"
#include "raftRpcUtil.h"
#include "util.h"
/// @brief //////////// 网络状态表示  todo：可以在rpc中删除该字段，实际生产中是用不到的.
constexpr int Disconnected =
    0;  // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

///////////////投票状态

constexpr int Killed = 0;
constexpr int Voted = 1;   //本轮已经投过票了
constexpr int Expire = 2;  //投票（消息、竞选者）过期
constexpr int Normal = 3;

class Raft : public raftRpcProctoc::raftRpc {
 private:
  std::mutex m_mtx;
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;
  std::shared_ptr<Persister> m_persister;
  int m_me;
  int m_currentTerm;
  int m_votedFor;
  std::vector<raftRpcProctoc::LogEntry> m_logs;  //// 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
                                                 // 这两个状态所有结点都在维护，易失
  int m_commitIndex;
  int m_lastApplied;  // 已经汇报给状态机（上层应用）的log 的index

  // 这两个状态是由服务器来维护，易失
  std::vector<int>
      m_nextIndex;  // 这两个状态的下标1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
  std::vector<int> m_matchIndex;
  // 写路径 pipeline 的在途管理：in-flight = 该 peer 上一轮 AE 尚未返回；
  // pending = 在途期间又有新日志/回缩需要再发，由在途回来后的 finishInflight 补发
  std::vector<bool> m_aeInflight;
  std::vector<bool> m_aePending;
  // AE 轮次号：新任期开始时自增，旧任期的在途 AE 返回后凭 epoch 失效，
  // 防止误清/误触发新任期的在途状态
  std::vector<int> m_aeEpoch;
  enum Status { Follower, Candidate, Leader };
  // 身份
  Status m_status;

  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;  // client从这里取日志（2B），client与raft通信的接口
  // ApplyMsgQueue chan ApplyMsg // raft内部使用的chan，applyChan是用于和服务层交互，最后好像没用上

  // commitIndex 推进时唤醒 applierTicker（替代原来的 10ms 轮询，
  // 否则写路径延迟下限被轮询间隔卡住，pipeline 的提速会被吃掉一半）
  std::condition_variable m_commitCv;

  // 选举超时

  std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
  // 心跳超时，用于leader
  std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

  // 2D中用于传入快照点
  // 储存了快照中的最后一个日志的Index和Term
  int m_lastSnapshotIncludeIndex;
  int m_lastSnapshotIncludeTerm;

  // 协程
  std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

  // 上次落盘的状态签名：(term, votedFor, 快照Index, 快照Term, 日志条数, 末条日志Index, 末条日志Term)
  // persist() 用它跳过无状态变化的落盘：心跳 AE 与被拒投票不再触发全量序列化重写（消除 IO 放大）
  std::tuple<int, int, int, int, int, int, int> m_lastPersistedSig{-1, -2, -1, -1, -1, -1, -1};

 public:
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);
  void applierTicker();
  void doElection();
  /**
   * \brief 发起一轮投票（预投票或正式选举）。调用前必须已持有 m_mtx。
   * PreVote（raft thesis 9.6 / etcd）：先以当前 term 试探多数派是否愿意投票，
   * 不增 term、不写 votedFor、不落盘；拿到多数派预投票才进入真正的选举。
   * 这样长期被分区/反复重启的节点不会用膨胀的 term 干扰健康集群。
   * \param isPreVote true=预投票轮；false=正式选举轮（term+1、投自己、持久化）
   */
  void startElectionRound(bool isPreVote);

  /**
   * \brief 写路径 pipeline 核心（持锁调用）：立即向单个 follower 复制从 nextIndex 起的日志。
   * 触发点：Start() 新日志提交后 / AE ack 后链式续发 / 心跳 tick 兜底。
   * 在途管理：该 peer 上一轮 AE 未返回时只置 m_aePending，由 finishInflight 补发，
   * 保证任意时刻每个 peer 至多一个在途 AE（不随提交速率堆积线程）。
   * 落后到快照边界内的 peer 交由心跳的快照分支（leaderSendSnapShot）处理。
   */
  void replicateToPeer(int server);

  /// 持锁调用：AE 返回后清在途标志；有 pending 且仍是 leader 则立即补发一轮。
  /// epoch 不匹配说明这是旧任期的在途返回，直接忽略
  void finishInflight(int server, int epoch);
  /**
   * \brief 发起心跳，只有leader才需要发起心跳
   */
  void doHeartBeat();
  // 每隔一段时间检查睡眠时间内有没有重置定时器，没有则说明超时了
  // 如果有则设置合适睡眠时间：睡眠到重置时间+超时时间
  void electionTimeOutTicker();
  std::vector<ApplyMsg> getApplyLogs();
  int getNewCommandIndex();
  void getPrevLogInfo(int server, int *preIndex, int *preTerm);
  void GetState(int *term, bool *isLeader);
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *reply);
  void leaderHearBeatTicker();
  void leaderSendSnapShot(int server);
  void leaderUpdateCommitIndex();
  bool matchLog(int logIndex, int logTerm);
  void persist();
  void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);
  bool UpToDate(int index, int term);
  int getLastLogIndex();
  int getLastLogTerm();
  void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);
  int getLogTermFromLogIndex(int logIndex);
  int GetRaftStateSize();
  int getSlicesIndexFromLogIndex(int logIndex);

  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);
  bool sendAppendEntries(int server, int epoch, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply);

  // rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
  // ，避免使用pthread_create，因此专门写一个函数来执行
  void pushMsgToKvServer(ApplyMsg msg);
  void readPersist(std::string data);
  std::string persistData();

  void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

  // Snapshot the service says it has created a snapshot that has
  // all info up to and including index. this means the
  // service no longer needs the log through (and including)
  // that index. Raft should now trim its log as much as possible.
  // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
  // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
  // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
  void Snapshot(int index, std::string snapshot);

 public:
  // 重写基类方法,因为rpc远程调用真正调用的是这个方法
  //序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
  void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;
  void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

 public:
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

  /**
   * \brief 线性一致读 ReadIndex（raft thesis §6.4 / etcd）：
   * leader 记录当前 (term, commitIndex)，向所有 follower 发一轮心跳确认多数派
   * 仍然认可自己的 leader 身份；确认成功后返回该 commitIndex，调用方等状态机
   * 应用到该 index 后读本地状态机即是线性一致的。
   * Get 请求由此不再写入 raft 日志，读吞吐大幅提升。
   * \param readIndex 出参：安全的一致读位点
   * \return 自己是 leader 且多数派确认成功返回 true
   */
  bool GetReadIndex(int* readIndex);

  /**
   * \brief 当前快照边界（重启恢复时 kvServer 用它初始化自己的 applied 水位）
   */
  int GetLastSnapshotIncludeIndex();

 private:
  // for persist

  class BoostPersistRaftNode {
   public:
    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar &m_currentTerm;
      ar &m_votedFor;
      ar &m_lastSnapshotIncludeIndex;
      ar &m_lastSnapshotIncludeTerm;
      ar &m_logs;
    }
    int m_currentTerm;
    int m_votedFor;
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    std::vector<std::string> m_logs;
    std::unordered_map<std::string, int> umap;

   public:
  };
};

#endif  // RAFT_H