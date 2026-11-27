#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的
  // Your code here (2A, 2B).
  //	不同的人收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term

  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);  // 论文中：让领导人可以及时更新自己
    DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%d}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return;  // 注意从过期的领导人收到消息不要重设定时器
  }
  //    Defer ec1([this]() -> void { this->persist(); });
  //    //由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.
  DEFER { persist(); };  //由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.
  if (args->term() > m_currentTerm) {
    // 三变 ,防止遗漏，无论什么时候都是三变
    // DPrintf("[func-AppendEntries-rf{%v} ] 变成follower且更新term 因为Leader{%v}的term{%v}> rf{%v}.term{%v}\n", rf.me,
    // args.LeaderId, args.Term, rf.me, rf.currentTerm)
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;  // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
    // 这里可不返回，应该改成让改节点尝试接收日志
    // 如果是领导人和candidate突然转到Follower好像也不用其他操作
    // 如果本来就是Follower，那么其term变化，相当于"不言自明"的换了追随的对象，因为原来的leader的term更小，是不会再接收其消息了
  }
  myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
  // 如果发生网络分区，那么candidate可能会收到同一个term的leader的消息，要转变为Follower，为了和上面，因此直接写
  m_status = Follower;  // 这里是有必要的，因为如果candidate收到同一个term的leader的AE，需要变成follower
  // term相等
  m_lastResetElectionTime = now();
  //  DPrintf("[	AppendEntries-func-rf(%v)		] 重置了选举超时定时器\n", rf.me);

  // 不能无脑的从prevlogIndex开始阶段日志，因为rpc可能会延迟，导致发过来的log是很久之前的

  //	那么就比较日志，日志有3种情况
  if (args->prevlogindex() > getLastLogIndex()) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    //  DPrintf("[func-AppendEntries-rf{%v}] 拒绝了节点{%v}，因为日志太新,args.PrevLogIndex{%v} >
    //  lastLogIndex{%v}，返回值：{%v}\n", rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex(), reply)
    return;
  } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    // 如果prevlogIndex还没有更上快照
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(
        m_lastSnapshotIncludeIndex +
        1);  // todo 如果想直接弄到最新好像不对，因为是从后慢慢往前匹配的，这里不匹配说明后面的都不匹配
    // 注意：这里必须直接 return！原实现漏掉 return 会继续走下面的 matchLog 断言，
    // 断言失败会 std::exit 导致整个节点进程崩溃退出
    //  DPrintf("[func-AppendEntries-rf{%v}] 拒绝了节点{%v}，因为log太老，返回值：{%v}\n", rf.me, args.LeaderId, reply)
    return;
  }
  //	本机日志有那么长，冲突(same index,different term),截断日志
  // 注意：这里目前当args.PrevLogIndex == rf.lastSnapshotIncludeIndex与不等的时候要分开考虑，可以看看能不能优化这块
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {
    //	todo：	整理logs
    //，不能直接截断，必须一个一个检查，因为发送来的log可能是之前的，直接截断可能导致"取回"已经在follower日志中的条目
    // 那意思是不是可能会有一段发来的AE中的logs中前半是匹配的，后半是不匹配的，这种应该：1.follower如何处理？ 2.如何给leader回复
    // 3. leader如何处理

    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex()) {
        //超过就直接添加日志
        m_logs.push_back(log);
      } else {
        //没超过就比较是否匹配，不匹配再更新，而不是直接截断
        // todo ： 这里可以改进为比较对应logIndex位置的term是否相等，term相等就代表匹配
        //  todo：这个地方放出来会出问题,按理说index相同，term相同，log也应该相同才对
        // rf.logs[entry.Index-firstIndex].Term ?= entry.Term

        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
          //相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
          myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                 " {%d:%d}却不同！！\n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                 log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
          //不匹配就更新
          m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
        }
      }
    }

    // 错误写法like：  rf.shrinkLogsToIndex(args.PrevLogIndex)
    // rf.logs = append(rf.logs, args.Entries...)
    // 因为可能会收到过期的log！！！ 因此这里是大于等于
    myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
    // if len(args.Entries) > 0 {
    //	fmt.Printf("[func-AppendEntries  rf:{%v}] ] : args.term:%v, rf.term:%v  ,rf.logs的长度：%v\n", rf.me, args.Term,
    // rf.currentTerm, len(rf.logs))
    // }
    if (args->leadercommit() > m_commitIndex) {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
      m_commitCv.notify_all();  // 立即唤醒 applier
      // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
    }

    // 领导会一次发送完所有的日志
    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);

    //        DPrintf("[func-AppendEntries-rf{%v}] 接收了来自节点{%v}的log，当前lastLogIndex{%v}，返回值：{%v}\n",
    //        rf.me,
    //                args.LeaderId, rf.getLastLogIndex(), reply)

    return;
  } else {
    // 优化
    // PrevLogIndex 长度合适，但是不匹配，因此往前寻找 矛盾的term的第一个元素
    // 为什么该term的日志都是矛盾的呢？也不一定都是矛盾的，只是这么优化减少rpc而已
    // ？什么时候term会矛盾呢？很多情况，比如leader接收了日志之后马上就崩溃等等
    reply->set_updatenextindex(args->prevlogindex());

    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    // 对UpdateNextIndex待优化  todo  找到符合的term的最后一个
    //        DPrintf("[func-AppendEntries-rf{%v}]
    //        拒绝了节点{%v}，因为prevLodIndex{%v}的args.term{%v}不匹配当前节点的logterm{%v}，返回值：{%v}\n",
    //                rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
    //                rf.logs[rf.getSlicesIndexFromLogIndex(args.PrevLogIndex)].LogTerm, reply)
    //        DPrintf("[func-AppendEntries-rf{%v}] 返回值: reply.UpdateNextIndex从{%v}优化到{%v}，优化了{%v}\n", rf.me,
    //                args.PrevLogIndex, reply.UpdateNextIndex, args.PrevLogIndex - reply.UpdateNextIndex) //
    //                很多都是优化了0
    return;
  }

  // fmt.Printf("[func-AppendEntries,rf{%v}]:len(rf.logs):%v, rf.commitIndex:%v\n", rf.me, len(rf.logs), rf.commitIndex)
}

void Raft::applierTicker() {
  while (true) {
    std::vector<ApplyMsg> applyMsgs;
    {
      std::unique_lock<std::mutex> lk(m_mtx);
      // commitIndex 推进时被 m_commitCv 唤醒（leaderUpdateCommitIndex notify），
      // 50ms 超时兜底轮询防丢失通知。原来是固定 10ms 轮询：写路径延迟下限被卡住
      m_commitCv.wait_for(lk, std::chrono::milliseconds(50), [this]() { return m_lastApplied < m_commitIndex; });
      if (m_status == Leader) {
        DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
                m_commitIndex);
      }
      applyMsgs = getApplyLogs();
    }
    //使用匿名函数是因为传递管道的时候不用拿锁
    // todo:好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
    if (!applyMsgs.empty()) {
      DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me, applyMsgs.size());
    }
    for (auto& message : applyMsgs) {
      applyChan->Push(message);
    }
  }
}

// CondInstallSnapshot 已移除：raft 层的 InstallSnapshot RPC 处理器在 push 消息前就完成了
// 日志截断/快照边界更新/落盘，任何基于 raft 状态的二次校验都会因"已应用"而永远拒绝安装。
// "是否安装到状态机"的条件校验属于状态机层，见 KvServer::GetSnapShotFromRaft 对
// m_lastSnapShotRaftLogIndex 水位的检查（6.824 中 condInstallSnapshot 本就是 service 侧方法）。

void Raft::doElection() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status != Leader) {
    DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，先发起预投票 \n", m_me);
    // PreVote：先试探多数派，避免被分区/重启的节点用膨胀 term 扰动集群
    startElectionRound(true);
  }
}

// 调用前必须持有 m_mtx
void Raft::startElectionRound(bool isPreVote) {
  m_status = Candidate;
  if (!isPreVote) {
    // 预投票拿到多数派支持，才真正开始选举：term+1、投自己、落盘
    m_currentTerm += 1;
    m_votedFor = m_me;  //即是自己给自己投，也避免candidate给同辈的candidate投
    persist();
  }
  std::shared_ptr<int> votedNum = std::make_shared<int>(1);  // 使用 make_shared 函数初始化 !! 亮点
  //	重新设置定时器
  m_lastResetElectionTime = now();
  //	发布RequestVote RPC
  for (int i = 0; i < m_peers.size(); i++) {
    if (i == m_me) {
      continue;
    }
    int lastLogIndex = -1, lastLogTerm = -1;
    getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //获取最后一个log的term和下标

    std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
        std::make_shared<raftRpcProctoc::RequestVoteArgs>();
    requestVoteArgs->set_term(isPreVote ? m_currentTerm + 1 : m_currentTerm);  // 预投票用下一个任期试探（自己不增 term）
    requestVoteArgs->set_candidateid(m_me);
    requestVoteArgs->set_lastlogindex(lastLogIndex);
    requestVoteArgs->set_lastlogterm(lastLogTerm);
    requestVoteArgs->set_isprevote(isPreVote);
    auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

    //使用匿名函数执行避免其拿到锁

    std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                  votedNum);  // 创建新线程并执行b函数，并传递参数
    t.detach();
  }
}

void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
    //写路径 pipeline 化之后，心跳退化为：保活 + 慢节点/异常兜底。
    //正常复制由 Start()（新日志立即发）与 AE ack（链式续发）驱动，见 replicateToPeer。
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
      //日志落后于快照边界的 peer 由 replicateToPeer 内部改发快照；
      //在途时只记 pending（由 finishInflight 补发），不叠加发送
      replicateToPeer(i);
    }
    m_lastResetHearBeatTime = now();  // leader发送心跳，就不是随机时间了
  }
}

void Raft::electionTimeOutTicker() {
  // Check if a Leader election should be started.
  while (true) {
    /**
     * 如果不睡眠，那么对于leader，这个函数会一直空转，浪费cpu。且加入协程之后，空转会导致其他协程无法运行，对于时间敏感的AE，会导致心跳无法正常发送导致异常
     */
    while (m_status == Leader) {
      // 注意单位：usleep 参数是微秒，HeartBeatTimeout 的单位是毫秒，必须乘 1000，
      // 原实现只睡 25 微秒会退化成忙等烧 CPU
      usleep(1000 * HeartBeatTimeout);  //定时时间没有严谨设置，因为HeartBeatTimeout比选举超时一般小一个数量级，因此就设置为HeartBeatTimeout了
    }
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      m_mtx.lock();
      wakeTime = now();
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      if (Debug) {
        std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                  << std::endl;
      }
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      if (Debug) {
        std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                  << std::endl;
      }
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
      //说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
      continue;
    }
    doElection();
  }
}

std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));

  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);
    //        DPrintf("[	applyLog func-rf{%v}	] apply Log,logIndex:%v  ，logTerm：{%v},command：{%v}\n",
    //        rf.me, rf.lastApplied, rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogTerm,
    //        rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].Command)
  }
  return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
  //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// getPrevLogInfo
// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    //要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// GetState return currentTerm and whether this server
// believes it is the Leader.
void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  DEFER {
    // todo 暂时不清楚会不会导致死锁
    m_mtx.unlock();
  };

  // Your code here (2A).
  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.term{%v}<rf{%v}.term{%v} ", rf.me, args.LeaderId,
    //        args.Term, rf.me, rf.currentTerm)

    return;
  }
  if (args->term() > m_currentTerm) {
    //后面两种情况都要接收日志
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
  }
  m_status = Follower;
  m_lastResetElectionTime = now();
  // 任何返回路径都必须填 term：字段全是默认值的响应序列化后是 0 字节，没有分帧时客户端什么都收不到，
  // 只能等到超时。leader 重发快照时会因此误判为失败，永远不给该节点发送快照之后的日志
  reply->set_term(m_currentTerm);
  // outdated snapshot
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.LastSnapShotIncludeIndex{%v} <=
    //        rf{%v}.lastSnapshotIncludeIndex{%v} ", rf.me, args.LeaderId, args.LastSnapShotIncludeIndex, rf.me,
    //        rf.lastSnapshotIncludeIndex)
    return;
  }
  //截断日志，修改commitIndex和lastApplied
  //截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
  //但是由于现在getSlicesIndexFromLogIndex的实现，不能传入不存在logIndex，否则会panic
  auto lastLogIndex = getLastLogIndex();

  if (lastLogIndex > args->lastsnapshotincludeindex()) {
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
  } else {
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_commitCv.notify_all();  // 快照内的数据视为已提交，立即唤醒 applier
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();

  std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
  t.detach();
  //看下这里能不能再优化
  //    DPrintf("[func-InstallSnapshot-rf{%v}] receive snapshot from {%v} ,LastSnapShotIncludeIndex ={%v} ", rf.me,
  //    args.LeaderId, args.LastSnapShotIncludeIndex)
  //持久化
  DPrintf("[func-InstallSnapshot-rf{%d}] 收到 leader{%d} 的快照 lastsnapshotincludeindex{%d} term{%d}", m_me,
          args->leaderid(), args->lastsnapshotincludeindex(), args->term());
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

void Raft::leaderHearBeatTicker() {
  while (true) {
    //不是leader的话就没有必要进行后续操作，况且还要拿锁，很影响性能，目前是睡眠，后面再优化优化
    while (m_status != Leader) {
      usleep(1000 * HeartBeatTimeout);
      // std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
    }
    static std::atomic<int32_t> atomicCount = 0;

    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      if (Debug) {
        std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                  << std::endl;
      }
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      if (Debug) {
        std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                  << " 毫秒\033[0m" << std::endl;
      }
      ++atomicCount;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0) {
      //睡眠的这段时间有重置定时器，没有超时，再次睡眠
      continue;
    }
    // DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了\n", m_me);
    doHeartBeat();
  }
}

// 向落后于快照边界的 follower 发送快照。自带在途保护：同一 peer 同时至多一个快照 RPC 在途，
// 因此可以从任意路径（心跳 / AE 被拒后立即触发）调用，不会堆积线程。
void Raft::leaderSendSnapShot(int server) {
  std::unique_lock<std::mutex> lk(m_mtx);
  if (m_status != Leader || m_snapshotInflight[server]) {
    return;
  }
  m_snapshotInflight[server] = true;
  // 任意返回路径都清在途标志（DEFER 在 lk 之后声明，析构时仍持锁）
  DEFER { m_snapshotInflight[server] = false; };

  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());
  DPrintf("[func-leaderSendSnapShot-rf{%d}] 向节点{%d}发送快照 lastsnapshotincludeindex{%d} term{%d}", m_me, server,
          args.lastsnapshotincludeindex(), args.term());

  raftRpcProctoc::InstallSnapshotResponse reply;
  lk.unlock();  // RPC 期间不持锁
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
  lk.lock();
  if (!ok) {
    DPrintf("[func-leaderSendSnapShot-rf{%d}] 向节点{%d}的快照 RPC 失败", m_me, server);
    return;  // 失败/超时：等下一次心跳重试
  }
  if (m_status != Leader || m_currentTerm != args.term()) {
    return;  //中间释放过锁，可能状态已经改变了
  }
  //	无论什么时候都要判断term
  if (reply.term() > m_currentTerm) {
    //三变
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
  DPrintf("[func-leaderSendSnapShot-rf{%d}] 节点{%d}快照安装成功 matchIndex{%d} nextIndex{%d} lastLogIndex{%d}", m_me,
          server, m_matchIndex[server], m_nextIndex[server], getLastLogIndex());
  leaderUpdateCommitIndex();  // matchIndex 变了，重新计算提交点
  // 快照装完后立即接着复制快照之后的日志，不等下一个心跳
  replicateToPeer(server);
}

void Raft::leaderUpdateCommitIndex() {
  // 从最后一条日志向下找：多数派 matchIndex 覆盖、且属于当前 term 的最大 index 即为可提交点。
  // 只有当前 term 的日志才能直接提交（论文 Fig8），之前 term 的日志靠当前 term 提交间接提交。
  // 下界取 max(commitIndex, lastSnapshotIncludeIndex)+1：
  //  - commitIndex 只前进（重置再找会把已提交进度倒退）
  //  - 快照边界之下的 index 已被截断不存在（重启恢复后 commitIndex 可能暂落后于边界，
  //    越过边界取 term 会触发断言崩溃——线上复现过 index{边界-1} 崩溃）
  int lowerBound = std::max(m_commitIndex, m_lastSnapshotIncludeIndex) + 1;
  for (int index = getLastLogIndex(); index >= lowerBound; index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me || m_matchIndex[i] >= index) {
        sum += 1;  // leader 自己的一票由 i==m_me 特判
      }
    }
    if (sum >= static_cast<int>(m_peers.size()) / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      m_commitCv.notify_all();  // 立即唤醒 applier，不等下一轮轮询
      break;
    }
  }
  DPrintf("[func-leaderUpdateCommitIndex-rf{%d}] commitIndex -> {%d}", m_me, m_commitIndex);
}

// 持锁调用：写路径 pipeline 核心，立即向单个 follower 复制从 nextIndex 起的日志
void Raft::replicateToPeer(int server) {
  if (m_status != Leader || server == m_me) {
    return;
  }
  if (m_nextIndex[server] <= m_lastSnapshotIncludeIndex) {
    // 该 peer 的日志落后于快照边界：发快照而不是 AE。leaderSendSnapShot 自带在途保护
    //（同一 peer 同时至多一个快照 RPC），这里直接触发，不必等下一个心跳
    std::thread t(&Raft::leaderSendSnapShot, this, server);
    t.detach();
    return;
  }
  if (m_aeInflight[server]) {
    m_aePending[server] = true;  // 上一轮 AE 还在途，返回时会补发
    return;
  }
  m_aeInflight[server] = true;
  const int epoch = ++m_aeEpoch[server];  // 新一轮 AE，旧任期的在途返回凭 epoch 失效

  // 构造 AE（从 nextIndex 起，单次上限 MaxEntriesPerAppendEntries，与心跳相同）
  int preLogIndex = -1;
  int PrevLogTerm = -1;
  getPrevLogInfo(server, &preLogIndex, &PrevLogTerm);
  std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
      std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
  appendEntriesArgs->set_term(m_currentTerm);
  appendEntriesArgs->set_leaderid(m_me);
  appendEntriesArgs->set_prevlogindex(preLogIndex);
  appendEntriesArgs->set_prevlogterm(PrevLogTerm);
  appendEntriesArgs->clear_entries();
  appendEntriesArgs->set_leadercommit(m_commitIndex);
  if (preLogIndex != m_lastSnapshotIncludeIndex) {
    for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1;
         j < static_cast<int>(m_logs.size()) && appendEntriesArgs->entries_size() < MaxEntriesPerAppendEntries; ++j) {
      *appendEntriesArgs->add_entries() = m_logs[j];
    }
  } else {
    for (const auto& item : m_logs) {
      if (appendEntriesArgs->entries_size() >= MaxEntriesPerAppendEntries) {
        break;
      }
      *appendEntriesArgs->add_entries() = item;
    }
  }

  std::thread t(&Raft::sendAppendEntries, this, server, epoch, appendEntriesArgs,
                std::make_shared<raftRpcProctoc::AppendEntriesReply>());
  t.detach();
}

// 持锁调用：AE 返回（成功/失败/超时）后清在途标志；有 pending 且仍是 leader 则立即补发。
// epoch 不匹配说明这是旧任期/旧轮次的在途返回，其状态已被新轮次接管，直接忽略
void Raft::finishInflight(int server, int epoch) {
  if (epoch != m_aeEpoch[server]) {
    return;
  }
  m_aeInflight[server] = false;
  if (m_aePending[server]) {
    m_aePending[server] = false;
    replicateToPeer(server);
  }
}

//进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
  // if logIndex == rf.lastSnapshotIncludeIndex {
  // 	return logTerm == rf.lastSnapshotIncludeTerm
  // } else {
  // 	return logTerm == rf.logs[rf.getSlicesIndexFromLogIndex(logIndex)].LogTerm
  // }
}

void Raft::persist() {
  // Your code here (2C).
  // 持久化去重：只有影响恢复正确性的状态（term/votedFor/快照边界/日志）发生变化时才写盘。
  // 原实现每条心跳 AE / 每次被拒投票都会全量序列化重写 raft state 文件，IO 放大严重。
  std::tuple<int, int, int, int, int, int, int> sig{
      m_currentTerm,
      m_votedFor,
      m_lastSnapshotIncludeIndex,
      m_lastSnapshotIncludeTerm,
      static_cast<int>(m_logs.size()),
      m_logs.empty() ? 0 : m_logs.back().logindex(),
      m_logs.empty() ? 0 : m_logs.back().logterm()};
  if (sig == m_lastPersistedSig) {
    return;
  }
  auto data = persistData();
  m_persister->SaveRaftState(data);
  m_lastPersistedSig = sig;
}

void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);

  // Your code here (2A, 2B).

  if (args->isprevote()) {
    // 预投票（PreVote）：只表达"如果你的日志够新、这个 term 我愿意投你"的意见，
    // 不更新本地 term/votedFor、不落盘、不重置选举定时器（避免预投票抑制别人的正常选举）
    // 候选人发来的是它的下一个任期；不高于我的当前任期，说明它并不比我新，拒绝（回复我的任期让它跟上）
    if (args->term() <= m_currentTerm) {
      reply->set_term(m_currentTerm);
      reply->set_votestate(Expire);
      reply->set_votegranted(false);
      return;
    }
    if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
      reply->set_term(m_currentTerm);
      reply->set_votestate(Voted);
      reply->set_votegranted(false);
      return;
    }
    // 同一个预期任期内只预投给一个人。注意不能拿 m_votedFor 来判断：它记录的是当前任期的正式投票，
    // 稳定集群里每个 follower 都已经投给现任 leader，用它判断会让 leader 宕机后永远选不出新 leader
    if (m_preVoteTerm == args->term() && m_preVotedFor != -1 && m_preVotedFor != args->candidateid()) {
      reply->set_term(m_currentTerm);
      reply->set_votestate(Voted);
      reply->set_votegranted(false);
      return;
    }
    m_preVoteTerm = args->term();
    m_preVotedFor = args->candidateid();
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
    return;
  }

  DEFER {
    //应该先持久化，再撤销lock（persist 内部有状态签名去重，无变化时不会真的写盘）
    persist();
  };
  //对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
  // reason: 出现网络分区，该竞选者已经OutOfDate(过时）
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    return;
  }
  // fig2:右下角，如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
  if (args->term() > m_currentTerm) {
    //        DPrintf("[	    func-RequestVote-rf(%v)		] : 变成follower且更新term
    //        因为candidate{%v}的term{%v}> rf{%v}.term{%v}\n ", rf.me, args.CandidateId, args.Term, rf.me,
    //        rf.currentTerm)
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;

    //	重置定时器：收到leader的ae，开始选举，透出票
    //这时候更新了term之后，votedFor也要置为-1
  }
  myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
  //	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)，还需要检查log的term和index是不是匹配的了

  int lastLogTerm = getLastLogTerm();
  //只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
    // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
    //日志太旧了
    if (args->lastlogterm() < lastLogTerm) {
      //                    DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
      //                    candidate_lastlog_term{%v} < lastlog_term{%v}\n", rf.me, args.CandidateId, args.LastLogTerm,
      //                    lastLogTerm)
    } else {
      //            DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
      //            candidate_log_index{%v} < log_index{%v}\n", rf.me, args.CandidateId, args.LastLogIndex,
      //            rf.getLastLogIndex())
    }
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  }
  // todo ： 啥时候会出现rf.votedFor == args.CandidateId ，就算candidate选举超时再选举，其term也是不一样的呀
  //     当因为网络质量不好导致的请求丢失重发就有可能！！！！
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    //        DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because has voted\n",
    //        rf.me, args.CandidateId)
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);

    return;
  } else {
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now();  //认为必须要在投出票的时候才重置定时器，
    //        DPrintf("[	    func-RequestVote-rf(%v)		] : voted rf[%v]\n", rf.me, rf.votedFor)
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);

    return;
  }
}

bool Raft::UpToDate(int index, int term) {
  // lastEntry := rf.log[len(rf.log)-1]

  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 *
 * @return 最新的log的logindex，即log的逻辑index。区别于log在m_logs中的物理index
 * 可见：getLastLogIndexAndTerm()
 */
int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 *
 * @param logIndex log的逻辑index。注意区别于m_logs的物理index
 * @return
 */
int Raft::getLogTermFromLogIndex(int logIndex) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 找到index对应的真实下标位置！！！
// 限制，输入的logIndex必须保存在当前的logs里面（不包含snapshot）
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
  //这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  // ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  // todo
  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 開始，term{%d} lastLogIndex{%d}", m_me, server,
          m_currentTerm, getLastLogIndex());
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 完畢，耗時:{%d} ms", m_me, server,
          std::chrono::duration_cast<std::chrono::milliseconds>(now() - start).count());

  if (!ok) {
    return ok;  //不知道为什么不加这个的话如果服务器宕机会出现问题的，通不过2B  todo
  }
  // for !ok {
  //
  //	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  //	//if ok {
  //	//	break
  //	//}
  // } //这里是发送出去了，但是不能保证他一定到达
  //对回应进行处理，要记得无论什么时候收到回复就要检查term
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) {
    m_status = Follower;  //三变：身份，term，和投票
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return true;
  } else if (reply->term() < m_currentTerm) {
    return true;
  }
  myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

  if (!reply->votegranted()) {
    return true;
  }

  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    //变成leader
    *votedNum = 0;
    if (m_status == Leader) {
      //如果已经是leader了，那么是就是了，不会进行下一步处理了k
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
    }
    if (args->isprevote()) {
      // 预投票拿到多数派，且本轮预投票仍然有效（期间没有因为收到 leader 心跳/更高任期而作废），
      // 才真正开始选举（term+1、投自己、落盘、重发正式投票）
      if (m_status == Candidate && args->term() == m_currentTerm + 1) {
        DPrintf("[func-sendRequestVote rf{%d}] pre-vote majority granted, start real election, term will bump from %d",
                m_me, m_currentTerm);
        startElectionRound(false);
      }
      return true;
    }
    //	第一次变成leader，初始化状态和nextIndex、matchIndex
    m_status = Leader;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;  //有效下标从1开始，因此要+1
      m_matchIndex[i] = 0;                //每换一个领导都是从0开始，见fig2
      m_aeInflight[i] = false;            // 新任期清理上一任期的在途状态
      m_aePending[i] = false;
      m_aeEpoch[i]++;  // 旧任期的在途 AE 返回后凭 epoch 比对自动失效
    }
    {
      // 当选后立即追加一条 no-op 空操作（etcd/TiKV 同款实践）：
      // 新 leader 不能直接提交旧 term 的日志（论文 Fig8），必须当前 term 先提交一条，
      // 才能间接提交历史日志。ReadIndex 化后纯读负载不再产生新日志，
      // 没有这条 no-op 的话重启集群的历史日志将永远无法提交重放（恢复实测 0/10 丢数据的根因）。
      // kvServer 侧 apply 到该条目时 Operation 为空，不会执行任何状态机操作
      Op noOp{};  // 值初始化（RequestId=0），kvServer 侧 Operation 为空不执行任何操作
      raftRpcProctoc::LogEntry noOpEntry;
      noOpEntry.set_command(noOp.asString());
      noOpEntry.set_logterm(m_currentTerm);
      noOpEntry.set_logindex(getNewCommandIndex());
      m_logs.emplace_back(noOpEntry);
      persist();
    }
    std::thread t(&Raft::doHeartBeat, this);  //马上向其他节点宣告自己就是leader
    t.detach();

    persist();
  }
  return true;
}

bool Raft::sendAppendEntries(int server, int epoch, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply) {
  //这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  // 如果网络不通的话肯定是没有返回的，不用一直重试（心跳 tick 会兜底重发）
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc開始 ， args->entries_size():{%d}", m_me,
          server, args->entries_size());
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  std::lock_guard<std::mutex> lg1(m_mtx);
  // 任意 return 路径都要清在途标志并处理 pending 补发（DEFER 在 lg1 之后创建，持锁执行）
  DEFER { finishInflight(server, epoch); };

  if (!ok) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失敗", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
  if (reply->appstate() == Disconnected) {
    return ok;
  }

  //对reply进行处理
  // 对于rpc通信，无论什么时候都要检查term
  if (reply->term() > m_currentTerm) {
    m_status = Follower;  //三变
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return ok;
  } else if (reply->term() < m_currentTerm) {
    DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader) {
    //如果不是leader，那么就不要对返回的情况进行处理了
    return ok;
  }
  // term相等

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
  if (!reply->success()) {
    //日志不匹配，正常来说就是index要往前-1，既然能到这里，第一个日志（idnex =
    // 1）发送后肯定是匹配的，因此不用考虑变成负数 因为真正的环境不会知道是服务器宕机还是发生网络分区了
    if (reply->updatenextindex() != -100) {
      DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
      // 只回缩不前进：迟到的旧 reply 不能把 nextIndex 拉回（原来直接赋值有此隐患）。
      // 这里不能加不低于快照点+1 的下限：follower 的日志落后于快照点时，nextIndex 必须能落到
      // 快照点及以下，replicateToPeer 才会改发快照。之前加了下限，快照同步永远触发不了，
      // 掉队的节点永远追不上（且 leader 对着它无限重发 AE）
      m_nextIndex[server] = std::min(m_nextIndex[server], reply->updatenextindex());  //失败时不更新matchIndex
    }
    //回缩后重试（pipeline：立即补发，不等下个心跳）；若已回缩到快照点内，replicateToPeer 会改发快照
    m_aePending[server] = true;
  } else {
    // rf.matchIndex[server] = len(args.Entries) //只要返回一个响应就对其matchIndex应该对其做出反应，
    //但是这么修改是有问题的，如果对某个消息发送了多遍（心跳时就会再发送），那么一条消息会导致n次上涨
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;

    myAssert(m_nextIndex[server] <= getLastLogIndex() + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d", server, m_logs.size()));

    // matchIndex 变化后按论文 Fig2 重新计算提交点（替换原"单轮心跳批次计数"提交，
    // pipeline 下 ack 跨批次分布，计数法无法正确工作）
    leaderUpdateCommitIndex();
    myAssert(m_commitIndex <= getLastLogIndex(),
             format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, getLastLogIndex(),
                    m_commitIndex));

    //还有剩余日志没同步完 -> 链式续发（pipeline 核心：ack 即触发下一批）
    if (m_nextIndex[server] <= getLastLogIndex()) {
      m_aePending[server] = true;
    }
  }
  return ok;
}

void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
  AppendEntries1(request, response);
  done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
  InstallSnapshot(request, response);

  done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
  RequestVote(request, response);
  done->Run();
}

int Raft::GetLastSnapshotIncludeIndex() {
  std::lock_guard<std::mutex> lg(m_mtx);
  return m_lastSnapshotIncludeIndex;
}

bool Raft::GetReadIndex(int* readIndex) {
  // 多数派确认上下文：acks 从 1 起步（算上自己）
  struct ReadConfirmCtx {
    std::mutex m;
    std::condition_variable cv;
    int acks = 1;
    bool confirmed = false;
  };
  auto ctx = std::make_shared<ReadConfirmCtx>();

  int term = 0;
  int commitIdx = 0;
  {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (m_status != Leader) {
      *readIndex = -1;
      return false;
    }
    term = m_currentTerm;
    commitIdx = m_commitIndex;
  }
  if (m_peers.size() == 1) {
    *readIndex = commitIdx;  // 单节点自己就是多数派
    return true;
  }

  for (int i = 0; i < static_cast<int>(m_peers.size()); i++) {
    if (i == m_me) {
      continue;
    }
    std::thread confirmThread([this, i, term, ctx]() {
      // 轻量心跳确认：只关心 follower 是否仍认可本 term 的 leader 身份（reply.term == term）。
      // follower 日志落后/冲突时回 success=false 但 term 已更新为我方 term，同样是有效 ack。
      // 注意：prevLogIndex 不能用 m_nextIndex[i]-1（getPrevLogInfo）——follower 落后于快照边界时
      // 该 index 可能已被截断（nextIndex <= lastSnapshotIncludeIndex 的窗口），直接用永远合法的
      // 快照边界即可，确认语义与日志匹配无关。
      auto args = std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        args->set_term(term);
        args->set_leaderid(m_me);
        args->set_prevlogindex(m_lastSnapshotIncludeIndex);
        args->set_prevlogterm(m_lastSnapshotIncludeTerm);
        args->set_leadercommit(m_commitIndex);  // 空entries心跳，顺带推进follower的commitIndex
      }
      auto reply = std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      bool ok = m_peers[i]->AppendEntries(args.get(), reply.get());
      if (!ok) {
        return;
      }
      {
        std::lock_guard<std::mutex> lg(m_mtx);
        if (reply->term() > m_currentTerm) {
          // 发现更高任期：立即退位，本轮确认作废
          m_status = Follower;
          m_currentTerm = reply->term();
          m_votedFor = -1;
          persist();
          return;
        }
      }
      if (reply->term() != term) {
        return;  // 过期回复，不计入
      }
      std::lock_guard<std::mutex> cl(ctx->m);
      if (!ctx->confirmed) {
        ctx->acks++;
        if (ctx->acks >= static_cast<int>(m_peers.size()) / 2 + 1) {
          ctx->confirmed = true;
          ctx->cv.notify_all();
        }
      }
    });
    confirmThread.detach();
  }

  std::unique_lock<std::mutex> cl(ctx->m);
  bool signaled = ctx->cv.wait_for(cl, std::chrono::milliseconds(100), [&ctx] { return ctx->confirmed; });
  if (!signaled) {
    return false;  // 100ms 内没拿到多数派确认（网络问题），让调用方重试
  }
  // 复核：等待确认期间身份/任期没有变化
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_status != Leader || m_currentTerm != term) {
    return false;
  }
  *readIndex = commitIdx;
  return true;
}

void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
  std::lock_guard<std::mutex> lg1(m_mtx);
  //    m_mtx.lock();
  //    Defer ec1([this]()->void {
  //       m_mtx.unlock();
  //    });
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader");
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString());
  newLogEntry.set_logterm(m_currentTerm);
  newLogEntry.set_logindex(getNewCommandIndex());
  m_logs.emplace_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();

  // 写路径 pipeline：新日志提交后立即向所有 follower 复制，不等下一个心跳 tick
  //（原实现要等最多 HeartBeatTimeout=25ms，写延迟/吞吐被心跳节奏卡死）
  for (int i = 0; i < static_cast<int>(m_peers.size()); i++) {
    replicateToPeer(i);
  }

  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d\n", m_me, lastLogIndex);
  // rf.timer.Reset(10) //接收到命令后马上给follower发送,改成这样不知为何会出现问题，待修正 todo
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  // Your initialization code here (2A, 2B, 2C).
  m_mtx.lock();

  // applier
  this->applyChan = applyCh;
  //    rf.ApplyMsgQueue = make(chan ApplyMsg)
  m_currentTerm = 0;
  m_status = Follower;
  m_commitIndex = 0;
  m_lastApplied = 0;
  m_logs.clear();
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);
    m_nextIndex.push_back(0);
    m_aeInflight.push_back(false);
    m_aePending.push_back(false);
    m_aeEpoch.push_back(0);
    m_snapshotInflight.push_back(false);
  }
  m_votedFor = -1;

  m_lastSnapshotIncludeIndex = 0;
  m_lastSnapshotIncludeTerm = 0;
  m_lastResetElectionTime = now();
  m_lastResetHearBeatTime = now();

  // initialize from state persisted before a crash
  readPersist(m_persister->ReadRaftState());
  if (m_lastSnapshotIncludeIndex > 0) {
    m_lastApplied = m_lastSnapshotIncludeIndex;
    // 快照边界内的数据视为已提交：Snapshot() 只在 index <= commitIndex 时执行，
    // 因此恢复时 commitIndex 至少是快照边界（回答上游 todo：可以且应该恢复到边界）。
    // 不恢复的话 leaderUpdateCommitIndex 的循环下界会落到边界之下，越界断言崩溃
    m_commitIndex = m_lastSnapshotIncludeIndex;
  }
  // 恢复出的状态与磁盘一致，同步持久化签名，避免重启后第一次心跳就重写一遍相同的文件
  m_lastPersistedSig = {m_currentTerm,
                        m_votedFor,
                        m_lastSnapshotIncludeIndex,
                        m_lastSnapshotIncludeTerm,
                        static_cast<int>(m_logs.size()),
                        m_logs.empty() ? 0 : m_logs.back().logindex(),
                        m_logs.empty() ? 0 : m_logs.back().logterm()};

  DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  m_mtx.unlock();

  m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

  // start ticker fiber to start elections
  // 启动三个循环定时器
  // todo:原来是启动了三个线程，现在是直接使用了协程，三个函数中leaderHearBeatTicker
  // 、electionTimeOutTicker执行时间是恒定的，applierTicker时间受到数据库响应延迟和两次apply之间请求数量的影响，这个随着数据量增多可能不太合理，最好其还是启用一个线程。
  m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
  m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); });

  std::thread t3(&Raft::applierTicker, this);
  t3.detach();

  // std::thread t(&Raft::leaderHearBeatTicker, this);
  // t.detach();
  //
  // std::thread t2(&Raft::electionTimeOutTicker, this);
  // t2.detach();
  //
  // std::thread t3(&Raft::applierTicker, this);
  // t3.detach();
}

std::string Raft::persistData() {
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
  for (auto& item : m_logs) {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode;
  return ss.str();
}

void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  // read class state from archive
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto& item : boostPersistRaftNode.m_logs) {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex();  //为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  //制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    //注意有=，因为要拿到最后一个日志
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  // rf.lastApplied = index //lastApplied 和 commit应不应该改变呢？？？ 为什么  不应该改变吧
  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}