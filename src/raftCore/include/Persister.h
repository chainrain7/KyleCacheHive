//
//

#ifndef SKIP_LIST_ON_RAFT_PERSISTER_H
#define SKIP_LIST_ON_RAFT_PERSISTER_H
#include <mutex>
#include <string>
class Persister {
 private:
  std::mutex m_mtx;
  /**
   * 持久化文件名：raftstate 与 snapshot 存同一个文件（8字节raftstate长度 + raftstate + snapshot）。
   * 两者必须原子地成对更新：如果分成两个文件分别 rename，崩溃落在两次 rename 之间会出现
   * "新 raftstate（日志已截断）+ 旧 snapshot（状态机还是旧的）"的不一致对，
   * 截断掉的日志在两边都找不到，数据永久丢失（6.824 的 persister 同样采用单文件方案）。
   */
  const std::string m_persistFileName;
  /**
   * raftState 当前内容的大小（缓存值，写入时同步更新）
   */
  long long m_raftStateSize;
  /**
   * snapshot 当前内容的缓存：SaveRaftState 只更新 raftstate 时需要原样保留 snapshot，
   * 从缓存取，避免读文件
   */
  std::string m_snapshotCache;

 public:
  void Save(std::string raftstate, std::string snapshot);
  std::string ReadSnapshot();
  void SaveRaftState(const std::string& data);
  long long RaftStateSize();
  std::string ReadRaftState();
  explicit Persister(int me);
  ~Persister() = default;
};

#endif  // SKIP_LIST_ON_RAFT_PERSISTER_H
