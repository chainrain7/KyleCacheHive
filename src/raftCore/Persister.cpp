//
//
#include "Persister.h"
#include "util.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <utility>

namespace {

// 原子落盘：写临时文件 -> fsync -> rename 原子替换 -> fsync 目录。
// 崩溃时要么是旧文件、要么是完整新文件，不会出现写了一半的损坏状态。
bool AtomicWriteFile(const std::string &fileName, const std::string &data) {
  const std::string tmpName = fileName + ".tmp";
  int fd = ::open(tmpName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    DPrintf("[func-AtomicWriteFile] open %s error, errno:%d", tmpName.c_str(), errno);
    return false;
  }
  size_t written = 0;
  while (written < data.size()) {
    ssize_t n = ::write(fd, data.data() + written, data.size() - written);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      DPrintf("[func-AtomicWriteFile] write %s error, errno:%d", tmpName.c_str(), errno);
      ::close(fd);
      return false;
    }
    written += static_cast<size_t>(n);
  }
  const bool fsyncOk = ::fsync(fd) == 0;
  ::close(fd);
  if (!fsyncOk || ::rename(tmpName.c_str(), fileName.c_str()) != 0) {
    DPrintf("[func-AtomicWriteFile] fsync/rename %s error, errno:%d", fileName.c_str(), errno);
    return false;
  }
  // rename 只保证名字落盘的原子性，目录项也要 fsync 才能保证崩溃后文件名不回退
  int dirFd = ::open(".", O_RDONLY | O_DIRECTORY);
  if (dirFd >= 0) {
    ::fsync(dirFd);
    ::close(dirFd);
  }
  return true;
}

// 文件格式：[8字节小端 raftstateSize][raftstate][snapshot]
std::string EncodePersistFile(const std::string &raftState, const std::string &snapshot) {
  std::string out;
  out.reserve(8 + raftState.size() + snapshot.size());
  uint64_t sz = static_cast<uint64_t>(raftState.size());
  char header[8];
  std::memcpy(header, &sz, sizeof(sz));  // 本机小端直接写，读回按同方式解，仅本机使用
  out.append(header, sizeof(header));
  out.append(raftState);
  out.append(snapshot);
  return out;
}

bool DecodePersistFile(const std::string &raw, std::string &raftState, std::string &snapshot) {
  if (raw.size() < 8) {
    return false;
  }
  uint64_t sz;
  std::memcpy(&sz, raw.data(), sizeof(sz));
  if (sz > raw.size() - 8) {
    return false;  // 文件损坏/不完整（理论上原子写不该出现）
  }
  raftState.assign(raw, 8, static_cast<size_t>(sz));
  snapshot.assign(raw, 8 + static_cast<size_t>(sz), std::string::npos);
  return true;
}

// 整文件读入。持久化内容含 boost text archive（空格分隔的文本流），
// 必须按整个文件读取；此前用 ifs >> string 只能读到第一个空白符，恢复必然失败。
bool ReadWholeFile(const std::string &fileName, std::string &out) {
  std::ifstream ifs(fileName, std::ios_base::in | std::ios_base::binary);
  if (!ifs.good()) {
    out.clear();
    return false;
  }
  out.assign((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
  return true;
}

}  // namespace

void Persister::Save(const std::string raftstate, const std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);
  // raftstate 与 snapshot 成对原子写入同一文件，保证崩溃后两者始终是同一时刻的配对状态
  if (AtomicWriteFile(m_persistFileName, EncodePersistFile(raftstate, snapshot))) {
    m_raftStateSize = static_cast<long long>(raftstate.size());
    m_snapshotCache = snapshot;
  }
}

void Persister::SaveRaftState(const std::string &data) {
  std::lock_guard<std::mutex> lg(m_mtx);
  // 只更新 raftstate，snapshot 从缓存原样保留，同样成对落盘
  if (AtomicWriteFile(m_persistFileName, EncodePersistFile(data, m_snapshotCache))) {
    m_raftStateSize = static_cast<long long>(data.size());
  }
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lg(m_mtx);
  return m_raftStateSize;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string raw;
  ReadWholeFile(m_persistFileName, raw);
  std::string raftState, snapshot;
  if (DecodePersistFile(raw, raftState, snapshot)) {
    return raftState;
  }
  return "";
}

std::string Persister::ReadSnapshot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string raw;
  ReadWholeFile(m_persistFileName, raw);
  std::string raftState, snapshot;
  if (DecodePersistFile(raw, raftState, snapshot)) {
    return snapshot;
  }
  return "";
}

Persister::Persister(const int me)
    : m_persistFileName("persist" + std::to_string(me) + ".txt"), m_raftStateSize(0) {
  // 注意：这里不能清空已存在的持久化文件！
  // 节点重启后要靠这个文件恢复状态（Raft::init -> readPersist / KvServer 构造 -> ReadSnapshot），
  // 只在文件不存在时创建空文件。
  int fd = ::open(m_persistFileName.c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd < 0) {
    DPrintf("[func-Persister::Persister] create file %s error, errno:%d", m_persistFileName.c_str(), errno);
  } else {
    ::close(fd);
  }
  // 启动时读一次快照进缓存，SaveRaftState 时才能原样保留
  std::string raw;
  ReadWholeFile(m_persistFileName, raw);
  std::string raftState, snapshot;
  if (DecodePersistFile(raw, raftState, snapshot)) {
    m_snapshotCache = snapshot;
  }
}
