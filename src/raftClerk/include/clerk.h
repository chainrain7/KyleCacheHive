//
//

#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H
#include <arpa/inet.h>
#include <netinet/in.h>
#include <raftServerRpcUtil.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <random>
#include <string>
#include <vector>
#include "kvServerRPC.pb.h"
#include "mprpcconfig.h"
class Clerk {
 private:
  std::vector<std::shared_ptr<raftServerRpcUtil>>
      m_servers;  //保存所有raft节点的fd //todo：全部初始化为-1，表示没有连接上
  std::string m_clientId;
  int m_requestId;
  int m_recentLeaderId;  //只是有可能是领导

  std::string Uuid() {
    // rand() 未播种：不同进程的首个 rand() 返回值固定相同，多个 clerk 会拿到同一 clientId，
    // 服务端去重表（clientId->requestId）会把别的客户端请求误判为重复 -> 丢写。
    // 改用 thread_local 随机引擎 + 真随机种子。
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> dist(0, 999999999);
    return std::to_string(dist(rng)) + std::to_string(dist(rng)) + std::to_string(dist(rng)) +
           std::to_string(dist(rng));
  }  //用于返回随机的clientId

  //    MakeClerk  todo
  void PutAppend(std::string key, std::string value, std::string op);

 public:
  //对外暴露的三个功能和初始化
  void Init(std::string configFileName);
  std::string Get(std::string key);

  void Put(std::string key, std::string value);
  void Append(std::string key, std::string value);

 public:
  Clerk();
};

#endif  // SKIP_LIST_ON_RAFT_CLERK_H
