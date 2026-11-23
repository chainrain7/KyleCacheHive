//
// 崩溃恢复验证客户端：
//   ./kvRecoverTest put   集群写入 key1..keyN 后退出（之后 kill -9 集群进程再重启）
//   ./kvRecoverTest get   重启后的集群上读回 key1..keyN，逐个校验值，输出 PASS/FAIL
//
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "clerk.h"

namespace {
constexpr int kKeyCount = 10;

std::string ExpectedValue(int i) { return "value-" + std::to_string(i); }
}  // namespace

int main(int argc, char **argv) {
  if (argc < 2) {
    std::printf("usage: %s put|get [-f test.conf]\n", argv[0]);
    return 1;
  }
  const std::string mode = argv[1];
  std::string configFile = "test.conf";
  if (argc >= 4 && std::string(argv[2]) == "-f") {
    configFile = argv[3];
  }

  Clerk clerk;
  clerk.Init(configFile);

  if (mode == "put") {
    for (int i = 1; i <= kKeyCount; i++) {
      clerk.Put("recoverkey" + std::to_string(i), ExpectedValue(i));
    }
    std::printf("[RECOVER-TEST] put %d keys done, now kill -9 the cluster and restart it\n", kKeyCount);
    return 0;
  }

  if (mode == "get") {
    int fail = 0;
    for (int i = 1; i <= kKeyCount; i++) {
      const std::string key = "recoverkey" + std::to_string(i);
      const std::string got = clerk.Get(key);
      if (got == ExpectedValue(i)) {
        std::printf("[RECOVER-TEST] %s = %s OK\n", key.c_str(), got.c_str());
      } else {
        std::printf("[RECOVER-TEST] %s = '%s' EXPECT '%s' FAIL\n", key.c_str(), got.c_str(),
                    ExpectedValue(i).c_str());
        fail++;
      }
    }
    std::printf("[RECOVER-TEST] %s (%d/%d keys survived)\n", fail == 0 ? "PASS" : "FAIL", kKeyCount - fail, kKeyCount);
    return fail == 0 ? 0 : 1;
  }

  std::printf("unknown mode: %s\n", mode.c_str());
  return 1;
}
