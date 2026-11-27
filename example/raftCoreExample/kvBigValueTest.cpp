//
// 大值回归：验证 RPC 分帧之后，超过单个 TCP 段（以及旧实现 4KB 接收缓冲区）的值
// 既能作为请求写入（client -> server 方向分帧），也能作为响应读回（server -> client 方向分帧）
//
#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>

#include "clerk.h"

int main(int argc, char **argv) {
  std::string configFile = (argc >= 3 && std::string(argv[1]) == "-f") ? argv[2] : "test.conf";
  Clerk clerk;
  clerk.Init(configFile);

  const std::vector<size_t> sizes = {1024, 64 * 1024, 300 * 1024};
  int fail = 0;
  for (size_t sz : sizes) {
    std::string value;
    value.reserve(sz);
    for (size_t i = 0; i < sz; i++) {
      value.push_back(static_cast<char>('a' + (i * 7 + sz) % 26));
    }
    const std::string key = "big" + std::to_string(sz);
    clerk.Put(key, value);
    const bool ok = clerk.Get(key) == value;
    std::printf("[BIGVALUE] put+get %-8zu bytes %s\n", sz, ok ? "OK" : "FAIL");
    if (!ok) {
      fail++;
    }
  }
  std::printf("[BIGVALUE] %s\n", fail == 0 ? "PASS" : "FAIL");
  return fail == 0 ? 0 : 1;
}
