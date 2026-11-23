//
// 压测客户端：N 线程各自持有独立 Clerk，按可配置的 Get/Put 比例混合负载
// 持续打满集群，统计吞吐与延迟分位数。
//
// 用法：./benchMain [-t 线程数(4)] [-d 时长秒(15)] [-g Get占比%(80)]
//                   [-k key空间(100)] [-f 配置文件(test.conf)]
//
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <algorithm>

#include "clerk.h"

namespace {

struct BenchConfig {
  int threads = 4;
  int durationSec = 15;
  int getRatioPercent = 80;
  int keySpace = 100;
  std::string configFile = "test.conf";
};

struct ThreadStat {
  uint64_t totalOps = 0;
  uint64_t failOps = 0;
  std::vector<double> latenciesMs;
};

void Worker(int threadId, const BenchConfig &cfg, std::atomic<bool> &stopFlag, ThreadStat *stat) {
  Clerk clerk;
  clerk.Init(cfg.configFile);

  // 预热：先做一次 Put 找到 leader 并建好连接，避免把发现 leader 的时间计入延迟
  clerk.Put("warmup" + std::to_string(threadId), "0");

  std::mt19937 rng(static_cast<uint32_t>(std::random_device{}()) ^ static_cast<uint32_t>(threadId + 1));
  std::uniform_int_distribution<int> keyDist(0, cfg.keySpace - 1);
  std::uniform_int_distribution<int> opDist(0, 99);
  std::uniform_int_distribution<int> valDist(0, 99999999);

  while (!stopFlag.load(std::memory_order_relaxed)) {
    const std::string key = "key" + std::to_string(keyDist(rng));
    const bool isGet = opDist(rng) < cfg.getRatioPercent;
    const auto t0 = std::chrono::steady_clock::now();
    try {
      if (isGet) {
        clerk.Get(key);  // 空串与有值都算成功（ErrNoKey 也是合法结果）
      } else {
        clerk.Put(key, "v" + std::to_string(valDist(rng)));
      }
    } catch (...) {
      stat->failOps++;
    }
    const auto t1 = std::chrono::steady_clock::now();
    stat->latenciesMs.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
    stat->totalOps++;
  }
}

double Percentile(std::vector<double> &sorted, double p) {
  if (sorted.empty()) {
    return 0.0;
  }
  size_t idx = static_cast<size_t>(p / 100.0 * static_cast<double>(sorted.size()));
  if (idx >= sorted.size()) {
    idx = sorted.size() - 1;
  }
  return sorted[idx];
}

void ShowArgsHelp() {
  std::cout << "format: ./benchMain [-t threads] [-d durationSec] [-g getRatioPercent] [-k keySpace] [-f configFile]"
            << std::endl;
}

}  // namespace

int main(int argc, char **argv) {
  BenchConfig cfg;
  int c = 0;
  while ((c = getopt(argc, argv, "t:d:g:k:f:")) != -1) {
    switch (c) {
      case 't':
        cfg.threads = atoi(optarg);
        break;
      case 'd':
        cfg.durationSec = atoi(optarg);
        break;
      case 'g':
        cfg.getRatioPercent = atoi(optarg);
        break;
      case 'k':
        cfg.keySpace = atoi(optarg);
        break;
      case 'f':
        cfg.configFile = optarg;
        break;
      default:
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
  }
  if (cfg.threads <= 0 || cfg.durationSec <= 0) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }

  std::printf("[BENCH] threads=%d duration=%ds getRatio=%d%% keySpace=%d config=%s\n", cfg.threads, cfg.durationSec,
              cfg.getRatioPercent, cfg.keySpace, cfg.configFile.c_str());

  std::atomic<bool> stopFlag{false};
  std::vector<ThreadStat> stats(cfg.threads);
  std::vector<std::thread> workers;
  const auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < cfg.threads; i++) {
    workers.emplace_back(Worker, i, std::cref(cfg), std::ref(stopFlag), &stats[i]);
  }
  std::this_thread::sleep_for(std::chrono::seconds(cfg.durationSec));
  stopFlag.store(true);
  for (auto &w : workers) {
    w.join();
  }
  const double wallSec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

  uint64_t totalOps = 0, failOps = 0;
  std::vector<double> allLat;
  for (const auto &s : stats) {
    totalOps += s.totalOps;
    failOps += s.failOps;
    allLat.insert(allLat.end(), s.latenciesMs.begin(), s.latenciesMs.end());
  }
  std::sort(allLat.begin(), allLat.end());
  const double sumLat = std::accumulate(allLat.begin(), allLat.end(), 0.0);

  std::printf("[BENCH-RESULT] ops=%llu fail=%llu qps=%.1f\n", static_cast<unsigned long long>(totalOps),
              static_cast<unsigned long long>(failOps), static_cast<double>(totalOps) / wallSec);
  if (!allLat.empty()) {
    std::printf("[BENCH-RESULT] avg_ms=%.3f p50_ms=%.3f p99_ms=%.3f max_ms=%.3f\n", sumLat / allLat.size(),
                Percentile(allLat, 50), Percentile(allLat, 99), allLat.back());
  }
  return 0;
}
