#!/bin/bash
# 性能测试：3 节点集群，4 并发客户端线程，纯读 / 纯写 / 混合(80% 读) 各 3 轮，每轮 10 秒。
# 用法：bash scripts/bench.sh          （先编译出 bin/；可选环境变量 BIN、WORK，同 regress.sh）
#
# 注意：写路径每次写都要同步落盘两轮，纯写延迟基本等于 2 x (一次落盘耗时)。
#       所以脚本一开始先测磁盘 fsync 延迟——不同机器 / 不同时刻的写性能数字只有在 fsync 延迟相同时才可比。
# 流程：预热阶段先把 100 个 key 全部写成有值，之后的读都是读存在的 key。
set -u
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$SCRIPT_DIR/.." && pwd)
BIN=${BIN:-$REPO/bin}
WORK=${WORK:-$HOME/kvcachehive-bench}
mkdir -p "$WORK" && cd "$WORK" || exit 1

echo "===== 磁盘落盘延迟（持久化层的落盘模式）====="
python3 "$SCRIPT_DIR/fsync_probe.py"

pkill -9 -x raftCoreRun 2>/dev/null
pkill -9 -x benchMain 2>/dev/null
sleep 1
cp -f "$BIN/raftCoreRun" "$BIN/benchMain" .
rm -f persist*.txt test.conf
setsid ./raftCoreRun -n 3 -f test.conf > bench_cluster.log 2>&1 < /dev/null &
sleep 14

echo "===== 预热：把 100 个 key 写成有值 ====="
./benchMain -t 4 -d 3 -g 0 -k 100 -f test.conf | grep RESULT

for spec in "纯读:100" "纯写:0" "混合80/20:80"; do
  name=${spec%%:*}
  ratio=${spec##*:}
  echo "===== $name（getRatio=$ratio%），3 轮 x 10 秒 ====="
  for r in 1 2 3; do
    ./benchMain -t 4 -d 10 -g "$ratio" -k 100 -f test.conf | grep RESULT | sed "s/^/round$r /"
    sleep 3
  done
done

pkill -9 -x raftCoreRun 2>/dev/null
echo "done（取每种负载 3 轮的中位数填入 README）"
