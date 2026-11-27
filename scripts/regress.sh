#!/bin/bash
# 集群回归测试。用法：bash scripts/regress.sh [t1 t2 ...]   （不带参数 = 跑全部）
# 前置：已经编译出 bin/（mkdir -p build && cd build && cmake .. && make -j$(nproc)）
# 环境：Linux（依赖 muduo，只能在 Linux 下构建运行）。脚本会把 bin/ 里的可执行文件拷到 WORK 目录再跑，
#       WORK 请指向原生 Linux 文件系统上的目录（跨系统挂载盘上新编译的二进制偶发 exec 失败，而且慢）。
# 可配置：BIN=可执行文件目录（默认 <仓库>/bin）   WORK=运行目录（默认 ~/kvcachehive-regress）
# 判读：每个用例末尾会打印结论行；出现 FAIL / FAILED / SAME(...)=False 即为回归
#
#   t1 小快照(>=1KB)  t2 大快照(>=50KB)  t3 超大快照(>=200KB，验证分帧)
#        —— 三组流程相同：kill -9 当前 leader（验证故障转移）-> 持续纯写直到快照达到目标大小 -> 重启全集群 -> 旧 leader 应追上
#   t4 分区愈合：kill -STOP 冻结 leader -> 持续纯写直到触发快照 -> 解冻 -> 旧 leader 应退位并追上（逐秒采样）
#   t5 常规回归（callerMain / 混合压测 / 崩溃恢复）
#   t6 leader 故障转移耗时（压测进行中 kill -9 leader，bench 的 max_ms 约等于故障转移耗时）
#   t7 大值回归（1KB / 64KB / 300KB 的值 put + get，验证两个方向的分帧）
#   t8 RPC 框架独立冒烟（rpcExample 的 provider + consumer；示例客户端每次请求间隔 5s，10 次约 50s）
#   t9 内存泄漏检查（15s 纯读压测前后三个节点的 RSS）
set -u
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$SCRIPT_DIR/.." && pwd)
BIN=${BIN:-$REPO/bin}
WORK=${WORK:-$HOME/kvcachehive-regress}
mkdir -p "$WORK" && cd "$WORK" || exit 1
STOP_ALL() { pkill -9 -x raftCoreRun 2>/dev/null; pkill -9 -x benchMain 2>/dev/null; pkill -9 -x kvRecoverTest 2>/dev/null; pkill -9 -x kvBigValueTest 2>/dev/null; }
FRESH() { STOP_ALL; sleep 1; rm -f persist*.txt test.conf; }
START() { setsid ./raftCoreRun -n 3 -f test.conf > "$1" 2>&1 < /dev/null & }
STATE() { for n in 0 1 2; do python3 "$SCRIPT_DIR/readstate.py" persist$n.txt; done; }
SAME() { python3 "$SCRIPT_DIR/readstate.py" persist0.txt persist1.txt persist2.txt --same; }
LEADER_ID() { python3 "$SCRIPT_DIR/readstate.py" persist0.txt persist1.txt persist2.txt --leader | cut -d= -f2; }
PID_OF() { pgrep -x raftCoreRun | sort -n | sed -n "$(( $1 + 2 ))p"; }   # 节点号 -> pid（第 1 个是父进程）
RSS_ALL() { for n in 0 1 2; do printf "%s " "$(ps -o rss= -p "$(PID_OF "$n")" 2>/dev/null | tr -d ' ')"; done; }
# 持续纯写，直到给定节点的快照都达到目标字节数（写入速率随磁盘 fsync 延迟波动，所以不能按固定时长触发快照）
#   $1=目标字节数  $2=key 空间  $3=最多轮数  其余=节点号。第一轮同时验证集群能否正常服务（选不出 leader 会卡住）
WRITE_UNTIL_SNAPSHOT() {
  local target=$1 ksp=$2 maxr=$3 round=0 files="" n line
  shift 3
  for n in "$@"; do files="$files persist$n.txt"; done
  while [ $round -lt "$maxr" ]; do
    round=$((round + 1))
    line=$(timeout 60 ./benchMain -t 4 -d 5 -g 0 -k "$ksp" -f test.conf | grep RESULT | head -1)
    echo "  round$round ${line:-!!! BENCH FAILED (集群无法服务)}"
    line=$(python3 "$SCRIPT_DIR/readstate.py" $files --atleast "$target")
    echo "  round$round $line"
    case "$line" in SNAP_OK*) return 0;; esac
  done
  echo "  !!! 写入 $round 轮后快照仍未达到 $target 字节"
  return 1
}
cp -f "$BIN/raftCoreRun" "$BIN/benchMain" "$BIN/kvRecoverTest" "$BIN/callerMain" "$BIN/provider" "$BIN/consumer" . 2>/dev/null
cp -f "$BIN/kvBigValueTest" . 2>/dev/null

for t in "$@"; do
case "$t" in
t1|t2|t3)
  case "$t" in
    t1) TARGET=1000 KSP=60 MAXR=12;;
    t2) TARGET=50000 KSP=20000 MAXR=20;;
    t3) TARGET=200000 KSP=200000 MAXR=45;;
  esac
  echo "########## $t: kill -9 leader -> 持续写入直到快照 >= $TARGET 字节 -> 重启全集群 -> 旧 leader 应追上"
  FRESH
  START $WORK/fix_${t}a.log
  sleep 14
  LEADER=$(LEADER_ID)
  SURV=""
  for n in 0 1 2; do [ "$n" != "$LEADER" ] && SURV="$SURV $n"; done
  echo "当前 leader=node$LEADER（本次就杀它，顺带验证 leader 故障转移）；存活节点:$SURV"
  timeout 20 ./kvRecoverTest put -f test.conf > /dev/null
  kill -9 "$(PID_OF "$LEADER")"
  echo "--- kill -9 leader 后开始写入："
  if ! WRITE_UNTIL_SNAPSHOT "$TARGET" "$KSP" "$MAXR" $SURV; then STOP_ALL; continue; fi
  echo "--- 掉线结束时两个存活节点："
  for n in $SURV; do python3 "$SCRIPT_DIR/readstate.py" persist$n.txt; done
  STOP_ALL
  sleep 1
  START $WORK/fix_${t}b.log
  sleep 14
  echo "--- 重启 14s 后三个节点（被杀的是 node$LEADER）："
  STATE
  SAME
  echo "--- 再写 10 个 key，1s 后（验证快照之后的日志复制也恢复了）："
  timeout 20 ./kvRecoverTest put -f test.conf > /dev/null
  sleep 1
  STATE
  SAME
  echo "parse_error=$(grep -c 'parse error' $WORK/fix_${t}b.log) assert_or_exit=$(grep -c '^Error' $WORK/fix_${t}b.log) alive=$(pgrep -x raftCoreRun | wc -l)"
  timeout 20 ./kvRecoverTest get -f test.conf | tail -1
  STOP_ALL
  ;;
t4)
  echo "########## t4: 分区愈合（kill -STOP 冻结 leader -> 持续写入直到触发快照 -> 解冻 -> 旧 leader 应退位并追上）"
  FRESH
  START $WORK/fix_t4.log
  sleep 14
  LEADER=$(LEADER_ID)
  SURV=""
  for n in 0 1 2; do [ "$n" != "$LEADER" ] && SURV="$SURV $n"; done
  timeout 20 ./kvRecoverTest put -f test.conf > /dev/null
  echo "当前 leader=node$LEADER，冻结它；存活节点:$SURV"
  kill -STOP "$(PID_OF "$LEADER")"
  echo "--- 冻结期间开始写入（应先故障转移，再继续服务）："
  if ! WRITE_UNTIL_SNAPSHOT 1000 60 12 $SURV; then echo "!!! 解冻"; kill -CONT "$(PID_OF "$LEADER")"; STOP_ALL; continue; fi
  echo "--- 解冻旧 leader，逐秒采样它的状态"
  kill -CONT "$(PID_OF "$LEADER")"
  for i in 1 2 3 4 5 6 7 8 9 10; do
    sleep 1
    echo "+${i}s node$LEADER: $(python3 "$SCRIPT_DIR/readstate.py" persist$LEADER.txt | sed 's/.*| //')"
  done
  echo "--- 最终三个节点："
  STATE
  SAME
  echo "parse_error=$(grep -c 'parse error' $WORK/fix_t4.log) assert_or_exit=$(grep -c '^Error' $WORK/fix_t4.log) alive=$(pgrep -x raftCoreRun | wc -l)"
  timeout 20 ./kvRecoverTest get -f test.conf | tail -1
  STOP_ALL
  ;;
t5)
  echo "########## t5: 常规回归（callerMain / 混合压测 / 崩溃恢复）"
  FRESH
  START $WORK/fix_t5a.log
  sleep 14
  timeout 90 ./callerMain > $WORK/fix_t5_caller.log 2>&1
  echo "callerMain exit=$? success=$(grep -c 'get return' $WORK/fix_t5_caller.log)"
  ./benchMain -t 4 -d 10 -g 80 -k 100 -f test.conf | grep RESULT
  timeout 20 ./kvRecoverTest put -f test.conf > /dev/null
  STOP_ALL
  sleep 1
  START $WORK/fix_t5b.log
  sleep 14
  timeout 20 ./kvRecoverTest get -f test.conf | tail -1
  echo "assert_or_exit=$(grep -c '^Error' $WORK/fix_t5a.log $WORK/fix_t5b.log | tr '\n' ' ')"
  STOP_ALL
  ;;
t6)
  echo "########## t6: leader 故障转移耗时（单线程压测进行到第 3 秒时 kill -9 leader）"
  FRESH
  START $WORK/fix_t6.log
  sleep 14
  LEADER=$(LEADER_ID)
  echo "当前 leader=node$LEADER"
  ./benchMain -t 1 -d 8 -g 0 -k 50 -f test.conf > $WORK/fix_t6_bench.log 2>&1 &
  BPID=$!
  sleep 3
  kill -9 "$(PID_OF "$LEADER")"
  wait $BPID
  grep RESULT $WORK/fix_t6_bench.log || echo "!!! FAILOVER FAILED (bench 未正常结束)"
  sleep 1
  STATE
  echo "assert_or_exit=$(grep -c '^Error' $WORK/fix_t6.log)"
  STOP_ALL
  ;;
t7)
  echo "########## t7: 大值回归（1KB / 64KB / 300KB 的值 put + get）"
  FRESH
  START $WORK/fix_t7.log
  sleep 14
  timeout 60 ./kvBigValueTest -f test.conf > $WORK/fix_t7_client.log 2>&1
  echo "exit=$?"
  grep BIGVALUE $WORK/fix_t7_client.log
  echo "rpc_fail_lines=$(grep -c 'rpc failed\|connect fail' $WORK/fix_t7_client.log) alive=$(pgrep -x raftCoreRun | wc -l)"
  STOP_ALL
  ;;
t8)
  echo "########## t8: RPC 框架独立冒烟（provider 监听 127.0.1.1:7788，consumer 发 10 次请求，每次间隔 5s）"
  pkill -x provider 2>/dev/null
  sleep 1
  setsid ./provider > $WORK/fix_t8_provider.log 2>&1 < /dev/null &
  sleep 1
  timeout 70 ./consumer > $WORK/fix_t8_consumer.log 2>&1
  echo "consumer exit=$? success=$(grep -c 'response success' $WORK/fix_t8_consumer.log)/10"
  pkill -x provider 2>/dev/null
  ;;
t9)
  echo "########## t9: 内存泄漏检查（15s 纯读压测前后三个节点的 RSS，单位 KB）"
  FRESH
  START $WORK/fix_t9.log
  sleep 14
  ./benchMain -t 1 -d 3 -g 100 -k 100 -f test.conf > /dev/null   # 预热
  R0=$(RSS_ALL)
  ./benchMain -t 4 -d 15 -g 100 -k 100 -f test.conf | grep RESULT
  R1=$(RSS_ALL)
  echo "rss_before_kb: $R0"
  echo "rss_after_kb:  $R1"
  python3 - "$R0" "$R1" <<'EOF'
import sys
b = [int(x) for x in sys.argv[1].split()]
a = [int(x) for x in sys.argv[2].split()]
print("rss_delta_kb:  " + " ".join(str(y - x) for x, y in zip(b, a)) + "   (node0 node1 node2)")
EOF
  STOP_ALL
  ;;
esac
done
echo done
