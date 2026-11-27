set(SRC_LIST raftKvDB.cpp)


add_executable(raftCoreRun ${SRC_LIST})
target_link_libraries(raftCoreRun skip_list_on_raft rpc_lib protobuf muduo_net muduo_base pthread )

#################################

set(SRC_LIST2 caller.cpp)
add_executable(callerMain ${src_raftClerk} ${SRC_LIST2}  ${src_common})
target_link_libraries(callerMain skip_list_on_raft  protobuf boost_serialization )

#################################

# 压测客户端：混合负载统计 QPS 与延迟分位数
set(SRC_LIST3 bench.cpp)
add_executable(benchMain ${src_raftClerk} ${SRC_LIST3} ${src_common})
target_link_libraries(benchMain skip_list_on_raft protobuf boost_serialization)

#################################

# 崩溃恢复验证：put 写入 -> kill -9 集群 -> 重启 -> get 校验
set(SRC_LIST4 kvRecoverTest.cpp)
add_executable(kvRecoverTest ${src_raftClerk} ${SRC_LIST4} ${src_common})
target_link_libraries(kvRecoverTest skip_list_on_raft protobuf boost_serialization)

#################################

# 大值回归：验证 RPC 分帧（超过单个 TCP 段与旧 4KB 接收缓冲区的值能写入并读回）
set(SRC_LIST5 kvBigValueTest.cpp)
add_executable(kvBigValueTest ${src_raftClerk} ${SRC_LIST5} ${src_common})
target_link_libraries(kvBigValueTest skip_list_on_raft protobuf boost_serialization)