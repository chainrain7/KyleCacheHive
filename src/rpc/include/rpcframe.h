#ifndef RPCFRAME_H
#define RPCFRAME_H

#include <cstdint>
#include <string>

// RPC 线上传输格式（请求、响应两个方向相同）：[4 字节大端 payload 长度][payload]
//  - 请求 payload：varint(rpcHeader 长度) + RpcHeader + args
//  - 响应 payload：序列化后的 response 消息
// 没有长度前缀时：TCP 半包会让请求被直接丢弃；客户端也无法区分「空响应」和「没有响应」
//（字段全是默认值的消息序列化后是 0 字节，对端什么都收不到，只能等到超时）
namespace rpcframe {

// 单帧上限：对端发来损坏的长度字段时，避免本地按它分配内存
constexpr uint32_t kMaxFrameLength = 64u * 1024 * 1024;

inline void EncodeLength(uint32_t len, char out[4]) {
  out[0] = static_cast<char>((len >> 24) & 0xFF);
  out[1] = static_cast<char>((len >> 16) & 0xFF);
  out[2] = static_cast<char>((len >> 8) & 0xFF);
  out[3] = static_cast<char>(len & 0xFF);
}

inline uint32_t DecodeLength(const char in[4]) {
  return (static_cast<uint32_t>(static_cast<unsigned char>(in[0])) << 24) |
         (static_cast<uint32_t>(static_cast<unsigned char>(in[1])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(in[2])) << 8) |
         static_cast<uint32_t>(static_cast<unsigned char>(in[3]));
}

// 给 payload 加上长度前缀，组成一帧
inline std::string Encode(const std::string &payload) {
  std::string frame;
  frame.reserve(4 + payload.size());
  char header[4];
  EncodeLength(static_cast<uint32_t>(payload.size()), header);
  frame.append(header, 4);
  frame.append(payload);
  return frame;
}

}  // namespace rpcframe

#endif  // RPCFRAME_H
