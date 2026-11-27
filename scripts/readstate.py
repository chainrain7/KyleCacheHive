import struct
import sys


# 解析 persist{me}.txt：[8B 小端 raftstate 长度][raftstate][snapshot]
# raftstate 是 boost text archive，token[5..8] = term / votedFor / snapIdx / snapTerm，token[11] = 日志条数
def parse(path):
    f = open(path, "rb").read()
    n = struct.unpack("<Q", f[:8])[0]
    t = f[8:8 + n].decode(errors="replace").split()
    return {"term": t[5], "votedFor": t[6], "snapIdx": t[7], "snapTerm": t[8], "nlogs": t[11],
            "raftstate": n, "snapshot": len(f) - 8 - n, "file": len(f)}


args = sys.argv[1:]
paths = [a for a in args if a.endswith(".txt")]
rows = [parse(p) for p in paths]
if "--leader" in args:
    votes = set(r["votedFor"] for r in rows)
    terms = set(r["term"] for r in rows)
    print("LEADER=%s" % (votes.pop() if len(votes) == 1 and len(terms) == 1 else "unknown"))
elif "--atleast" in args:
    need = int(args[args.index("--atleast") + 1])
    idx = min(int(r["snapIdx"]) for r in rows)
    by = min(r["snapshot"] for r in rows)
    print("%s snapIdx_min=%d bytes_min=%d" % ("SNAP_OK" if idx > 0 and by >= need else "SNAP_LOW", idx, by))
elif "--same" in args:
    snaps = sorted(set((r["snapIdx"], r["snapshot"]) for r in rows))
    logs = sorted(set(r["nlogs"] for r in rows))
    print("SAME(snapshot)=%s %s   SAME(nlogs)=%s nlogs=%s" % (len(snaps) == 1, snaps, len(logs) == 1, logs))
else:
    for p, r in zip(paths, rows):
        print("%-14s file=%-8d raftstate=%-8d snapshot=%-8d | term=%s votedFor=%s snapIdx=%s snapTerm=%s nlogs=%s" % (
            p, r["file"], r["raftstate"], r["snapshot"], r["term"], r["votedFor"], r["snapIdx"], r["snapTerm"], r["nlogs"]))
