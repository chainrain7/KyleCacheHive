"""fsync 探针：模拟 Persister::AtomicWriteFile 的落盘模式（写临时文件 -> fsync -> rename -> fsync 目录），
测出一次落盘的耗时，用于判断写路径延迟是否由落盘主导。只写 WSL 家目录下的临时文件，结束后清理。"""
import os
import statistics
import time

home = os.path.expanduser("~")
target = os.path.join(home, "fsync_probe_target.tmp")
tmp = target + ".tmp"
dir_fd = os.open(home, os.O_RDONLY | os.O_DIRECTORY)
payload = b"x" * 8192


def one_round(fsync_file, fsync_dir):
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    os.write(fd, payload)
    if fsync_file:
        os.fsync(fd)
    os.close(fd)
    os.rename(tmp, target)
    if fsync_dir:
        os.fsync(dir_fd)


for label, ff, fd_ in (("no fsync        ", False, False), ("fsync file only ", True, False), ("fsync file + dir", True, True)):
    for _ in range(30):
        one_round(ff, fd_)  # 预热
    samples = []
    for _ in range(300):
        t = time.perf_counter()
        one_round(ff, fd_)
        samples.append((time.perf_counter() - t) * 1000)
    samples.sort()
    print("%s: median=%.3fms  p99=%.3fms  mean=%.3fms" % (label, statistics.median(samples), samples[int(len(samples) * 0.99) - 1], statistics.mean(samples)))

os.close(dir_fd)
for p in (tmp, target):
    if os.path.exists(p):
        os.remove(p)
