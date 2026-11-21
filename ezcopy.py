#!/usr/bin/env python3
"""EzCopy - fast, verified file copy for Mac and Windows. Pure standard library.

  python3 ezcopy.py                          -> opens the window
  python3 ezcopy.py SRC [SRC ...] DEST       -> command line (exit code 1 if anything failed)
      --no-verify   skip the read-back check (faster, NOT recommended)
      --workers N   parallel small-file copies (default 8)
      --drives M    auto (default) | hdd | ssd  -> how big files are scheduled, see "Speed" below

How a file is copied (this is the whole safety story):
  1. read source in 8 MB chunks, hashing (SHA-256) the bytes as they are read
  2. write them to  <name>.ezcopy-part  next to the final name, then fsync
  3. re-read the written file (bypassing the OS cache where possible) and compare hashes
  4. only if equal: atomically rename to the final name. Any mismatch -> retry once, then report FAILED
So a file with its final name was always verified; a crash leaves only *.ezcopy-part. Source is never touched.

Speed:
  - A helper thread reads the next chunk while the current one is written, so both drives work at once.
  - A hard disk is fastest when its head moves straight through one file. So when either drive is a hard
    disk (or the system can't tell), files of 16 MB and up stream ONE at a time, and the flush + read-back
    of one file overlaps the streaming of the next. Smaller files, and everything on SSD/network drives,
    copy in parallel.
"""
import argparse
import collections
import contextlib
import hashlib
import os
import queue
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

BUF = 8 << 20                 # 8 MB chunks
DEPTH = 4                     # reads in flight per file: slow USB/network drives need several queued
BIG = 256 << 20               # SSD/network: files >= this are copied at most 2 at a time (drive-bound)
HDD_BIG = 16 << 20            # hard disks: files >= this stream one at a time (no head seeking between files)
MTIME_TOL_NS = 2_000_000_000  # exFAT/FAT store mtime with 2 s granularity
PART = ".ezcopy-part"
NETWORK_FS = {"smbfs", "afpfs", "nfs", "webdav", "cifs"}
_FREE = contextlib.nullcontext()


class EzCopyError(Exception):
    pass


class Cancelled(Exception):
    pass


def lp(p):
    """Absolute path; on Windows add the \\\\?\\ prefix so paths over 260 chars work."""
    p = os.path.abspath(p)
    if os.name != "nt" or p.startswith("\\\\?\\"):
        return p
    return "\\\\?\\UNC\\" + p[2:] if p.startswith("\\\\") else "\\\\?\\" + p


def _open_read(path):
    """Open for reading. On Windows, flag it as a sequential scan: bigger read-ahead, less cache churn."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_SEQUENTIAL", 0))
    return open(fd, "rb", buffering=0)


def _no_cache(f):
    """Make reads of f come from the disk, not from RAM (Mac/Linux; Windows can't, see hash_file)."""
    if sys.platform == "darwin":
        import fcntl
        fcntl.fcntl(f.fileno(), getattr(fcntl, "F_NOCACHE", 48), 1)
    elif hasattr(os, "posix_fadvise"):
        os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)


def _chunks(path, size, nocache=False):
    """Yield the first `size` bytes of a file as memoryviews, in order, with up to DEPTH reads in flight
    (each reader thread has its own handle). One read at a time leaves a USB/network drive idle between
    requests: measured 19 MB/s vs 37 MB/s on a USB hard disk. A chunk is only valid until the next one
    is requested. Use with contextlib.closing() so the reader threads always stop."""
    nchunks = -(-size // BUF)
    readers = max(1, min(DEPTH, nchunks))
    free = queue.Queue()
    for _ in range(readers + 1):               # +1: the consumer holds one while the readers fill the rest
        free.put(bytearray(BUF))
    done, cv, nxt, err, stop = {}, threading.Condition(), [0], [], threading.Event()

    def reader():
        try:
            with _open_read(path) as f:
                if nocache:
                    _no_cache(f)
                while not stop.is_set():
                    buf = free.get()           # take a buffer BEFORE a chunk number, so the lowest
                    if buf is None or stop.is_set():   # unfinished chunk always has a buffer: no deadlock
                        return
                    with cv:
                        i = nxt[0]
                        nxt[0] += 1
                    if i >= nchunks:
                        return
                    want, got, mv = min(BUF, size - i * BUF), 0, memoryview(buf)
                    f.seek(i * BUF)
                    while got < want:
                        k = f.readinto(mv[got:want])
                        if not k:
                            raise OSError("file got shorter while it was being read")
                        got += k
                    with cv:
                        done[i] = (buf, got)
                        cv.notify_all()
        except BaseException as e:             # handed to the consumer
            with cv:
                err.append(e)
                cv.notify_all()

    ts = [threading.Thread(target=reader, daemon=True) for _ in range(readers)]
    for t in ts:
        t.start()
    try:
        for i in range(nchunks):
            with cv:
                while i not in done and not err:
                    cv.wait()
                if i not in done:
                    raise err[0]
                buf, n = done.pop(i)
            yield memoryview(buf)[:n]
            free.put(buf)
    finally:
        stop.set()
        for _ in ts:
            free.put(None)                     # wakes readers waiting for a buffer
        for t in ts:
            t.join()


def hash_file(path, cancel=None, prog=None):
    """SHA-256 of a file, reading around the OS cache so the disk itself is checked.
    ponytail: Windows has no cache bypass here (needs aligned unbuffered I/O), so on Windows
    the read-back can be served from RAM; it still catches copy/short-write errors."""
    h = hashlib.sha256()
    size = os.path.getsize(path)
    if size > BUF:
        with contextlib.closing(_chunks(path, size, nocache=True)) as chunks:
            for mv in chunks:
                if cancel is not None and cancel.is_set():
                    raise Cancelled()
                h.update(mv)
                if prog:
                    prog.add(len(mv))
        if os.path.getsize(path) != size:
            raise OSError("file size changed while it was being checked")
        return h.hexdigest()
    with _open_read(path) as f:
        _no_cache(f)
        buf = bytearray(size + 1)              # small file -> small buffer; +1 sees growth / EOF
        mv = memoryview(buf)
        while True:
            if cancel is not None and cancel.is_set():
                raise Cancelled()
            n = f.readinto(buf)
            if not n:
                break
            h.update(mv[:n])
            if prog:
                prog.add(n)
    return h.hexdigest()


class Progress:
    def __init__(self, total_work, total_files, mode=""):
        self.lock = threading.Lock()
        self.total, self.total_files, self.mode = total_work, total_files, mode
        self.work = self.copied = self.files = 0
        self.t0 = time.time()

    def add(self, n, copied=False):
        with self.lock:
            self.work += n
            if copied:
                self.copied += n


@dataclass
class Report:
    copied: int = 0
    skipped: int = 0
    bytes: int = 0
    failed: list = field(default_factory=list)   # (path, reason)
    notes: list = field(default_factory=list)
    seconds: float = 0.0
    cancelled: bool = False
    log: str = ""
    drives: str = ""

    @property
    def ok(self):
        return not self.failed and not self.cancelled


def _up_to_date(s, d):
    try:
        ss, ds = os.stat(s), os.stat(d)
    except OSError:
        return False
    return ss.st_size == ds.st_size and abs(ss.st_mtime_ns - ds.st_mtime_ns) <= MTIME_TOL_NS


def _copy_small(fi, fo, h, cancel, prog, size):
    """One thread: read, hash, write. For files that fit in one chunk."""
    buf = bytearray(min(BUF, size + 1))
    mv = memoryview(buf)
    total = 0
    while True:
        if cancel.is_set():
            raise Cancelled()
        n = fi.readinto(buf)
        if not n:
            break
        h.update(mv[:n])
        fo.write(mv[:n])                      # buffered writer: loops until every byte is written
        total += n
        prog.add(n, copied=True)
    return total


def _stream(src, fo, h, cancel, prog, size):
    """Copy `size` bytes of src -> fo, hashing every chunk. Reader threads keep the source drive busy;
    this thread hashes and writes (the OS flushes writes to the destination in the background meanwhile)."""
    total = 0
    with contextlib.closing(_chunks(src, size)) as chunks:
        for mv in chunks:
            if cancel.is_set():
                raise Cancelled()
            h.update(mv)
            fo.write(mv)
            total += len(mv)
            prog.add(len(mv), copied=True)
    return total


def copy_file(src, dst, verify, cancel, prog, gate=None, verify_gate=None):
    """Copy one file safely. Returns sha256 hex. Raises on failure (after one retry).
    gate: held only while bytes stream source -> destination. On hard disks it is one shared lock, so
    one file streams at a time while the previous file is flushed and read back."""
    s, d = lp(src), lp(dst)
    tmp = d + PART
    gate, verify_gate = gate or _FREE, verify_gate or _FREE
    last = None
    for _ in range(2):
        try:
            h = hashlib.sha256()
            fo = None
            try:
                with gate:
                    with _open_read(s) as fi:
                        fo = open(tmp, "wb")
                        expected = os.fstat(fi.fileno()).st_size
                        if expected > BUF:
                            total = _stream(s, fo, h, cancel, prog, expected)
                        else:
                            total = _copy_small(fi, fo, h, cancel, prog, expected)
                        if os.fstat(fi.fileno()).st_size != expected:
                            raise OSError("source file changed while it was being copied")
                        fo.flush()
                os.fsync(fo.fileno())         # outside the gate: the next file streams meanwhile
            finally:
                if fo is not None:
                    fo.close()
            if total != expected or os.path.getsize(tmp) != total:
                raise OSError(f"size mismatch (read {total}, expected {expected}, wrote {os.path.getsize(tmp)})")
            st = os.stat(s)               # timestamps only: copying xattrs makes ._junk files on exFAT/Windows drives
            os.utime(tmp, ns=(st.st_atime_ns, st.st_mtime_ns))
            digest = h.hexdigest()
            if verify:
                with verify_gate:
                    same = hash_file(tmp, cancel, prog) == digest
                if not same:
                    raise OSError("verify failed: copy on disk differs from source")
            os.replace(tmp, d)
            _drop_sidecar(d)
            return digest
        except Cancelled:
            _rm(tmp)
            raise
        except Exception as e:                # retry once, then give up loudly
            last = e
            _rm(tmp)
    raise OSError(str(last))


def _rm(p):
    try:
        os.remove(p)
    except OSError:
        pass


def _drop_sidecar(p):
    """macOS tags new files with an xattr; on exFAT/FAT that shows up as a visible ._name file on
    Windows. We write the file ourselves, so any ._name beside it is only that tag: remove it."""
    if sys.platform == "darwin":
        _rm(os.path.join(os.path.dirname(p), "._" + os.path.basename(p)))


# ----------------------------------------------------------------------------- what kind of drive
def drive_kind(path):
    """'hdd', 'ssd', 'network', or None when the system won't say (EzCopy then plays safe: hard disk)."""
    try:
        if os.name == "nt":
            return _kind_windows(path)
        if sys.platform == "darwin":
            return _kind_mac(path)
    except Exception:
        pass
    return None


def _kind_mac(path):
    import plistlib
    import subprocess
    rp = os.path.realpath(path)
    dev, mnt, fs = "", "", ""
    for line in subprocess.run(["/sbin/mount"], capture_output=True, text=True, timeout=10).stdout.splitlines():
        d, sep, rest = line.partition(" on ")          # "/dev/disk4s2 on /Volumes/X (exfat, local, ...)"
        m, _, opts = rest.rpartition(" (")
        if sep and (rp == m or rp.startswith(m.rstrip("/") + "/")) and len(m) > len(mnt):
            dev, mnt, fs = d, m, opts.split(",")[0]
    if fs in NETWORK_FS:
        return "network"
    if not dev.startswith("/dev/"):
        return None
    out = subprocess.run(["diskutil", "info", "-plist", dev], capture_output=True, timeout=20).stdout
    solid = plistlib.loads(out).get("SolidState")
    return None if solid is None else ("ssd" if solid else "hdd")


def _kind_windows(path):
    import ctypes
    from ctypes import wintypes
    p = os.path.abspath(path)
    if p.startswith("\\\\"):                            # \\server\share
        return "network"
    drive = os.path.splitdrive(p)[0]                    # "D:"
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.GetDriveTypeW.argtypes = [wintypes.LPCWSTR]
    if k32.GetDriveTypeW(drive + "\\") == 4:            # DRIVE_REMOTE: mapped network drive
        return "network"
    k32.CreateFileW.restype = wintypes.HANDLE
    k32.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                                wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    k32.DeviceIoControl.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD,
                                    wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
    k32.CloseHandle.argtypes = [wintypes.HANDLE]
    # access 0 = ask questions only; works without administrator rights. 3 = share read+write, 3 = OPEN_EXISTING
    h = k32.CreateFileW("\\\\.\\" + drive, 0, 3, None, 3, 0, None)
    if not h or h == ctypes.c_void_p(-1).value:
        return None
    try:
        class Query(ctypes.Structure):                  # STORAGE_PROPERTY_QUERY
            _fields_ = [("PropertyId", ctypes.c_int), ("QueryType", ctypes.c_int), ("Extra", ctypes.c_ubyte * 1)]

        class SeekPenalty(ctypes.Structure):            # DEVICE_SEEK_PENALTY_DESCRIPTOR
            _fields_ = [("Version", wintypes.DWORD), ("Size", wintypes.DWORD), ("IncursSeekPenalty", wintypes.BOOLEAN)]

        q, out, got = Query(7, 0), SeekPenalty(), wintypes.DWORD()   # 7 = StorageDeviceSeekPenaltyProperty
        if not k32.DeviceIoControl(h, 0x2D1400, ctypes.byref(q), ctypes.sizeof(q),  # IOCTL_STORAGE_QUERY_PROPERTY
                                   ctypes.byref(out), ctypes.sizeof(out), ctypes.byref(got), None):
            return None
        return "hdd" if out.IncursSeekPenalty else "ssd"
    finally:
        k32.CloseHandle(h)


# ----------------------------------------------------------------------------- planning and running
def plan(sources, dest):
    """-> (files [(src, dst, size)], dirs [dst dirs], notes). Folders keep their own name, like Finder."""
    files, dirs, notes = [], [], []
    for src in sources:
        src = os.path.abspath(src)
        base = os.path.basename(src) or src.replace(":", "").strip("\\/")
        if os.path.islink(src):
            notes.append(f"skipped symlink: {src}")
        elif os.path.isfile(src):
            files.append((src, os.path.join(dest, base), os.path.getsize(src)))
        elif os.path.isdir(src):
            real_s, real_d = os.path.realpath(src), os.path.realpath(dest)
            if real_d == real_s or real_d.startswith(real_s + os.sep):
                raise EzCopyError(f"Destination is inside the source folder: {src}")
            root = lp(src)
            dirs.append(os.path.join(dest, base))
            for r, dn, fn in os.walk(root):
                rel = os.path.relpath(r, root)
                out = os.path.join(dest, base) if rel == "." else os.path.join(dest, base, rel)
                for x in list(dn):
                    if os.path.islink(os.path.join(r, x)):
                        notes.append(f"skipped symlink: {os.path.join(r, x)}")
                        dn.remove(x)
                    else:
                        dirs.append(os.path.join(out, x))
                for x in fn:
                    p = os.path.join(r, x)
                    if os.path.islink(p) or not os.path.isfile(p):
                        notes.append(f"skipped {'symlink' if os.path.islink(p) else 'special file'}: {p}")
                        continue
                    files.append((p, os.path.join(out, x), os.path.getsize(p)))
        else:
            notes.append(f"not found: {src}")
    return files, dirs, notes


def run(sources, dest, verify=True, workers=8, progress_cb=None, cancel=None, drives="auto"):
    """Copy sources into dest. progress_cb(Progress) is called once, right after planning.
    drives: 'auto' (detect), 'hdd' (big files one at a time) or 'ssd' (big files two at a time)."""
    cancel = cancel or threading.Event()
    t0 = time.time()
    rep = Report()
    dest = os.path.abspath(dest)
    files, dirs, rep.notes = plan(sources, dest)
    if any(os.path.abspath(s) == os.path.abspath(d) for s, d, _ in files):
        raise EzCopyError("Source and destination are the same file.")
    todo = []
    for s, d, size in files:
        if _up_to_date(lp(s), lp(d)):
            rep.skipped += 1
        else:
            todo.append((s, d, size))
    need = sum(x[2] for x in todo)
    probe = dest
    while not os.path.exists(probe):
        probe = os.path.dirname(probe) or "."
    free = shutil.disk_usage(probe).free
    if need + (64 << 20) > free:
        raise EzCopyError(f"Not enough space on destination: need {need / 2**30:.2f} GB, free {free / 2**30:.2f} GB")

    src_kinds = sorted({drive_kind(s) or "unknown" for s in sources if os.path.exists(s)})
    dst_kind = drive_kind(probe) or "unknown"
    hdd = drives == "hdd" or (drives == "auto" and bool({dst_kind, *src_kinds} & {"hdd", "unknown"}))
    mode = "hard-disk mode" if hdd else "parallel mode"
    rep.drives = f"from {'+'.join(src_kinds) or '?'} to {dst_kind}: {mode}"

    os.makedirs(lp(dest), exist_ok=True)
    for d in dirs:
        os.makedirs(lp(d), exist_ok=True)
        _drop_sidecar(lp(d))
    prog = Progress(need * (2 if verify else 1), len(todo), mode)
    if progress_cb:
        progress_cb(prog)
    manifest, lock = [], threading.Lock()

    def job(item, gate, verify_gate):
        s, d, size = item
        if cancel.is_set():
            return
        try:
            digest = copy_file(s, d, verify, cancel, prog, gate, verify_gate)
            with lock:
                rep.copied += 1
                rep.bytes += size
                manifest.append((digest, d))
        except Cancelled:
            return
        except Exception as e:
            with lock:
                rep.failed.append((s, str(e)))
        finally:
            with prog.lock:
                prog.files += 1

    if hdd:
        # one file streams at a time (gate); the 2nd worker flushes + verifies the previous file meanwhile.
        # On a Mac the read-back really reads the disk, so if the destination may be a hard disk, keep that
        # inside the gate too (it would make the destination head jump between two files).
        gate = threading.Lock()
        vgate = gate if (sys.platform == "darwin" and dst_kind != "ssd") else None
        cut, groups = HDD_BIG, ((workers, None, None), (2, gate, vgate))
    else:
        cut, groups = BIG, ((workers, None, None), (min(2, workers), None, None))
    small = [f for f in todo if f[2] < cut]
    big = [f for f in todo if f[2] >= cut]
    try:
        for group, (n, gate, vgate) in zip((small, big), groups):
            if group:
                with ThreadPoolExecutor(max(1, n)) as ex:
                    list(ex.map(lambda it: job(it, gate, vgate), group))
    except KeyboardInterrupt:
        cancel.set()
        raise
    rep.cancelled = cancel.is_set()
    rep.seconds = time.time() - t0
    rep.log = _write_log(rep, manifest, dest, verify)
    return rep


def _write_log(rep, manifest, dest, verify):
    try:
        folder = os.path.join(os.path.expanduser("~"), "EzCopy Logs")
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, time.strftime("ezcopy-%Y%m%d-%H%M%S.txt"))
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"EzCopy {time.ctime()}\ndest: {dest}\nverify: {verify}\ndrives: {rep.drives}\n")
            f.write(f"copied: {rep.copied}  skipped: {rep.skipped}  failed: {len(rep.failed)}  cancelled: {rep.cancelled}"
                    f"  time: {fmt_time(rep.seconds)}\n")
            for p, why in rep.failed:
                f.write(f"FAILED  {p}  ({why})\n")
            for n in rep.notes:
                f.write(f"NOTE    {n}\n")
            f.write("\n# sha256  destination path\n")
            for digest, d in sorted(manifest, key=lambda x: x[1]):
                f.write(f"{digest}  {d}\n")
        return path
    except OSError:
        return ""


def fmt_size(n):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024


def fmt_time(s):
    s = int(s)
    if s >= 3600:
        return f"{s // 3600}h {s % 3600 // 60:02d}m"
    return f"{s // 60}m {s % 60:02d}s" if s >= 60 else f"{s}s"


def summary(rep):
    head = "CANCELLED" if rep.cancelled else ("ALL COPIED AND VERIFIED" if rep.ok else "FINISHED WITH ERRORS")
    return (f"{head}: {rep.copied} copied ({fmt_size(rep.bytes)}), {rep.skipped} already there, "
            f"{len(rep.failed)} failed, in {fmt_time(rep.seconds)}"
            + (f" ({fmt_size(rep.bytes / rep.seconds)}/s)" if rep.seconds > 0.5 and rep.bytes else ""))


class Speedo:
    """Speed and time-left over the last ~30 s, not since the start: the OS cache makes the first
    seconds look far faster than the drives really are."""
    def __init__(self, window=30.0):
        self.window, self.hist = window, collections.deque()

    def read(self, p):
        now = time.time()
        self.hist.append((now, p.work, p.copied))
        while now - self.hist[0][0] > self.window:
            self.hist.popleft()
        t0, w0, c0 = self.hist[0] if now - self.hist[0][0] >= 3 else (p.t0, 0, 0)
        el = max(now - t0, 1e-3)
        rate = (p.work - w0) / el
        return (p.copied - c0) / el, ((p.total - p.work) / rate if rate > 0 else 0)


# ----------------------------------------------------------------------------- command line
def cli(argv):
    ap = argparse.ArgumentParser(prog="ezcopy", description="Fast verified copy.")
    ap.add_argument("paths", nargs="+", help="SRC [SRC ...] DEST")
    ap.add_argument("--no-verify", action="store_true")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--drives", choices=("auto", "hdd", "ssd"), default="auto")
    a = ap.parse_args(argv)
    if len(a.paths) < 2:
        ap.error("need at least one SRC and a DEST")
    holder = {}
    done = threading.Event()

    def printer():
        sp = Speedo()
        while not done.wait(0.5):
            p = holder.get("p")
            if p:
                speed, eta = sp.read(p)
                print(f"\r{100 * p.work / max(p.total, 1):5.1f}%  {p.files}/{p.total_files} files  "
                      f"{fmt_size(speed)}/s  about {fmt_time(eta)} left   ", end="", flush=True)
    threading.Thread(target=printer, daemon=True).start()
    try:
        rep = run(a.paths[:-1], a.paths[-1], not a.no_verify, a.workers, lambda p: holder.update(p=p), drives=a.drives)
    except (EzCopyError, OSError) as e:
        done.set()
        print(f"\nERROR: {e}")
        return 2
    done.set()
    print("\r" + summary(rep) + " " * 20)
    print(f"  drives: {rep.drives}")
    for p, why in rep.failed:
        print(f"  FAILED {p}: {why}")
    for n in rep.notes:
        print(f"  note: {n}")
    if rep.log:
        print(f"  log: {rep.log}")
    return 0 if rep.ok else 1


# ----------------------------------------------------------------------------- window
DRIVE_CHOICES = {"Auto-detect": "auto", "Hard disks (HDD)": "hdd", "SSD / network": "ssd"}


def gui():
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title("EzCopy")
    root.geometry("680x630")
    root.minsize(560, 500)
    pad = {"padx": 10, "pady": 4}
    sources, state = [], {"prog": None, "rep": None, "err": None, "cancel": threading.Event()}

    ttk.Label(root, text="1. Copy FROM  (files and/or folders)", font=("", 11, "bold")).pack(anchor="w", **pad)
    box = tk.Listbox(root, height=6, selectmode="extended")
    box.pack(fill="x", padx=10)
    row = ttk.Frame(root)
    row.pack(fill="x", **pad)

    def add(paths):
        for p in paths:
            if p and p not in sources:
                sources.append(p)
                box.insert("end", p)

    def remove():
        for i in reversed(box.curselection()):
            box.delete(i)
            sources.pop(i)

    def clear():
        box.delete(0, "end")
        sources.clear()

    for text, cmd in (("Add Files…", lambda: add(filedialog.askopenfilenames())),
                      ("Add Folder…", lambda: add([filedialog.askdirectory()])),
                      ("Remove", remove), ("Clear", clear)):
        ttk.Button(row, text=text, command=cmd).pack(side="left", padx=(0, 6))

    ttk.Label(root, text="2. Copy TO  (destination folder)", font=("", 11, "bold")).pack(anchor="w", **pad)
    drow = ttk.Frame(root)
    drow.pack(fill="x", padx=10)
    dest = tk.StringVar()
    ttk.Entry(drow, textvariable=dest).pack(side="left", fill="x", expand=True)
    ttk.Button(drow, text="Browse…", command=lambda: dest.set(filedialog.askdirectory() or dest.get())).pack(side="left", padx=(6, 0))

    opt = ttk.Frame(root)
    opt.pack(fill="x", **pad)
    verify = tk.BooleanVar(value=True)
    ttk.Checkbutton(opt, text="Verify every file after copying (recommended)", variable=verify).pack(side="left")
    opt2 = ttk.Frame(root)
    opt2.pack(fill="x", **pad)
    ttk.Label(opt2, text="Drives:").pack(side="left")
    drives = tk.StringVar(value="Auto-detect")
    ttk.Combobox(opt2, textvariable=drives, values=list(DRIVE_CHOICES), state="readonly", width=17).pack(side="left", padx=4)
    workers = tk.IntVar(value=8)
    ttk.Spinbox(opt2, from_=1, to=32, width=3, textvariable=workers).pack(side="right")
    ttk.Label(opt2, text="Parallel small files:").pack(side="right", padx=4)

    go = ttk.Button(root, text="START COPY")
    go.pack(fill="x", padx=10, pady=6)
    bar = ttk.Progressbar(root, maximum=100)
    bar.pack(fill="x", padx=10)
    status = tk.StringVar(value="Ready.")
    ttk.Label(root, textvariable=status).pack(anchor="w", **pad)
    log = tk.Text(root, height=8, state="disabled")
    log.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    speedo = Speedo()

    def say(text):
        log.configure(state="normal")
        log.insert("end", text + "\n")
        log.see("end")
        log.configure(state="disabled")

    def work(srcs, dst, ver, n, drv):   # runs off the main thread: must not touch Tk variables
        try:
            state["rep"] = run(srcs, dst, ver, n, lambda p: state.update(prog=p), state["cancel"], drv)
        except Exception as e:
            state["err"] = e

    def poll():
        p = state["prog"]
        if p:
            speed, eta = speedo.read(p)
            bar["value"] = 100 * p.work / max(p.total, 1)
            status.set(f"{'Copying + verifying' if verify.get() else 'Copying'} ({p.mode}): "
                       f"{p.files}/{p.total_files} files   {fmt_size(speed)}/s   about {fmt_time(eta)} left")
        if thread.is_alive():
            root.after(250, poll)
            return
        go.configure(text="START COPY", command=start)
        if state["err"]:
            status.set("Could not start.")
            messagebox.showerror("EzCopy", str(state["err"]))
            return
        rep = state["rep"]
        bar["value"] = 100 if rep.ok else bar["value"]
        status.set(summary(rep))
        say(f"Drives: {rep.drives}")
        for pth, why in rep.failed:
            say(f"FAILED {pth}: {why}")
        for n in rep.notes:
            say(f"note: {n}")
        if rep.log:
            say(f"Log with checksums: {rep.log}")
        (messagebox.showinfo if rep.ok else messagebox.showwarning)("EzCopy", summary(rep))

    def start():
        nonlocal thread, speedo
        if not sources or not dest.get().strip():
            messagebox.showinfo("EzCopy", "Add something to copy and choose a destination first.")
            return
        state.update(prog=None, rep=None, err=None, cancel=threading.Event())
        speedo = Speedo()
        bar["value"] = 0
        status.set("Scanning files…")
        try:
            n = max(1, workers.get())
        except tk.TclError:
            n = 8
        thread = threading.Thread(target=work, args=(list(sources), dest.get().strip(), verify.get(), n,
                                                     DRIVE_CHOICES.get(drives.get(), "auto")), daemon=True)
        thread.start()
        go.configure(text="CANCEL", command=state["cancel"].set)
        poll()

    thread = threading.Thread()
    go.configure(command=start)
    root.mainloop()


if __name__ == "__main__":
    sys.exit(cli(sys.argv[1:]) if len(sys.argv) > 1 else gui())
