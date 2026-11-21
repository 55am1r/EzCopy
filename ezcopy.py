#!/usr/bin/env python3
"""EzCopy - fast, verified file copy for Mac and Windows. Pure standard library.

  python3 ezcopy.py                          -> opens the window
  python3 ezcopy.py SRC [SRC ...] DEST       -> command line (exit code 1 if anything failed)
      --no-verify   skip the read-back check (faster, NOT recommended)
      --workers N   parallel small-file copies (default 8)

How a file is copied (this is the whole safety story):
  1. read source in 8 MB chunks, hashing (SHA-256) the bytes as they are read
  2. write them to  <name>.ezcopy-part  next to the final name, then fsync
  3. re-read the written file (bypassing the OS cache where possible) and compare hashes
  4. only if equal: atomically rename to the final name. Any mismatch -> retry once, then report FAILED
So a file with its final name was always verified; a crash leaves only *.ezcopy-part. Source is never touched.
"""
import argparse
import hashlib
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

BUF = 8 << 20                 # 8 MB chunks
BIG = 256 << 20               # files >= this are copied at most 2 at a time (drive-bound, not overhead-bound)
MTIME_TOL_NS = 2_000_000_000  # exFAT/FAT store mtime with 2 s granularity
PART = ".ezcopy-part"


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


def hash_file(path, cancel=None, prog=None):
    """SHA-256 of a file, reading around the OS cache so the disk itself is checked.
    ponytail: Windows has no cache bypass here (needs aligned unbuffered I/O), so on Windows
    the read-back can be served from RAM; it still catches copy/short-write errors."""
    h = hashlib.sha256()
    buf = bytearray(BUF)
    mv = memoryview(buf)
    with open(path, "rb", buffering=0) as f:
        if sys.platform == "darwin":
            import fcntl
            fcntl.fcntl(f.fileno(), getattr(fcntl, "F_NOCACHE", 48), 1)
        elif hasattr(os, "posix_fadvise"):
            os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
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
    def __init__(self, total_work, total_files):
        self.lock = threading.Lock()
        self.total, self.total_files = total_work, total_files
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

    @property
    def ok(self):
        return not self.failed and not self.cancelled


def _up_to_date(s, d):
    try:
        ss, ds = os.stat(s), os.stat(d)
    except OSError:
        return False
    return ss.st_size == ds.st_size and abs(ss.st_mtime_ns - ds.st_mtime_ns) <= MTIME_TOL_NS


def copy_file(src, dst, verify, cancel, prog):
    """Copy one file safely. Returns sha256 hex. Raises on failure (after one retry)."""
    s, d = lp(src), lp(dst)
    tmp = d + PART
    last = None
    for _ in range(2):
        try:
            h = hashlib.sha256()
            buf = bytearray(BUF)
            mv = memoryview(buf)
            total = 0
            with open(s, "rb", buffering=0) as fi, open(tmp, "wb") as fo:
                expected = os.fstat(fi.fileno()).st_size
                while True:
                    if cancel.is_set():
                        raise Cancelled()
                    n = fi.readinto(buf)
                    if not n:
                        break
                    h.update(mv[:n])
                    fo.write(mv[:n])          # buffered writer: loops until every byte is written
                    total += n
                    prog.add(n, copied=True)
                fo.flush()
                os.fsync(fo.fileno())
            if total != expected or os.path.getsize(tmp) != total:
                raise OSError(f"size mismatch (read {total}, expected {expected}, wrote {os.path.getsize(tmp)})")
            st = os.stat(s)               # timestamps only: copying xattrs makes ._junk files on exFAT/Windows drives
            os.utime(tmp, ns=(st.st_atime_ns, st.st_mtime_ns))
            digest = h.hexdigest()
            if verify and hash_file(tmp, cancel, prog) != digest:
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


def run(sources, dest, verify=True, workers=8, progress_cb=None, cancel=None):
    """Copy sources into dest. progress_cb(Progress) is called once, right after planning."""
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
    os.makedirs(lp(dest), exist_ok=True)
    for d in dirs:
        os.makedirs(lp(d), exist_ok=True)
        _drop_sidecar(lp(d))
    prog = Progress(need * (2 if verify else 1), len(todo))
    if progress_cb:
        progress_cb(prog)
    manifest, lock = [], threading.Lock()

    def job(item):
        s, d, size = item
        if cancel.is_set():
            return
        try:
            digest = copy_file(s, d, verify, cancel, prog)
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

    small = [f for f in todo if f[2] < BIG]
    big = [f for f in todo if f[2] >= BIG]
    try:
        for group, n in ((small, workers), (big, min(2, workers))):
            if group:
                with ThreadPoolExecutor(max(1, n)) as ex:
                    list(ex.map(job, group))
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
            f.write(f"EzCopy {time.ctime()}\ndest: {dest}\nverify: {verify}\n")
            f.write(f"copied: {rep.copied}  skipped: {rep.skipped}  failed: {len(rep.failed)}  cancelled: {rep.cancelled}\n")
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
    return f"{s // 60}m {s % 60:02d}s" if s >= 60 else f"{s}s"


def summary(rep):
    head = "CANCELLED" if rep.cancelled else ("ALL COPIED AND VERIFIED" if rep.ok else "FINISHED WITH ERRORS")
    return (f"{head}: {rep.copied} copied ({fmt_size(rep.bytes)}), {rep.skipped} already there, "
            f"{len(rep.failed)} failed, in {fmt_time(rep.seconds)}"
            + (f" ({fmt_size(rep.bytes / rep.seconds)}/s)" if rep.seconds > 0.5 and rep.bytes else ""))


# ----------------------------------------------------------------------------- command line
def cli(argv):
    ap = argparse.ArgumentParser(prog="ezcopy", description="Fast verified copy.")
    ap.add_argument("paths", nargs="+", help="SRC [SRC ...] DEST")
    ap.add_argument("--no-verify", action="store_true")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args(argv)
    if len(a.paths) < 2:
        ap.error("need at least one SRC and a DEST")
    holder = {}
    done = threading.Event()

    def printer():
        while not done.wait(0.5):
            p = holder.get("p")
            if p:
                print(f"\r{100 * p.work / max(p.total, 1):5.1f}%  {p.files}/{p.total_files} files  "
                      f"{fmt_size(p.copied / max(time.time() - p.t0, 1e-3))}/s   ", end="", flush=True)
    threading.Thread(target=printer, daemon=True).start()
    try:
        rep = run(a.paths[:-1], a.paths[-1], not a.no_verify, a.workers, lambda p: holder.update(p=p))
    except (EzCopyError, OSError) as e:
        done.set()
        print(f"\nERROR: {e}")
        return 2
    done.set()
    print("\r" + summary(rep) + " " * 20)
    for p, why in rep.failed:
        print(f"  FAILED {p}: {why}")
    for n in rep.notes:
        print(f"  note: {n}")
    if rep.log:
        print(f"  log: {rep.log}")
    return 0 if rep.ok else 1


# ----------------------------------------------------------------------------- window
def gui():
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title("EzCopy")
    root.geometry("680x600")
    root.minsize(560, 480)
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
    workers = tk.IntVar(value=8)
    ttk.Spinbox(opt, from_=1, to=32, width=3, textvariable=workers).pack(side="right")
    ttk.Label(opt, text="Parallel files:").pack(side="right", padx=4)

    go = ttk.Button(root, text="START COPY")
    go.pack(fill="x", padx=10, pady=6)
    bar = ttk.Progressbar(root, maximum=100)
    bar.pack(fill="x", padx=10)
    status = tk.StringVar(value="Ready.")
    ttk.Label(root, textvariable=status).pack(anchor="w", **pad)
    log = tk.Text(root, height=8, state="disabled")
    log.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def say(text):
        log.configure(state="normal")
        log.insert("end", text + "\n")
        log.see("end")
        log.configure(state="disabled")

    def work(srcs, dst, ver, n):        # runs off the main thread: must not touch Tk variables
        try:
            state["rep"] = run(srcs, dst, ver, n, lambda p: state.update(prog=p), state["cancel"])
        except Exception as e:
            state["err"] = e

    def poll():
        p = state["prog"]
        if p:
            el = max(time.time() - p.t0, 1e-3)
            bar["value"] = 100 * p.work / max(p.total, 1)
            rate = p.work / el
            eta = (p.total - p.work) / rate if rate else 0
            status.set(f"{'Copying + verifying' if verify.get() else 'Copying'}: {p.files}/{p.total_files} files   "
                       f"{fmt_size(p.copied / el)}/s   about {fmt_time(eta)} left")
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
        for pth, why in rep.failed:
            say(f"FAILED {pth}: {why}")
        for n in rep.notes:
            say(f"note: {n}")
        if rep.log:
            say(f"Log with checksums: {rep.log}")
        (messagebox.showinfo if rep.ok else messagebox.showwarning)("EzCopy", summary(rep))

    def start():
        nonlocal thread
        if not sources or not dest.get().strip():
            messagebox.showinfo("EzCopy", "Add something to copy and choose a destination first.")
            return
        state.update(prog=None, rep=None, err=None, cancel=threading.Event())
        bar["value"] = 0
        status.set("Scanning files…")
        try:
            n = max(1, workers.get())
        except tk.TclError:
            n = 8
        thread = threading.Thread(target=work, args=(list(sources), dest.get().strip(), verify.get(), n), daemon=True)
        thread.start()
        go.configure(text="CANCEL", command=state["cancel"].set)
        poll()

    thread = threading.Thread()
    go.configure(command=start)
    root.mainloop()


if __name__ == "__main__":
    sys.exit(cli(sys.argv[1:]) if len(sys.argv) > 1 else gui())
