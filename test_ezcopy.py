"""Self-check: python3 test_ezcopy.py  (no framework). Proves copies are exact AND that corruption is caught."""
import hashlib
import os
import shutil
import tempfile
import threading

import ezcopy


def sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def tree(root):
    out = {}
    for r, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(r, f)
            out[os.path.relpath(p, root)] = sha(p)
    return out


tmp = tempfile.mkdtemp(prefix="ezcopy-test-")
try:
    src, dst = os.path.join(tmp, "Shoot"), os.path.join(tmp, "out")
    os.makedirs(os.path.join(src, "a", "b"))
    os.makedirs(os.path.join(src, "empty"))
    for i in range(200):
        open(os.path.join(src, "a", f"f{i}.bin"), "wb").write(os.urandom(i * 997))
    open(os.path.join(src, "a", "b", "big.bin"), "wb").write(os.urandom(20 << 20))   # > one 8 MB chunk
    open(os.path.join(src, "zero.bin"), "wb").close()                                # empty file
    open(os.path.join(src, "ünï cødé 名前.txt"), "wb").write(b"unicode name")
    try:
        os.symlink(os.path.join(src, "zero.bin"), os.path.join(src, "link"))
        has_link = True
    except OSError:                       # Windows without symlink permission
        has_link = False

    # 1. exact copy, structure, mtime, no leftovers, symlink skipped with a note
    rep = ezcopy.run([src], dst, workers=4)
    assert rep.ok and rep.copied == 203 and not rep.failed, rep
    want = {k: v for k, v in tree(src).items() if k != "link"}
    assert tree(os.path.join(dst, "Shoot")) == want
    assert os.path.isdir(os.path.join(dst, "Shoot", "empty"))
    assert abs(os.stat(os.path.join(src, "zero.bin")).st_mtime - os.stat(os.path.join(dst, "Shoot", "zero.bin")).st_mtime) < 2
    assert not [f for _, _, fs in os.walk(dst) for f in fs if f.endswith(ezcopy.PART)]
    assert any("symlink" in n for n in rep.notes) == has_link

    # 2. re-run copies nothing (resume), still ok
    rep = ezcopy.run([src], dst)
    assert rep.ok and rep.copied == 0 and rep.skipped == 203, rep

    # 3. a changed source file is re-copied, never silently kept
    p = os.path.join(src, "a", "f5.bin")
    open(p, "wb").write(b"new content!")
    rep = ezcopy.run([src], dst)
    assert rep.copied == 1 and sha(os.path.join(dst, "Shoot", "a", "f5.bin")) == sha(p)

    # 4. corruption on disk after write -> detected, retried once, succeeds when it was transient
    real, calls = ezcopy.hash_file, {"n": 0}

    def flaky(path, *a, **k):
        calls["n"] += 1
        if calls["n"] == 1:                       # flip one byte on disk, once
            with open(path, "r+b") as f:
                f.seek(10)
                f.write(b"\xff")
        return real(path, *a, **k)
    ezcopy.hash_file = flaky
    d2 = os.path.join(tmp, "out2")
    rep = ezcopy.run([os.path.join(src, "a", "b", "big.bin")], d2)
    assert rep.ok and rep.copied == 1 and calls["n"] == 2, (rep, calls)
    assert sha(os.path.join(d2, "big.bin")) == sha(os.path.join(src, "a", "b", "big.bin"))

    # 5. persistent corruption / short write -> FAILED, nothing under the final name, no temp left
    def always_bad(path, *a, **k):
        with open(path, "r+b") as f:
            f.truncate(max(os.path.getsize(path) - 1, 0))   # simulated short write
        return real(path, *a, **k)
    ezcopy.hash_file = always_bad
    d3 = os.path.join(tmp, "out3")
    rep = ezcopy.run([os.path.join(src, "a", "f9.bin")], d3)
    assert not rep.ok and len(rep.failed) == 1 and rep.copied == 0, rep
    assert os.listdir(d3) == [], os.listdir(d3)
    ezcopy.hash_file = real

    # 6. guards: destination inside source, cancel
    try:
        ezcopy.run([src], os.path.join(src, "a"))
        raise AssertionError("should refuse")
    except ezcopy.EzCopyError:
        pass
    ev = threading.Event()
    ev.set()
    rep = ezcopy.run([src], os.path.join(tmp, "out4"), cancel=ev)
    assert rep.cancelled and rep.copied == 0

    # 7. streaming copy: chunk-edge sizes come out exact
    edge = os.path.join(tmp, "edge")
    os.makedirs(edge)
    B = ezcopy.BUF
    for n in (B - 1, B, B + 1, 2 * B, 3 * B - 1, 17 << 20, 18 << 20, 19 << 20):
        open(os.path.join(edge, f"e{n}.bin"), "wb").write(os.urandom(n))
    for mode in ("hdd", "ssd"):
        rep = ezcopy.run([edge], os.path.join(tmp, f"out-{mode}"), drives=mode)
        assert rep.ok and rep.copied == 8 and ("hard-disk" in rep.drives) == (mode == "hdd"), rep
        assert tree(os.path.join(tmp, f"out-{mode}", "edge")) == tree(edge)

    # 8. hard-disk mode: never two big files streaming at once, and it really overlaps
    real_stream, live = ezcopy._stream, {"now": 0, "max": 0}
    mx = threading.Lock()

    def counted(*a, **k):
        with mx:
            live["now"] += 1
            live["max"] = max(live["max"], live["now"])
        try:
            return real_stream(*a, **k)
        finally:
            with mx:
                live["now"] -= 1
    ezcopy._stream = counted
    rep = ezcopy.run([edge], os.path.join(tmp, "out-gate"), drives="hdd")
    assert rep.ok and live["max"] == 1, (rep, live)
    live["max"] = 0
    rep = ezcopy.run([edge], os.path.join(tmp, "out-par"), drives="ssd", workers=8)
    assert rep.ok and live["max"] >= 2, (rep, live)          # SSD mode: small files in parallel
    ezcopy._stream = real_stream

    # 9. a write error or a cancel in the middle of a stream: raised, and the reader thread ends
    class Boom:
        def __init__(self):
            self.n = 0

        def write(self, b):
            self.n += 1
            if self.n == 2:
                raise OSError("disk full")
    big = os.path.join(src, "a", "b", "big.bin")
    size = os.path.getsize(big)
    prog = ezcopy.Progress(1, 1)
    before = threading.active_count()
    try:
        ezcopy._stream(big, Boom(), hashlib.sha256(), threading.Event(), prog, size)
        raise AssertionError("should raise")
    except OSError as e:
        assert "disk full" in str(e)
    cancel_now = threading.Event()

    class CancelAfterFirst(ezcopy.Progress):
        def add(self, n, copied=False):
            cancel_now.set()
    with open(os.path.join(tmp, "x.bin"), "wb") as fo:
        try:
            ezcopy._stream(big, fo, hashlib.sha256(), cancel_now, CancelAfterFirst(1, 1), size)
            raise AssertionError("should cancel")
        except ezcopy.Cancelled:
            pass
    # source shrinks mid-copy: a read error, not a silently short copy
    shrink = os.path.join(tmp, "shrink.bin")
    open(shrink, "wb").write(os.urandom(10 * B))
    gen = ezcopy._chunks(shrink, 10 * B)
    first = next(gen)
    assert bytes(first[:16]) == open(shrink, "rb").read(16)
    os.truncate(shrink, B)
    try:
        for _ in gen:
            pass
        raise AssertionError("should notice the file got shorter")
    except OSError as e:
        assert "shorter" in str(e), e
    gen.close()
    assert threading.active_count() == before, "reader threads left running"

    # 9b. the parallel-read checksum equals a plain one, for awkward sizes
    for n in (B + 1, 5 * B - 3):
        p = os.path.join(tmp, f"h{n}.bin")
        open(p, "wb").write(os.urandom(n))
        assert ezcopy.hash_file(p) == sha(p)

    # 10. drive detection never crashes; says what it found
    kind = ezcopy.drive_kind(tmp)
    assert kind in ("hdd", "ssd", "network", None), kind
    print(f"drive kind of {tmp}: {kind}")

    print("ALL EZCOPY CHECKS PASSED")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
