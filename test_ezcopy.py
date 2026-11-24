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

    # 3. a destination file that differs is NEVER silently replaced: reported as a conflict, left untouched;
    #    only replace=True overwrites it
    p = os.path.join(src, "a", "f5.bin")
    q = os.path.join(dst, "Shoot", "a", "f5.bin")
    before = sha(q)
    open(p, "wb").write(b"new content!")
    rep = ezcopy.run([src], dst)
    assert not rep.ok and len(rep.failed) == 1 and "CONFLICT" in rep.failed[0][1] and rep.copied == 0, rep
    assert sha(q) == before
    rep = ezcopy.run([src], dst, replace=True)
    assert rep.ok and rep.copied == 1 and sha(q) == sha(p)

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

    # 11. speed test: measures every part, leaves no test files, copies nothing, cancels cleanly
    card, out = os.path.join(tmp, "card"), os.path.join(tmp, "st-out")
    os.makedirs(os.path.join(card, "DCIM"))
    os.makedirs(out)
    for i, n in enumerate((3 * B + 5, B, 4096, 1)):
        open(os.path.join(card, "DCIM", f"IMG_{i}.CR3"), "wb").write(os.urandom(n))
    open(os.path.join(card, "empty.txt"), "wb").close()
    for ver in (True, False):
        r = ezcopy.speed_test([card], out, verify=ver, seconds=0.5, max_bytes=64 << 20)
        assert r.read > 0 and r.write > 0 and r.cpu > 0 and r.speed > 0, r
        assert (r.readback > 0) == ver, r
        assert r.files == 4 and r.total == 4 * B + 4102, r
        assert r.limit and r.speed <= max(r.read, r.write, 2 * r.cpu), r
        assert os.listdir(out) == [], os.listdir(out)          # test files deleted, nothing copied
        text = "\n".join(ezcopy.speed_lines(r))
        assert "should copy" in text and ("Reading back" in text) == ver, text
    print("\n".join(ezcopy.speed_lines(r)))
    if os.name == "nt":   # the source read must really go around the memory cache on Windows
        n, _ = ezcopy._read_test([(os.path.join(card, "DCIM", "IMG_0.CR3"), 3 * B + 5)], 5, 1 << 30,
                                 threading.Event(), direct=True)
        assert n == 3 * B + 5, n
    gone = threading.Event()
    gone.set()
    try:
        ezcopy.speed_test([card], out, cancel=gone, seconds=0.5, max_bytes=64 << 20)
        raise AssertionError("should cancel")
    except ezcopy.Cancelled:
        pass
    assert os.listdir(out) == [], os.listdir(out)
    try:
        ezcopy.speed_test([os.path.join(card, "empty.txt")], out)
        raise AssertionError("should say there is nothing to test")
    except ezcopy.EzCopyError as e:
        assert "Nothing to test" in str(e), e
    # the limit and tip follow the slowest part
    r = ezcopy.SpeedResult(read=400e6, write=150e6, readback=3e9, cpu=1.5e9, verify=True)
    ezcopy._estimate(r, True, "hdd")
    assert r.limit == "writing to the destination" and "hard disk" in r.tip, r
    r = ezcopy.SpeedResult(read=37e6, write=150e6, readback=3e9, cpu=1.5e9, verify=False)
    ezcopy._estimate(r, True, "hdd")
    assert r.speed == 37e6 and "USB 2.0" in r.tip, r
    assert threading.active_count() == before, "speed test left threads running"

    # 12. two cards holding the same file name copied into one folder: BOTH survive (the 2nd gets "(2)"),
    #     also for names that differ only in case
    c1, c2, o12 = (os.path.join(tmp, n) for n in ("card1", "card2", "out12"))
    for c, body in ((c1, b"card one footage"), (c2, b"card two footage")):
        os.makedirs(os.path.join(c, "DCIM"))
        open(os.path.join(c, "DCIM", "IMG_0001.JPG"), "wb").write(body)
        open(os.path.join(c, "DCIM", "clip.MOV" if c == c1 else "CLIP.mov"), "wb").write(body + b"!")
    rep = ezcopy.run([os.path.join(c1, "DCIM"), os.path.join(c2, "DCIM")], o12)
    assert rep.ok and rep.copied == 4, rep
    got = sorted(open(os.path.join(r, f), "rb").read() for r, _, fs in os.walk(o12) for f in fs)
    assert got == sorted([b"card one footage", b"card two footage", b"card one footage!", b"card two footage!"]), got
    assert len([n for n in rep.notes if "same name" in n]) == 2, rep.notes

    # 13. the cheap resume check can be fooled (same size + same time, different bytes); compare=True is not
    o13 = os.path.join(tmp, "out13")
    f13 = os.path.join(tmp, "f13.bin")
    open(f13, "wb").write(os.urandom(50000))
    assert ezcopy.run([f13], o13).ok
    d13 = os.path.join(o13, "f13.bin")
    st = os.stat(d13)
    with open(d13, "r+b") as f:
        f.seek(100)
        f.write(b"\x00\x01\x02")
    os.utime(d13, ns=(st.st_atime_ns, st.st_mtime_ns))             # size and time look untouched
    rep = ezcopy.run([f13], o13)
    assert rep.ok and rep.skipped == 1 and sha(d13) != sha(f13)    # documented limit of the fast check
    rep = ezcopy.run([f13], o13, compare=True)
    assert not rep.ok and "CONFLICT" in rep.failed[0][1] and sha(d13) != sha(f13), rep
    rep = ezcopy.run([f13], o13, compare=True, replace=True)
    assert rep.ok and rep.copied == 1 and sha(d13) == sha(f13), rep
    rep = ezcopy.run([f13], o13, compare=True)
    assert rep.ok and rep.skipped == 1 and rep.copied == 0, rep

    # 14. check mode: copies nothing, finds a changed byte and a missing file
    rep = ezcopy.check([src], dst)
    assert rep.kind == "check" and rep.ok and rep.copied == 203, rep
    cs = os.path.join(dst, "Shoot", "a", "b", "big.bin")
    st = os.stat(cs)
    with open(cs, "r+b") as f:
        f.seek(15 << 20)
        f.write(b"\xaa")
    os.utime(cs, ns=(st.st_atime_ns, st.st_mtime_ns))
    os.remove(os.path.join(dst, "Shoot", "a", "f7.bin"))
    rep = ezcopy.check([src], dst)
    why = {os.path.basename(s): w for s, w in rep.failed}
    assert not rep.ok and "DIFFERENT" in why["big.bin"] and "MISSING" in why["f7.bin"] and len(why) == 2, rep
    assert not os.path.exists(os.path.join(dst, "Shoot", "a", "f7.bin"))        # check never copies
    assert "PROBLEMS" in ezcopy.summary(rep)

    # 15. no temp files are left behind by any of the runs above
    assert not [f for r, _, fs in os.walk(tmp) for f in fs if f.endswith(ezcopy.PART)]

    print("ALL EZCOPY CHECKS PASSED")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
