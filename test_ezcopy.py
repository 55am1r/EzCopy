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

    print("ALL EZCOPY CHECKS PASSED")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
