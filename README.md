# EzCopy

Fast, verified file copy for Mac and Windows. Copies many files in parallel, then **re-reads every
copied file and compares a SHA-256 checksum with the source** before it gets its final name.

- Window: open the app, add files/folders, choose destination, START COPY.
- Command line: `python3 ezcopy.py SRC [SRC ...] DEST` (exit code 1 if any file failed).
  Options: `--drives auto|hdd|ssd`, `--workers N` (parallel small files), `--no-verify`,
  `--speedtest` (measure only, copy nothing), `--check` (compare only), `--replace`, `--compare`.
- Every run writes a log with the checksum of each file to `~/EzCopy Logs/`.
- Run it again on the same folders to resume: files already copied (same size + time) are skipped.
- Self-test (includes simulated corruption): `python3 test_ezcopy.py`

## Nothing is ever overwritten or lost silently
- **Same name from two places** (two cards both holding `IMG_0001.JPG`, or `a.txt` and `A.TXT`) copied into
  one folder: both are kept, the later one is saved as `IMG_0001 (2).JPG`, and the log says so.
- **A different file is already at the destination:** it is left untouched and reported as a CONFLICT (the
  run ends with errors). Tick **Replace files already there that differ** (`--replace`) to overwrite them.
- **Resume** trusts "same size and same time" (fast). That can be fooled by a file changed without changing
  its size or time, so tick **Compare contents of files already there** (`--compare`) to hash both copies
  (slower: it reads every such file twice).
- **Check copy** (`--check`) copies nothing: it compares the destination with the source, byte for byte
  (SHA-256 of both), and lists anything missing or different. Run it after unplugging and replugging the
  destination, and it reads the real disk, not memory: the strongest proof a copy is intact.
- Leftover `*.ezcopy-part` files appear only if a copy was interrupted (crash, power loss). They are safe to delete.

## Install
- **Mac (Apple Silicon):** unzip `EzCopy-mac-AppleSilicon.zip`, drag EzCopy.app to Applications.
  The app is not signed by Apple, so the first time: right-click the app -> Open -> Open
  (or System Settings -> Privacy & Security -> "Open Anyway").
- **Windows:** build `EzCopy.exe` with `build_windows.bat` on any Windows PC with Python, or push this folder
  to GitHub and download both builds from Actions (`.github/workflows/build.yml`). Windows SmartScreen will
  warn the first time: More info -> Run anyway.
- **Intel Macs:** run `build_mac.sh` on an Intel Mac (don't run it from an exFAT drive).

## Speed test: what limits my copy?
Add what you want to copy and choose the destination, then press **Speed test** (about 20 seconds; nothing is
copied). It measures, the way a copy uses them:
- **reading the source**: the card, card reader or drive. On Windows this goes around the memory cache, so
  files you opened a minute ago still give the drive's real speed;
- **writing the destination**: test files are written and flushed to the disk, then deleted;
- **reading back for the check**, and the **processor's checksum speed**.

It then says how fast EzCopy can copy big files there, which part is the slowest, how long your selection
would take, and what would help, for example a USB 2.0 port or hub, or a single hard disk.
Command line: `python3 ezcopy.py --speedtest SRC DEST`.

## Speed: how fast can it go?
A copy can never be faster than the **slower drive, or the slower cable/port**, whichever is lower:

| Link / drive                                   | real speed    | 200 GB takes at least |
|------------------------------------------------|---------------|-----------------------|
| USB 2.0 port or USB 2.0 hub (any drive)        | ~35-40 MB/s   | ~1 h 30 m             |
| USB 3 / USB-C hard disk, or internal SATA HDD  | ~120-220 MB/s | ~15-30 min            |
| SMR hard disk during long writes               | can drop to 30-60 MB/s | 1 h+         |
| SATA SSD / NVMe                                | 500-3000 MB/s | a few minutes         |

**Check the port first.** A USB 3 drive plugged into a USB 2.0 port, an old hub or a front-panel port runs
at USB 2.0 speed. On Windows use the blue / "SS" ports at the back of the PC (or USB-C), no hub.

What EzCopy does to reach that limit:
- **Drives: Auto-detect** finds out whether each drive is a hard disk, SSD or network drive.
  When a hard disk is involved (or Windows can't tell, e.g. some USB cases), it uses **hard-disk mode**:
  files of 16 MB and up are copied one at a time, so the disk head moves straight through each file instead
  of jumping between files. The flush and read-back of one file overlap the copying of the next.
- It keeps 4 reads in flight per file. One read at a time leaves a USB or network drive idle between
  requests (measured on a USB hard disk: 19 MB/s with one read at a time, 37 MB/s with 4).
- Small files are copied in parallel (Parallel small files, default 8): there the per-file overhead is what costs.
- The speed and time left shown are measured over the last 30 seconds. The first seconds of any copy look
  too fast because the OS cache soaks them up, so don't trust an early estimate (Windows Explorer's either).
- If a USB hard disk is not detected (the window says "parallel mode"), choose **Drives: Hard disks** yourself.

## What it can and cannot do
- It removes software overhead (many small files copy ~2.5x faster than `cp`/Finder in our test).
  It cannot beat your drive, port or network (see the table above).
- Verification reads every copied file back and compares checksums. Keep it on.
- Mac and Windows often serve that read-back from memory, because a file just written is still cached there.
  Then the check costs little time, and it proves the copy step and the file size, but not that the disk
  stored the bytes correctly. A file too big to stay in memory (a long video) is read back from the disk
  itself; that file then takes up to twice as long.
- Symlinks are skipped (listed in the log). Permissions/Finder tags/extended attributes are not copied; timestamps are.
- It does not know card types (SD, CFexpress, ...); it measures what a drive actually delivers (Speed test)
  and treats hard disks differently from everything else, which is the only distinction that changes how to copy.
- Linux is not supported (no Linux drive detection or build).
- On a fast SSD (4 GB file, internal drive) EzCopy matched the system `cp` (about 250-390 MB/s each,
  verify on or off); run-to-run swings of that SSD were larger than the difference between the tools.
  So the copy engine is not what limits speed on that hardware; the drive is.
- The Windows build passes the self-test on Windows (GitHub Actions), but the author has not run the app by hand there.
