# EzCopy

Fast, verified file copy for Mac and Windows. Copies many files in parallel, then **re-reads every
copied file and compares a SHA-256 checksum with the source** before it gets its final name.

- Window: open the app, add files/folders, choose destination, START COPY.
- Command line: `python3 ezcopy.py SRC [SRC ...] DEST` (exit code 1 if any file failed).
- Every run writes a log with the checksum of each file to `~/EzCopy Logs/`.
- Run it again on the same folders to resume: files already copied (same size + time) are skipped.
- Self-test (includes simulated corruption): `python3 test_ezcopy.py`

## Install
- **Mac (Apple Silicon):** unzip `EzCopy-mac-AppleSilicon.zip`, drag EzCopy.app to Applications.
  The app is not signed by Apple, so the first time: right-click the app -> Open -> Open
  (or System Settings -> Privacy & Security -> "Open Anyway").
- **Windows:** build `EzCopy.exe` with `build_windows.bat` on any Windows PC with Python, or push this folder
  to GitHub and download both builds from Actions (`.github/workflows/build.yml`). Windows SmartScreen will
  warn the first time: More info -> Run anyway.
- **Intel Macs:** run `build_mac.sh` on an Intel Mac (don't run it from an exFAT drive).

## What it can and cannot do
- It removes software overhead (many small files copy ~2.5x faster than `cp`/Finder in our test).
  It cannot beat your drive or network: one huge video file copies at about the same speed as `cp`.
- Verification re-reads the destination, so a verified copy takes longer than an unverified one. Keep it on.
- Symlinks are skipped (listed in the log). Permissions/Finder tags/extended attributes are not copied; timestamps are.
- Windows: the read-back may be served from the OS cache, so it proves the copy logic and file size, but cannot
  prove the bytes reached the platter. The Windows build is untested by the author.
