#!/bin/bash
# Builds EzCopy.app (Mac). Needs Python 3 WITH tkinter (python.org installer, or: brew install python-tk@3.14).
# Do NOT run this on an exFAT/FAT drive: copy the folder to your Desktop first.
set -e
cd "$(dirname "$0")"
PY="${PYTHON:-python3}"
"$PY" -c "import tkinter" || { echo "This Python has no tkinter. Install Python from python.org, then retry."; exit 1; }
"$PY" test_ezcopy.py
"$PY" -m venv ${TMPDIR:-/tmp}/ezcopy-venv
${TMPDIR:-/tmp}/ezcopy-venv/bin/pip install --quiet pyinstaller
${TMPDIR:-/tmp}/ezcopy-venv/bin/pyinstaller --noconfirm --windowed --name EzCopy \
  --distpath dist --workpath ${TMPDIR:-/tmp}/ezcopy-build --specpath ${TMPDIR:-/tmp}/ezcopy-build ezcopy.py
echo "Done: dist/EzCopy.app  (zip it to share:  cd dist && zip -r EzCopy-mac.zip EzCopy.app)"
