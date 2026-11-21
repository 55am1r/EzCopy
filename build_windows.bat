@echo off
rem Builds EzCopy.exe (Windows). Needs Python 3 from python.org (tick "Add to PATH"; tkinter is included).
rem NOT TESTED by the author - first run on a real Windows PC and report any error.
cd /d "%~dp0"
python test_ezcopy.py || goto :err
python -m venv "%TEMP%\ezcopy-venv" || goto :err
"%TEMP%\ezcopy-venv\Scripts\pip" install --quiet pyinstaller || goto :err
"%TEMP%\ezcopy-venv\Scripts\pyinstaller" --noconfirm --onefile --windowed --name EzCopy --distpath dist --workpath "%TEMP%\ezcopy-build" --specpath "%TEMP%\ezcopy-build" ezcopy.py || goto :err
echo Done: dist\EzCopy.exe
exit /b 0
:err
echo BUILD FAILED
exit /b 1
