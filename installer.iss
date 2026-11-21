; Inno Setup script -> EzCopy-Setup.exe (a normal Windows installer: Start Menu + Desktop shortcut, Uninstall entry).
; Installs for the current user, so no administrator password is needed.
#ifndef Version
  #define Version "1.0.0"
#endif
[Setup]
AppName=EzCopy
AppVersion={#Version}
AppPublisher=Chaitus Media
DefaultDirName={autopf}\EzCopy
DefaultGroupName=EzCopy
PrivilegesRequired=lowest
OutputDir=installer
OutputBaseFilename=EzCopy-Setup
Compression=lzma2
SolidCompression=yes
UninstallDisplayIcon={app}\EzCopy.exe
DisableProgramGroupPage=yes
[Files]
Source: "dist\EzCopy.exe"; DestDir: "{app}"; Flags: ignoreversion
[Icons]
Name: "{group}\EzCopy"; Filename: "{app}\EzCopy.exe"
Name: "{autodesktop}\EzCopy"; Filename: "{app}\EzCopy.exe"
[Run]
Filename: "{app}\EzCopy.exe"; Description: "Open EzCopy"; Flags: nowait postinstall skipifsilent
