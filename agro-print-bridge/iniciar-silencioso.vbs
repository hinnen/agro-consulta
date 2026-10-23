' Agro Etiqueta Print — sobe em segundo plano (sem janela preta)
Option Explicit
Dim sh, fso, root, electron, override, cmd
Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
root = fso.GetParentFolderName(WScript.ScriptFullName)
override = sh.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\AgroEtiquetaPrint\electron-dist"
electron = override & "\electron.exe"

If Not fso.FileExists(electron) Then
  ' Primeira vez / faltou o Electron: abre o instalador com janela
  sh.Run """" & root & "\Iniciar-ponte-etiquetas.bat""", 1, True
End If

If Not fso.FileExists(electron) Then
  WScript.Quit 1
End If

' Já rodando? (porta 19192 responde) — evita duas pontes
On Error Resume Next
Dim http
Set http = CreateObject("MSXML2.XMLHTTP")
http.Open "GET", "http://127.0.0.1:19192/health", False
http.Send
If Err.Number = 0 Then
  If http.Status = 200 Then WScript.Quit 0
End If
Err.Clear
On Error GoTo 0

sh.Environment("Process")("ELECTRON_OVERRIDE_DIST_PATH") = override
' 0 = oculto (só ícone na bandeja)
sh.Run """" & electron & """ """ & root & """", 0, False
