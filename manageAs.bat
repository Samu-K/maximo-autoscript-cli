@echo off

set ogDir="%cd%"
cd /d %~dp0
node src\funcs\manageAs.js --runDir=%ogDir% %*
cd /d %ogDir%
exit /b %ERRORLEVEL%