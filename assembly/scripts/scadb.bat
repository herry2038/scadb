
@echo on


@REM set %HOME% to equivalent of $HOME
if "%HOME%" == "" (set HOME=%HOMEDRIVE%%HOMEPATH%)

set ERROR_CODE=0

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM ==== START VALIDATION ====
if not "%JAVA_HOME%" == "" goto OkJHome

for /f %%j in ("java.exe") do (
  set JAVA_EXE=%%~$PATH:j
  goto init
)

:OkJHome
if exist "%JAVA_HOME%\bin\java.exe" (
 SET JAVA_EXE="%JAVA_HOME%\bin\java.exe"
 goto init
)

echo.
echo ERROR: JAVA_HOME is set to an invalid directory.
echo JAVA_HOME = %JAVA_HOME%
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation
echo.
goto error

:init
@REM Decide how to startup depending on the version of windows

set COMMAND=%1

set bcd=0
set CMD_LINE_ARGS=
for %%a in (%*) do (
	set /a "bcd+=1"
	echo !bcd!
	if !bcd! geq 2 (
		echo %%a
		set CMD_LINE_ARGS=!CMD_LINE_ARGS! %%a
	)
)

goto endInit


@REM Reaching here means variables are defined and arguments have been captured


:endInit

echo I am server 0
set CLASS=%COMMAND%
echo I am server 1
if "%COMMAND%" == "server" (
    echo I am server
    set CLASS="org.herry2038.scadb.scadb.server.ScadbServer"
) else if "%COMMAND%" == "admin" (
    set CLASS="org.herry2038.scadb.admin.server.AdminServer"
)

SET PROG_HOME=%~dp0..
SET PSEP=;

@REM Start Java program
:runm2
SET CMDLINE=%JAVA_EXE% %JVM_OPT% "-Xmx4096m" -cp "%PROG_HOME%\conf%PSEP%%PROG_HOME%\lib\*;" -Dprog.home="%PROG_HOME%" -Dprog.version="1.0" %CLASS% %CMD_LINE_ARGS%
%CMDLINE%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set JAVA_EXE=
set CMD_LINE_ARGS=
set CMDLINE=
set PSEP=
goto postExec

:endNT
@endlocal

:postExec
exit /B %ERROR_CODE%

