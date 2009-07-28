@echo off
rem -------------------------------------------------------------------------
rem This scripts sends a shuts down signal to the running Teiid Server on 
rem the local machine.
rem -------------------------------------------------------------------------


@if not "%ECHO%" == ""  echo %ECHO%
@if "%OS%" == "Windows_NT" setlocal

if "%OS%" == "Windows_NT" (
  set "DIRNAME=%~dp0%"
) else (
  set DIRNAME=.\
)

pushd %DIRNAME%..
if "x%TEIID_HOME%" == "x" (
  set "TEIID_HOME=%CD%"
)
popd

set DIRNAME=

if "x%JAVA_HOME%" == "x" (
  set  JAVA=java
  echo JAVA_HOME is not set. Unexpected results may occur.
  echo Set JAVA_HOME to the directory of your local JDK to avoid this message.
) else (
  set "JAVA=%JAVA_HOME%\bin\java"
  if exist "%JAVA_HOME%\lib\tools.jar" (
    set "JAVAC_JAR=%JAVA_HOME%\lib\tools.jar"
  )
)

set TEIID_CLASSPATH=%TEIID_HOME%\lib\patches\*;%TEIID_HOME%\deploy;%TEIID_HOME%\client\*;%TEIID_HOME%\lib\*;

rem JVM memory allocation pool parameters. Modify as appropriate.
set JAVA_OPTS=%JAVA_OPTS% -Xms128m -Xmx512m -XX:MaxPermSize=256m

rem Add JMX support
set JMX_PORT=9999

rem With Sun JVMs reduce the RMI GCs to once per hour
set JAVA_OPTS=%JAVA_OPTS% -Dsun.rmi.dgc.client.gcInterval=3600000 -Dsun.rmi.dgc.server.gcInterval=3600000

rem JPDA options. Uncomment and modify as appropriate to enable remote debugging.
rem set JAVA_OPTS=%JAVA_OPTS% -Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=y

cd %TEIID_HOME%
set JAVA_OPTS=%JAVA_OPTS% -Dteiid.home=%TEIID_HOME%

echo ===============================================================================
echo.
echo   Teiid Bootstrap Environment
echo.
echo   TEIID_HOME: %TEIID_HOME%
echo.
echo   JAVA: %JAVA%
echo.
echo   JAVA_OPTS: %JAVA_OPTS%
echo.
echo   CLASSPATH: %TEIID_CLASSPATH%
echo.
echo ===============================================================================
echo.

"%JAVA%" %JAVA_OPTS% ^
   -classpath "%TEIID_CLASSPATH%" ^
   -server ^
   org.teiid.Shutdown %TEIID_HOME%\deploy.properties %JMX_PORT% %*

