@rem
@rem ##COPYRIGHT##

@rem *************************************************************************
@rem This script is used to set the environment for the MetaMatrix scripts
@rem
@rem    MM_DEBUG - If this is defined (with any value), the script
@rem                       will emit debug messages.
@rem set MM_DEBUG=true

@rem    MM_ROOT - The location of the MetaMatrix installation.
@rem set MM_ROOT=

@rem    MM_JAVA_HOME - The location of the JRE that the server will
@rem                          use. This will be ignored if
@rem                          MM_JAVA_EXE_FILE_PATH is set.
@rem                          If this and MM_JAVA_EXE_FILE_PATH are
@rem                          not set, then JAVA_HOME JRE will be used.
@rem
@rem set MM_JAVA_HOME=


@rem    MM_JAVA_EXE_FILE_PATH - Defines the full path to the Java
@rem                                   executable to use. If this is set,
@rem                                   MM_JAVA_HOME is ignored.
@rem                                   If this is not set, then
@rem                                   $MM_JAVA_HOME/bin/java
@rem                                   is used. If this and
@rem                                   MM_JAVA_HOME are not set, then
@rem                                   JAVA_HOME JRE will be used.
@rem
@rem set MM_JAVA_EXE_FILE_PATH=

@rem    MM_JAVA_OPTS - Java VM command line options to be
@rem                          passed into the executing VM. If this is not defined
@rem                          this script will pass in a default set of options.
@rem                          If this is set, it completely overrides the
@rem                          defaults. If you only want to add options
@rem                          to the agent's defaults, then you will want to
@rem                          use MM_ADDITIONAL_JAVA_OPTS instead.
@rem
@rem set MM_JAVA_OPTS="-Xms160M -Xmx160M -Djava.net.preferIPv4Stack=true"

@rem    MM_ADDITIONAL_JAVA_OPTS - additional Java VM command line options
@rem                                     to be passed into the executing VM. This
@rem                                     is added to MM_JAVA_OPTS; it
@rem                                     is mainly used to augment the
@rem                                     default set of options. This can be
@rem                                     left unset if it is not needed.
@rem
@rem set MM_ADDITIONAL_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,address=9797,server=y,suspend=n"

@clear

@ECHO OFF

if not "%MM_ROOT%"=="" goto cont
@REM ********************************************************************************************


@REM Root of MetaMatrix installation
@REM 	save the current directory
@pushd
@REM 	go up a directory level
@cd..
@REM 	get the ROOT directory of this installation
@for /F "delims=" %%i IN ('cd') DO set MM_ROOT=%%i
@REM 	return to the bin directory
@popd

:cont



rem ----------------------------------------------------------------------
rem Find the Java executable and verify we have a VM available
rem ----------------------------------------------------------------------

if not defined MM_JAVA_EXE_FILE_PATH (
   if not defined MM_JAVA_HOME goto :checkjavahome
   if defined MM_DEBUG echo Using MM_JAVA_HOME %MM_JAVA_HOME%
   set MM_JAVA_EXE_FILE_PATH=%MM_JAVA_HOME%\bin\java.exe
)
goto :javaset

:checkjavahome
if defined MM_DEBUG echo Try JAVA_HOME %JAVA_HOME%
set MM_JAVA_EXE_FILE_PATH=%JAVA_HOME%\bin\java.exe


:javaset

if defined MM_DEBUG echo MM_JAVA_EXE_FILE_PATH: %MM_JAVA_EXE_FILE_PATH%

if not exist "%MM_JAVA_EXE_FILE_PATH%" (
   echo There is no JVM defined, will try to use java.
   echo "To use a specific java, set MM_JAVA_HOME, MM_JAVA_EXE_FILE_PATH or JAVA_HOME appropriately."
   set MM_JAVA_EXE_FILE_PATH=java.exe 
)

@set MM_JAVA=%MM_JAVA_EXE_FILE_PATH%

rem -----------------
rem Build the classpath from the lib directory
rem ------------------
for /R "%MM_ROOT%\lib" %%G in ("*.jar") do (
   call :append_classpath "%%G"
   if defined MM_DEBUG echo CLASSPATH entry: %%G
)


set CLASSPATH=%MM_ROOT%/config;%CLASSPATH%

@REM ********************************************************************************************
@REM setup Environment
@REM Standard system environment changes

@set JAVA_OPTS= -Djava.io.tmpdir=%MM_ROOT%\data\temp

if defined %MM_JAVA_OPTS set JAVA_OPTS=%JAVA_OPTS% %MM_JAVA_OPTS%

if defined %MM_ADDITIONAL_JAVA_OPTS% set JAVA_OPTS=%JAVA_OPTS% %MM_ADDITIONAL_JAVA_OPTS%


@rem @set PATH=%MM_ROOT%\bin;%MM_JAVA%\bin;%MM_JAVA%\jre\bin\server;%MM_ROOT%\odbc

@REM ********************************************************************************************
@REM Print the env settings
if defined MM_DEBUG echo ========================== GLOBAL ENV SETTINGS ==========================
if defined MM_DEBUG echo MM_ROOT     = %MM_ROOT%
if defined MM_DEBUG echo MM_JAVA     = %MM_JAVA%
if defined MM_DEBUG echo JAVA_OPTS   = %JAVA_OPTS%
if defined MM_DEBUG echo CLASSPATH	 = %CLASSPATH%
if defined MM_DEBUG echo ==================================================================
goto :end

rem ----------------------------------------------------------------------
rem CALL subroutine that appends the first argument to CLASSPATH
rem ----------------------------------------------------------------------

:append_classpath
set _entry=%1
if not defined CLASSPATH (
   set CLASSPATH=%_entry:"=%
) else (
   set CLASSPATH=%CLASSPATH%;%_entry:"=%
)

goto :eof



:end
