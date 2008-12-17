@rem
@rem ##COPYRIGHT##
@rem

@rem -----------------------------------------------------------------------------------------
@rem setupmm is run to complete the installation of a server.  
@rem This will setup the repository and install the first host
@rem for this system.
@rem After this is run, additional host(s) can added to this system
@rem by running systemhostsetup.cmd
@rem -----------------------------------------------------------------------------------------


@setlocal
@rem @title %0

@echo off
call "%~dp0mmenv.cmd"

@rem if running silent install then run
if not defined %1 goto :cont

if exist "%1" goto :runsetup
@echo -----------------------------------------------
@echo File %1 does not exist, please verify and retry
@echo -----------------------------------------------
goto :end

:cont
@rem if the system already exist, prompt to determine if the
@rem user wants it replaced.

if not exist "%MM_ROOT%\lib\metamatrix.properties" goto :runsetup

@echo on
@echo ---------------------------------------------------------------
@echo The System has already been setup, running option 11 will remove
@echo all hosts and all data in the repository will be deleted.
@set /P INPUT="Do you wish to continue (Enter y to continue):"
@echo off
@IF /I %INPUT% EQU Y goto :runsetup
@IF /I %INPUT% EQU y goto :runsetup
goto :end

:runsetup

@pushd %MM_ROOT%


@rem ComSpec is a Windows system environment property that defines the full path to the cmd.exe
@rem this property is used for setting up the NT Service prop files in the util directory

@set HOSTNAME=localhost

@set ANT_ARGS= -Dmetamatrix.message.bus.type=noop.message.bus
@set ANT_ARGS=%ANT_ARGS% -DComSpec=%ComSpec%
@set ANT_ARGS=%ANT_ARGS% -Dmetamatrix.installationDir=%MM_ROOT%
@set ANT_ARGS=%ANT_ARGS% -Dhost.name.resolved=%HOSTNAME%
@set ANT_ARGS=%ANT_ARGS% -propertyfile %MM_ROOT%\config\install\setup_default.properties

if defined %JAVA_OPTS% set ANT_ARGS=%JAVA_OPT% %ANT_ARGS%


@set ANT_OPTS="-Xmx512m"
@set ANT_HOME=%MM_ROOT%\lib

@rem  uncomment for additional debugging info in the logfile
@rem @set ANT_OPTS=%ANT_OPTS% -verbose


%MM_JAVA% -cp "%CLASSPATH%" -Dant.home="%ANT_HOME%" %ANT_OPTS% org.apache.tools.ant.Main %ANT_ARGS% -buildfile %MM_ROOT%\config\setup_server.xml install.server

@echo off
@rem Removing unnecessary shell scripts
@for /R %MM_ROOT% %%i in (*.sh) do del "%%i"

:end