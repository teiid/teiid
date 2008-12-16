@setlocal

@rem
@rem ##COPYRIGHT##
@rem
@rem This will first stop the processes for this host, and then
@rem stop this hostcontroller.
@rem
@rem
@rem If you ran startserver with the -config or -host parameter, you will need
@rem to pass the same host name as an argument when running this script
@rem
@rem If you only wish to stop the processes on this host and not the hostcontroller,
@rem run stopserver -processes
@rem

@rem -----------------------------------------------------------------------------------------
@rem  stopserver {-config <hostidentifier> } {-processes}
@rem
@rem Options:
@rem 
@rem 		-config <hostidentifier>  The hostidentifier is the name given to this host configuration within the 
@rem					MetaMatrix Configuration file. If omitted, the fully-qualified 
@rem					host name is used, derived from InetAddress.  If the value passed 
@rem					can not be found in the configuration, an attempt is made to resolve
@rem					it to the physical host name or IP address. 
@rem 
@rem		-processes	- If present, will cause only the processes started by this host to be stopped.
@rem
@rem 
@rem    Examples: stopserver -config 192.254.254.5
@rem 
@rem           This command will pass the IP address as the host name for 
@rem			which must be matched to a host in the configuration.
@rem            
@rem 
@rem -----------------------------------------------------------------------------------------

@title %0
@echo off
call "%~dp0mmenv.cmd"

@rem the call to kill all processes and the hostcontroller
@set KILLHOST="killHostController"
@rem the call to kill only the processes for the host
@set KILLPROCESSES="killHost"

@set KILLCALL=%KILLHOST%
@set KILLMSG=Stopping hostcontroller


@set HOSTNAME=localhost
@set PARMS=

:argCheck
if ""%1""=="""" goto gotAllArgs
if ""%1""==""-config"" goto gotConfigParm
if ""%1""==""-processes"" goto gotProcessParm
if not "%1"=="" goto gotHostParm

goto argCheck


:gotConfigParm
@set PARMS=%PARMS% %1
shift
@set PARMS=%PARMS% %1
@set HOSTNAME=%1
shift
goto argCheck

:gotProcessParm
@set KILLCALL=%KILLPROCESSES%
@set KILLMSG=Stopping processes
shift
goto argCheck

:gotHostParm
@set HOSTNAME=%1
@set PARMS=%PARMS% %HOSTNAME%
shift
goto argCheck

:gotAllArgs


@echo %KILLMSG% %HOSTNAME%

%MM_JAVA% -Xmx128m com.metamatrix.platform.util.MetaMatrixController %KILLCALL% %PARMS%

@endlocal
