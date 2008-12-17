@setlocal

@rem
@rem ##COPYRIGHT##
@rem
@rem This script will start the hostcontroller and all defined VM's for that host.
@rem The default behavior is to start all components defined for the host
@rem in the configuration.
@rem 
@rem 
@rem -----------------------------------------------------------------------------------------
@rem	startserver {-config <hostidentifier>  | -help} {-noprocesses} 
@rem
@rem Options:
@rem 
@rem 		-config <hostidentifier>  The hostidentifier is the name given to this host configuration within the 
@rem					MetaMatrix Configuration file. If omitted, the fully-qualified 
@rem					host name is used, derived from InetAddress.  If the value passed 
@rem					can not be found in the configuration, an attempt is made to resolve
@rem					it to the physical host name or IP address. 
@rem
@rem 		-noprocesses  - If present, no processes will be started, only the hostcontroller.
@rem                               
@rem 		-help  - If present, will display the available command line arguments that can be used
@rem 
@rem Examples: startserver -config 192.168.1.1 
@rem 
@rem           The configuration must either contain a host segment with the value passed in (e.g., IP Address) 
@rem		   or must contain a name for which the value is resolved to (e.g, Host Name or IP Address). 
@rem 
@rem Examples: startserver -noprocesses
@rem 
@rem           This command will start only the hostcontroller.  To start the processes, either
@rem		   run startserver (without the -noprocesses argument) or svcmgr.sh (.cmd).
@rem 
@rem          
@rem            
@rem 
@rem -----------------------------------------------------------------------------------------

@title %0 %*


call "%~dp0mmenv.cmd"


@pushd %MM_LOG%

@echo off
@set HOSTARG=-startprocesses
@set PARMS=


:argCheck
if ""%1""=="""" goto gotAllArgs
if ""%1""==""-noprocesses"" @set HOSTARG=%1
if ""%1""==""-config"" goto gotParm
if ""%1""==""-host"" goto gotParm
shift
goto argCheck

:gotParm

@set PARMS=%PARMS% %1
shift
@set PARMS=%PARMS% %1
shift
goto argCheck

:gotAllArgs

%MM_JAVA% -server -Xmx128m -Dmetamatrix.log=0 com.metamatrix.server.HostController %HOSTARG%  %PARMS%
@endlocal
