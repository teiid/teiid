@ECHO OFF
@setlocal

@REM Teiid Connector Development Kit (CDK)
@REM JBoss, Home of Professional Open Source.
@REM Copyright (C) 2008 Red Hat, Inc.
@REM Licensed to Red Hat, Inc. under one or more contributor 
@REM license agreements.  See the copyright.txt file in the
@REM distribution for a full listing of individual contributors.
@REM 
@REM This library is free software; you can redistribute it and/or
@REM modify it under the terms of the GNU Lesser General Public
@REM License as published by the Free Software Foundation; either
@REM version 2.1 of the License, or (at your option) any later version.
@REM 
@REM This library is distributed in the hope that it will be useful,
@REM but WITHOUT ANY WARRANTY; without even the implied warranty of
@REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
@REM Lesser General Public License for more details.
@REM 
@REM You should have received a copy of the GNU Lesser General Public
@REM License along with this library; if not, write to the Free Software
@REM Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
@REM 02110-1301 USA.

@REM This assumes it's run from its installation directory. It is also assumed there is a java
@REM executable defined along the PATH

@pushd
@REM 	get the ROOT directory of this installation
@for /F "delims=" %%i IN ('cd') DO set TEIID_ROOT=%%i
@REM 	return to the bin directory
@popd


@echo off
@REM @SET JAVA_HOME=c:\jdk1.6.0_11


if "x%JAVA_HOME%" == "x" goto noJavaHome
if not exist %JAVA_HOME%\bin\java.exe goto noJavaExecutable
goto runProgram

:noJavaHome
@echo Please set the JAVA_HOME environment variable for this script.  
pause
goto endOfScript

:noJavaExecutable
@echo Unable to find the %JAVA_HOME%\bin\java.exe executable. Please ensure the JAVA_HOME environment variable is set to the root directory of your java installation.
pause
goto endOfScript

:runProgram
@REM Set up directories
@set TEIID_LIB=%TEIID_ROOT%\lib

@REM Set up classpath variables
@set PATCH_JAR=%TEIID_LIB%\patch\*
@set CDK_CLASSPATH=%TEIID_LIB%\*
@set CP=%PATCH_JAR%;%CDK_CLASSPATH%;.;.\*

@REM Print the env settings
@echo ========================== ENV SETTINGS ==========================
@echo TEIID_ROOT  = %TEIID_ROOT%
@echo JAVA_HOME      = %JAVA_HOME%
@echo CP      	     = %CP%
@echo ==================================================================

@REM Execute the cdk tool

@echo on
%JAVA_HOME%/bin/java -Xmx256m -cp %CP% -Dmetamatrix.config.none -Dmetamatrix.log=4 com.metamatrix.cdk.ConnectorShell %*

:endOfScript

@endlocal
