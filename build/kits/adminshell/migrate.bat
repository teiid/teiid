@ECHO OFF
@setlocal

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

@if not "%ECHO%" == ""  echo %ECHO%
@if "%OS%" == "Windows_NT" setlocal

if "%OS%" == "Windows_NT" (
  set "DIRNAME=%~dp0%"
) else (
  set DIRNAME=.\
)

pushd %DIRNAME%
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

set TEIID_CLASSPATH=%TEIID_HOME%\lib\patches\*;%TEIID_HOME%\lib\*;

rem JVM memory allocation pool parameters. Modify as appropriate.
set JAVA_OPTS=%JAVA_OPTS% -Xms128m -Xmx256m -XX:MaxPermSize=256m
set JAVA_OPTS=%JAVA_OPTS% -Djava.util.logging.config.file=log.properties

"%JAVA%" %JAVA_OPTS% ^
   -classpath "%TEIID_CLASSPATH%" ^
   org.teiid.adminshell.MigrationUtil %*
