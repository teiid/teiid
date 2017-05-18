@ECHO OFF
@setlocal

@REM Copyright Red Hat, Inc. and/or its affiliates
@REM and other contributors as indicated by the @author tags and
@REM the COPYRIGHT.txt file distributed with this work.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.

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
