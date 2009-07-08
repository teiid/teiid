@echo off
rem 
rem This script file encrypts the passed in clear text and spits out encypted form
rem 

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
set KEYSTORE_FILE=%TEIID_HOME%\deploy\teiid.keystore
if not exist  %KEYSTORE_FILE (	
	%JAVA%" -classpath "%TEIID_CLASSPATH%" com.metamatrix.common.util.crypto.CryptoUtil -genkey %KEYSTORE_FILE%
	echo A new key with keystore generated at %KEYSTORE_FILE%    
)

if "x%1%" == "x"(
	echo Provide text phrase to encrypt.
) else (
	%JAVA%" -classpath "%TEIID_CLASSPATH%" com.metamatrix.common.util.crypto.CryptoUtil -key %KEYSTORE_FILE% -encrypt %1%
)    
