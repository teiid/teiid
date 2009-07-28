@echo off
rem ================================================================================== ###
rem This script helps in creating a security key for encrypting                        ### 
rem passwords and also helps in converting a clear text password into a                ###
rem encrypted one. For more information on how to use encrypted passwords please       ###
rem check out https://www.jboss.org/community/wiki/EncryptingpasswordsinTeiid          ###
rem                                                                                    ###
rem usage: keystore.bat [options]                                                      ###
rem	options:                                                                           ###
rem -c, --create Creates security key "teiid.keystore" file in the deploy directory    ###
rem -e, --encrypt <password> Encrypts the clear text password into encrypted form      ###
rem ================================================================================== ###

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

if "%1%" == "--create" (
	goto create
)
if "%1%" == "-c" (
	goto create
)
if "%1%" == "--encrypt" (
	goto encrypt
)
if "%1%" == "-e" (
	goto encrypt
)
goto prompt

:create
	if not exist  %KEYSTORE_FILE% (	
		"%JAVA%" -classpath "%TEIID_CLASSPATH%" com.metamatrix.common.util.crypto.CryptoUtil -genkey %KEYSTORE_FILE%
		echo A new key with keystore generated at %KEYSTORE_FILE%    
	) else (
		echo %KEYSTORE_FILE% already exists. Delete the current one if you would like to create a new keystore.
	)	
goto end

:encrypt
	if NOT "x%2%" == "x" (
		if exist  %KEYSTORE_FILE% (
			"%JAVA%" -classpath "%TEIID_CLASSPATH%" com.metamatrix.common.util.crypto.CryptoUtil -key %KEYSTORE_FILE% -encrypt %2%
		) else (
            echo %KEYSTORE_FILE% not found. Create a security key by using
            echo usage:%0% --create
		)
	) else (
		goto prompt
	)
goto end

:prompt
    echo This scripts helps to create a security key, and helps to encrypt a clear text password
    echo
	echo usage: %0% [options]
	echo options:
	echo -c, --create Creates security key "teiid.keystore" file in the "deploy" directory
	echo -e, --encrypt [password] Encrypts the clear text password into encrypted form
goto end

:end