@ECHO OFF

setLocal EnableDelayedExpansion
set mainClasspath=lib\*;optional\*
set mainClasspath=%mainClasspath%;optional\file\*;optional\jdbc\*;optional\jdbc\h2\*;optional\tm\*
set mainClasspath=%mainClasspath%;optional\excel\*
set mainClasspath=%mainClasspath%;optional\webservice\*
set mainClasspath=%mainClasspath%;optional\mongodb\*
set mainClasspath=%mainClasspath%;optional\cassandra\*
set mainClasspath=%mainClasspath%;optional\ldap\*
set mainClasspath=%mainClasspath%;optional\hbase\*

set mainClass=org.teiid.example.TeiidExamplesApp

echo "Usage: runExamples.bat"
echo "For example: runExamples.bat"
echo "Some notes:"
echo "- Working dir should be the directory of this script."
echo "- Java is recommended to be JDK and java 7 for optimal performance"
echo "- The environment variable JAVA_HOME should be set to the JDK installation directory"
echo "  For example: set JAVA_HOME="C:\Program Files\Java\jdk1.7.0"
echo
echo "Starting examples app..."

rem You can use -Xmx128m or less too, but it might be slower
if exist %JAVA_HOME%\bin\java.exe (
    %JAVA_HOME%\bin\java -Xms256m -Xmx512m -server -cp %mainClasspath% %mainClass%
) else (
    java -Xms256m -Xmx512m -cp %mainClasspath% %mainClass%
)
