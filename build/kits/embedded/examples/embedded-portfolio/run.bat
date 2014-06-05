TEIID_PATH=..\..\lib\*;..\..\optional\file\*;..\..\optional\jdbc\*;h2-1.3.161.jar

javac -cp %TEIID_PATH% src\org\teiid\example\*.java 

java -cp .\src;%TEIID_PATH% org.teiid.example.TeiidEmbeddedPortfolio %*

