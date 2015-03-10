TEIID_PATH=..\..\lib\*;..\..\optional\mongodb\*;..\..\optional\jdbc\*

javac -cp %TEIID_PATH% src\org\teiid\example\*.java 

java -cp .\src;%TEIID_PATH% org.teiid.example.TeiidEmbeddedMongoDBDataSource %*

