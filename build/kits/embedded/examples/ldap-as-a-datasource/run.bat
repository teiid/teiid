TEIID_PATH=..\..\lib\*;..\..\optional\ldap\*

javac -cp %TEIID_PATH% src\org\teiid\example\*.java 

java -cp .\src;%TEIID_PATH% org.teiid.example.TeiidEmbeddedLDAPDataSource %*

