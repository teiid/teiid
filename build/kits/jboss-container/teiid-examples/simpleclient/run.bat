rem First one sets the path for the client
set CLIENT_PATH=.

rem Second one adds the Teiid client
set TEIID_PATH=../../lib/teiid-${pom.version}-client.jar

java -cp %CLIENT_PATH%;%TEIID_PATH% JDBCClient %*

