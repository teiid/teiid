rem First one sets the path for the client
set CLIENT_PATH=.

rem Second one adds the Teiid client
set TEIID_PATH=../../../modules/org/jboss/teiid/common-core/main/teiid-common-core-${pom.version}.jar;../../../modules/org/jboss/teiid/client/main/teiid-client-${pom.version}.jar

java -cp %CLIENT_PATH%;%TEIID_PATH% JDBCClient %*

