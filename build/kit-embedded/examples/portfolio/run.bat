rem First one sets the path for client jars and VDB
set CLIENT_PATH=java/*;PortfolioModel/

rem Second one for the JARs in Teiid embedded
set TEIID_PATH=../../teiid-${pom.version}-client.jar;../../deploy;../../lib/patches/*;../../lib/*;../../extensions/*

java -cp %CLIENT_PATH%;%TEIID_PATH% JDBCClient "select * from CustomerAccount"

