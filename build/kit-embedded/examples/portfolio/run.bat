rem First one sets the path for client jars and VDB
set CLIENT_PATH=java/*;PortfolioModel/

rem Second one for the JARs in federate embedded
set FEDERATE_PATH=../../federate-0.0.1-SNAPSHOT-embedded.jar;../../lib/*;../../extensions/*

java -cp %CLIENT_PATH%;%FEDERATE_PATH% JDBCClient "select * from CustomerAccount"

