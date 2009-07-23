#!/bin/sh

# First one sets the path for the client
CLIENT_PATH=.

#Second one for the Teiid client jar
TEIID_PATH=../../client/teiid-${pom.version}-client.jar

java -cp ${CLIENT_PATH}:${TEIID_PATH} JDBCClient "$@"
