#!/bin/sh

TEIID_PATH=../../lib/*:../../optional/cassandra/*:../../optional/netty-${version.io.netty}.jar

javac -cp ${TEIID_PATH} src/org/teiid/example/*.java 

java -cp ./src:${TEIID_PATH} org.teiid.example.TeiidEmbeddedCassandraDataSource "$@"
