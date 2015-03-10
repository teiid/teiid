#!/bin/sh

TEIID_PATH=../../lib/*:../../optional/hbase/*:../../optional/jdbc/*

javac -cp ${TEIID_PATH} src/org/teiid/example/*.java 

java -cp ./src:${TEIID_PATH} org.teiid.example.TeiidEmbeddedHBaseDataSource "$@"
