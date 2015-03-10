#!/bin/sh

TEIID_PATH=../../lib/*:../../optional/file/*:../../optional/excel/*

javac -cp ${TEIID_PATH} src/org/teiid/example/*.java 

java -cp ./src:${TEIID_PATH} org.teiid.example.TeiidEmbeddedExcelDataSource "$@"
