#!/bin/sh

TEIID_PATH=../../lib/*:../../optional/file/*:../../optional/jdbc/*:../../optional/jdbc/h2/*:../../optional/*:../../optional/tm/*

javac -cp ${TEIID_PATH} src/org/teiid/example/*.java 

java -cp ./src:${TEIID_PATH} org.teiid.example.TeiidEmbeddedPortfolio "$@"
