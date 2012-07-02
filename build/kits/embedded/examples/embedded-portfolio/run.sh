#!/bin/sh

TEIID_PATH=../../lib/*:../../optional/translator-file-8.1.0.Alpha3-SNAPSHOT.jar:../../optional/translator-jdbc-8.1.0.Alpha3-SNAPSHOT.jar:h2-1.3.161.jar

javac -cp ${TEIID_PATH} src/org/teiid/example/*.java 

java -cp ./src:${TEIID_PATH} org.teiid.example.TeiidEmbeddedPortfolio "$@"
