#!/bin/sh

# First one sets the path for client jars and VDB
CLIENT_PATH=java/*:PortfolioModel/

#Second one for the JARs in federate embedded
FEDERATE_PATH=../../federate-0.0.1-SNAPSHOT-embedded.jar:../../lib/*

java -cp ${CLIENT_PATH}:${FEDERATE_PATH} JDBCClient "select * from CustomerAccount"
