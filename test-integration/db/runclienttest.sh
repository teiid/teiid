#!/bin/sh

#  This script will run the client tests based on the
#  below project scenarios.  

#  The scenario

SC_DIR=./src/main/resources/ctc_tests/scenarios
QS_DIR=./src/main/resources/ctc_tests/queries
# VDB_DIR=./src/main/resources/ctc_tests/vdbs

VDB_DIR=./src/main/resources/transactions

mvn integration-test  -Dscenario.dir=$SC_DIR -Dqueryset.artifacts.dir=$QS_DIR -Dvdb.artifacts.dir=$VDB_DIR -Prunclienttests



