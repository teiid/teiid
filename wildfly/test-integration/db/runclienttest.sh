#!/bin/sh

#  This script will run the client tests 

#============================
#	REQUIRED PROPERTIES
#

#============================
#	OPTIONAL PROPERTIES
#
#  (if not set, will default to files within the db project)
#	-	QUERYSETDIR	-	directory location where the test querties can be found
#	-	SCENARIODIR	-	directory location where the scenario files found and determine which query sets and vdbs to use, 
#	-	DATASOURCEDIR	-	root directory location to find the various datasources to use
#	-	CONFIGFILE	-	specify the configuration file to use (override ctc-test.properties)

######################################################
#	DEBUGGING OPTION
#
#    JBEDSP_DEBUG - uncomment to enable the output of debug messages.
#                 
# JBEDSP_DEBUG=true
#

if [ -z "${QUERYSETDIR}" ] 
	then
	
	QUERYSETDIR='./src/main/resources/ctc_tests/queries'
	
fi

if [ -z "${SCENARIODIR}" ] 
	then
	
	SCENARIODIR='./src/main/resources/ctc_tests/scenarios'
	
fi


if [ -z "${DATASOURCEDIR}" ] 
	then
	
	DATASOURCEDIR='./src/main/resources/datasources'
fi

echo "Use Datasource directory: ${DATASOURCEDIR}"

#--------------------

# resolve links - $0 may be a softlink
LOC="$0"

while [ -h "$LOC" ] ; do
  ls=`ls -ld "$LOC"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    LOC="$link"
  else
    LOC=`dirname "$LOC"`/"$link"
  fi
done

LOCDIR=`dirname "$LOC"`
PRGDIR=`cd "$LOCDIR"; pwd`

cd "${PRGDIR}"

echo "============================"
echo "Running tests from ${PRGDIR}"
echo "============================"

# check if running from the kit
if [  -f "${PRGDIR}/resources/ctc_tests/ctc.xml" ]; then
   CTCXML=${PRGDIR}/resources/ctc_tests/ctc.xml
else
	CTCXML='./src/main/resources/ctc_tests/ctc.xml'
 
fi


ANT_ARGS=" -Dscenario.dir=${SCENARIODIR}"
ANT_ARGS="${ANT_ARGS} -Dqueryset.artifacts.dir=${QUERYSETDIR}"
#ANT_ARGS="${ANT_ARGS} -Dvdb.artifacts.dir=${vdb.artifacts.dir}"
ANT_ARGS="${ANT_ARGS} -Dproj.dir=${PRGDIR}"

# default to the ip address used to start the server
SVRNAME="0.0.0.0"

if [ ! -z "${SERVERNAME}" ] 
	then
	
	SVRNAME=${SERVERNAME}

fi

ANT_ARGS="${ANT_ARGS} -Dserver.host.name=${SVRNAME}"


if [ ! -z "${XMLCLZZ}" ] 
	then
	
	ANT_ARGS="${ANT_ARGS} -Dquery.scenario.classname=$XMLCLZZ"
	
fi

if [ ! -z "${CONFIGFILE}" ] 
	then
	
	ANT_ARGS="${ANT_ARGS} -Dconfig.file=${CONFIGFILE}"
	
fi

if [ ! -z "${PROPFILE}" ] 
	then
	
	ANT_ARGS=" -propertyfile $PROPFILE $ANT_ARGS "
	
fi


ANT_OPTS="-Xmx512m"
ANT_HOME=${PRGDIR}/ant

# uncomment for additional debugging info in the logfile
# turn on debug for additional debugging info in the logfile
if [ -n "$JBEDSP_DEBUG" ]; then
	ANT_ARGS="${ANT_ARGS} -verbose"
 fi
 
if [ ! -x "${PRGDIR}/log" ]; then
    echo "Create ${PRGDIR}/log directory"
	mkdir "${PRGDIR}"/log
fi

if [ -n "${JAVA_OPTS}" ]; then
	ANT_OPTS="${JAVA_OPTS} $ANT_OPTS "
fi

CP="${PRGDIR}:${PRGDIR}/ant/*"

LOGLEVEL=info

echo "ANT BUILDFILE=${CTCXML}"
echo "ANT_HOME=${ANT_HOME}"
echo "ANT_ARGS=${ANT_ARGS}"
echo "CP=$CP"

java -cp "${CP}" -Dant.home="${ANT_HOME}" $ANT_OPTS org.apache.tools.ant.Main $ANT_ARGS -buildfile ${CTCXML}






