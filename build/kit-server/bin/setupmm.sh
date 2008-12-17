#!/bin/sh

#
# Copyright (c) 2008 Varsity Gateway LLC
#

# -----------------------------------------------------------------------------------------
# setupmm is run to complete the installation of a system.  
# This will setup the repository and install the first host
# for this system.
# After this is run, additional host(s) can added to this system
# by running systemhostsetup.sh
# -----------------------------------------------------------------------------------------


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

cd ${PRGDIR}

# load the environment variables
. ./mmenv.sh


#if not run in silent mode then check if previously installed
if [ -z "$1" ] 
	then
	
# if the system directories alread exist, then prompt to determine
# if the system should be replaced

	if [ -f "${MM_ROOT}/lib/metamatrix.properties" ]
	    then

    	echo ""
		echo "The System has already been setup, continuing with installation"
		echo "will remove all hosts and all data in the repository will be deleted."
		printf "Do you wish to continue (Enter y to continue):"
		read INPUTCONT
		if [ "$INPUTCONT" = "y" -o "$INPUTCONT" = "Y" ]
	        then
				echo ""
	    else
			echo ""
			echo "Exit"
			exit        
		fi 
	fi
fi

export RESOLVED_HOST=`hostname`

ANT_ARGS=" -Dmetamatrix.message.bus.type=noop.message.bus"
ANT_ARGS="${ANT_ARGS} -Dmetamatrix.installationDir=${MM_ROOT}"
ANT_ARGS="${ANT_ARGS} -Dhost.name.resolved=${RESOLVED_HOST}"
ANT_ARGS="${ANT_ARGS} -propertyfile ${MM_ROOT}/config/install/setup_default.properties"

if [ -n "${JAVA_OPTS}" ]; then
	ANT_ARGS="${JAVA_OPTS} $ANT_ARGS "
fi


ANT_OPTS="-Xmx512m"
ANT_HOME=${MM_ROOT}/lib

# uncomment for additional debugging info in the logfile
# ANT_OPTS="${ANT_OPTS} -verbose"

if [ ! -x "${MM_ROOT}/log" ]; then
    echo "Create ${MM_ROOT}/log directory"
	mkdir ${MM_ROOT}/log
fi


LOGLEVEL=info

${MM_JAVA} -cp "${CLASSPATH}" -Dant.home="${ANT_HOME}" $ANT_OPTS org.apache.tools.ant.Main $ANT_ARGS -buildfile ${MM_ROOT}/config/install/setup_server.xml install.server 


# change exec persmissions on the newly installed hosts directory

find ${MM_ROOT}/  \( -name "*.sh" -o -name "*.jar" \)  -exec  chmod -R 755 {} \;

# Remove all the .cmd scripts
find ${MM_ROOT} \( -name "*.cmd" -o -name "*.bat" \)  -exec  rm -f {} \;




