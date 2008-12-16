#!/bin/sh

#
# ##COPYRIGHT##
#

######################################################
# This script is used to set the environment for the Federate scripts
#
#    MM_DEBUG - If this is defined (with any value), the script
#                       will emit debug messages.
# MM_DEBUG=true

#    MM_ROOT - The location of the MetaMatrix installation.
#MM_ROOT=

#    MM_JAVA_HOME - The location of the JRE that the server will
#                          use. This will be ignored if
#                          MM_JAVA_EXE_FILE_PATH is set.
#                          If this and MM_JAVA_EXE_FILE_PATH are
#                          not set, then JAVA_HOME JRE will be used.
#
#MM_JAVA_HOME=/opt/java


#    MM_JAVA_EXE_FILE_PATH - Defines the full path to the Java
#                                   executable to use. If this is set,
#                                   MM_JAVA_HOME is ignored.
#                                   If this is not set, then
#                                   $MM_JAVA_HOME/bin/java
#                                   is used. If this and
#                                   MM_JAVA_HOME are not set, then
#                                   JAVA_HOME JRE will be used.
#
#MM_JAVA_EXE_FILE_PATH=/usr/local/bin/java

#    MM_JAVA_OPTS - Java VM command line options to be
#                          passed into the executing VM. If this is not defined
#                          this script will pass in a default set of options.
#                          If this is set, it completely overrides the
#                          defaults. If you only want to add options
#                          to the agent's defaults, then you will want to
#                          use MM_ADDITIONAL_JAVA_OPTS instead.
#
#MM_JAVA_OPTS="-Xms160M -Xmx160M -Djava.net.preferIPv4Stack=true"

#    MM_ADDITIONAL_JAVA_OPTS - additional Java VM command line options
#                                     to be passed into the executing VM. This
#                                     is added to MM_JAVA_OPTS; it
#                                     is mainly used to augment the
#                                     default set of options. This can be
#                                     left unset if it is not needed.
#
#MM_ADDITIONAL_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,address=9797,server=y,suspend=n"

# 
######################################################

clear

# ----------------------------------------------------------------------
# Subroutine that simply dumps a message iff debug mode is enabled
# ----------------------------------------------------------------------
debug_msg ()
{
   if [ -n "$MM_DEBUG" ]; then
      echo $1
   fi
}

# if MM_ROOT is already set, then do not change
if [ -z "${MM_ROOT}" ] 
	then

	cd `pwd`/..
	MM_ROOT=`pwd`; export MM_ROOT

fi

# Return to the bin directory
cd ${MM_ROOT}/bin

debug_msg "MM_ROOT: $MM_ROOT"

#####################################################

# ----------------------------------------------------------------------
# Find the Java executable and verify we have a VM available
# ----------------------------------------------------------------------


if [ -z "$MM_JAVA_EXE_FILE_PATH" ]; then
   if [ -z "$MM_JAVA_HOME" ]; then
         debug_msg "No JRE specified - will try to use JAVA_HOME: $JAVA_HOME"
         MM_JAVA_HOME=$JAVA_HOME
   fi  
   debug_msg "MM_JAVA_HOME: $MM_JAVA_HOME"
   MM_JAVA_EXE_FILE_PATH=${MM_JAVA_HOME}/bin/java
fi
debug_msg "MM_JAVA_EXE_FILE_PATH: $MM_JAVA_EXE_FILE_PATH"

if [ ! -f "$MM_JAVA_EXE_FILE_PATH" ]; then
   echo "There is no JRE/JDK specified, will try to use java."
   echo "To use a specific java, set MM_JAVA_HOME, MM_JAVA_EXE_FILE_PATH or JAVA_HOME appropriately."
   JAVA="java"
 
fi

MM_JAVA=$MM_JAVA_EXE_FILE_PATH; export MM_JAVA

_JAR_FILES=`ls -1 ${MM_ROOT}/lib/*.jar`
for _JAR in $_JAR_FILES ; do
   if [ -z "$JAR_LIB" ]; then
      JAR_LIB="${_JAR}"
   else
      JAR_LIB="${JAR_LIB}:${_JAR}"
   fi
   debug_msg "JAR entry: $_JAR"
done
debug_msg "JAR LIB: $JAR_LIB}"


CLASSPATH=${MM_ROOT}/config:${JAR_LIB}

export CLASSPATH


###################################################################################
#
# Set the temp location used for temporary files by MetaMatrix
#
JAVA_OPTS=" -Djava.io.tmpdir=$MM_ROOT/data/temp"

if [ -n "$MM_JAVA_OPTS" ]; then
	JAVA_OPTS="$JAVA_OPTS $MM_JAVA_OPTS"
fi


if [ -n "$MM_ADDITIONAL_JAVA_OPTS" ]; then
	JAVA_OPTS="$JAVA_OPTS $MM_ADDITIONAL_JAVA_OPTS"
fi

export JAVA_OPTS

#################################################################################################
# Print the env settings
debug_msg "========================== ENV SETTINGS =========================="
debug_msg "MM_ROOT     	= ${MM_ROOT}"
debug_msg "MM_JAVA     	= ${MM_JAVA}"
debug_msg "JAVA_OPTS	= ${JAVA_OPTS}"
debug_msg "CLASSPATH	= ${CLASSPATH}"
debug_msg "========================== ENV SETTINGS =========================="


