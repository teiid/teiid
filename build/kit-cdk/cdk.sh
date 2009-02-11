#!/bin/sh
#
# Teiid Connector Development Kit (CDK)
# JBoss, Home of Professional Open Source.
# Copyright (C) 2008 Red Hat, Inc.
# Licensed to Red Hat, Inc. under one or more contributor 
# license agreements.  See the copyright.txt file in the
# distribution for a full listing of individual contributors.
# 
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA.

# This assumes it's run from its installation directory. It is also assumed there is a java
# executable defined along the PATH

if [ ! -f "$JAVA_HOME" ]; then
   echo "Warning-JAVA_HOME is not defined, will try to use default java."
   JAVA="java"
else
	JAVA=${JAVA_HOME}/bin/java   
fi

if [ -e $(basename $0) ];
then
    TEIID_ROOT=`pwd`
else
    echo -e "\nPlease execute cdk.sh from the directory in which it is installed.\n"
    exit
fi

# Set up directories
TEIID_LIB=${TEIID_ROOT}/lib

# Set up classpath variables
PATCH_JAR=${TEIID_LIB}/patches/tools_patch.jar
CDK_CLASSPATH=$PATCH_JAR:${TEIID_LIB}/*:.:./*


# Print the env settings
echo ========================== ENV SETTINGS ==========================
echo TEIID_ROOT  = $TEIID_ROOT
echo CLASSPATH      = $CDK_CLASSPATH
echo ==================================================================

# Execute the cdk tool
$JAVA -cp $CDK_CLASSPATH -Xmx256m -Dmetamatrix.config.none -Dmetamatrix.log=4 com.metamatrix.cdk.ConnectorShell $*