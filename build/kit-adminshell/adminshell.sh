#!/bin/sh
#
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

DIRNAME=`dirname $0`

# OS specific support (must be 'true' or 'false').
cygwin=false;
linux=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;
        
    Linux)
        linux=true
        ;;
esac

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
    [ -n "$TEIID_HOME" ] &&
        TEIID_HOME=`cygpath --unix "$TEIID_HOME"`
    [ -n "$JAVA_HOME" ] &&
        JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
fi

# Setup TEIID_HOME
if [ "x$TEIID_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    TEIID_HOME=`cd $DIRNAME; pwd`
fi
export TEIID_HOME

# Setup the JVM
if [ "x$JAVA" = "x" ]; then
    if [ "x$JAVA_HOME" != "x" ]; then
	JAVA="$JAVA_HOME/bin/java"
    else
	JAVA="java"
    fi
fi

TEIID_CLASSPATH="$TEIID_HOME/lib/patches/*:$TEIID_HOME/lib/*"


# Print the env settings
echo "========================================================================="
echo ""
echo "  Teiid AdminSehll Bootstrap Environment"
echo ""
echo "	TEIID_HOME  = $TEIID_HOME"
echo "	CLASSPATH   = $TEIID_CLASSPATH"
echo "	JAVA        = $JAVA"
echo ""
echo "========================================================================="
echo ""

$JAVA -cp $TEIID_CLASSPATH -Xmx256m  org.teiid.AdminShell $*