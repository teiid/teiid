#!/bin/sh
#
# Copyright Red Hat, Inc. and/or its affiliates
# and other contributors as indicated by the @author tags and
# the COPYRIGHT.txt file distributed with this work.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

# JPDA options. Uncomment and modify as appropriate to enable remote debugging.
# JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n"

TEIID_CLASSPATH="$TEIID_HOME/lib/patches/*:$TEIID_HOME/lib/teiid-adminshell-${pom.version}.jar:$TEIID_HOME/lib/*"
JAVA_OPTS="$JAVA_OPTS -Xms128m -Xmx256m -XX:MaxPermSize=256m"
JAVA_OPTS="$JAVA_OPTS -Djava.util.logging.config.file=log.properties"

# Print the env settings
echo "======================================================================"
echo ""
echo "  Teiid AdminShell Bootstrap Environment"
echo ""
echo "  TEIID_HOME  = $TEIID_HOME"
echo "  CLASSPATH   = $TEIID_CLASSPATH"
echo "  JAVA        = $JAVA"
echo ""
echo "======================================================================"
echo ""

$JAVA $JAVA_OPTS -cp $TEIID_CLASSPATH -Xmx256m  org.teiid.adminshell.GroovyAdminConsole $*