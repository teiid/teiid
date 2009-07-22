#!/bin/sh
### ====================================================================== ###
##                                                                          ##
##  Teiid Bootstrap Script                                                  ##
##  (script borrowed from JBossAS)                                          ##
### ====================================================================== ###

DIRNAME=`dirname $0`
PROGNAME=`basename $0`

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

# Add JMX support
JMX_PORT=9999

# JPDA options. Uncomment and modify as appropriate to enable remote debugging.
# JAVA_OPTS=$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=y

# Force IPv4 on Linux systems since IPv6 doesn't work correctly with jdk5 and lower
if [ "$linux" = "true" ]; then
   JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true"
fi


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
    TEIID_HOME=`cd $DIRNAME/..; pwd`
fi
export TEIID_HOME
cd $TEIID_HOME

# Setup the JVM
if [ "x$JAVA" = "x" ]; then
    if [ "x$JAVA_HOME" != "x" ]; then
	JAVA="$JAVA_HOME/bin/java"
    else
	JAVA="java"
    fi
fi

TEIID_CLASSPATH="$TEIID_HOME/lib/patches/*:$TEIID_HOME/deploy:$TEIID_HOME/client/*:$TEIID_HOME/lib/*"

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
    TEIID_HOME=`cygpath --path --windows "$TEIID_HOME"`
    JAVA_HOME=`cygpath --path --windows "$JAVA_HOME"`
    TEIID_CLASSPATH=`cygpath --path --windows "$TEIID_CLASSPATH"`
fi

JAVA_OPTS="$JAVA_OPTS -Dteiid.home=$TEIID_HOME"

# Display our environment
echo "========================================================================="
echo ""
echo "  Teiid Bootstrap Environment"
echo ""
echo "  TEIID_HOME: $TEIID_HOME"
echo ""
echo "  JAVA: $JAVA"
echo ""
echo "  JAVA_OPTS: $JAVA_OPTS"
echo ""
echo "  CLASSPATH: $TEIID_CLASSPATH"
echo ""
echo "========================================================================="
echo ""

while true; do
  "$JAVA" $JAVA_OPTS \
     -classpath "$TEIID_CLASSPATH" \
     -server \
     org.teiid.Shutdown $TEIID_HOME/deploy.properties $JMX_PORT "$@"
  _STATUS=$?      

   if [ "$_STATUS" -eq 10 ]; then
      echo "Restarting ..."
   else
      exit $_STATUS
   fi
done

