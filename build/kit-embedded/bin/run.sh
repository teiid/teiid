#!/bin/sh
### ====================================================================== ###
##                                                                          ##
##  Teiid Bootstrap Script                                                  ##
##  (script borrowed from JBossAS)                                          ##
### ====================================================================== ###

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

# Add JMX support
JMX_PORT=9999
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

# JPDA options. Uncomment and modify as appropriate to enable remote debugging.
# JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n"

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

# generate teiid.keystore if does not exist.
KEYSTORE_FILE=$TEIID_HOME/deploy/teiid.keystore 
if [ ! -f $KEYSTORE_FILE ]; then
	"$JAVA" -classpath $TEIID_CLASSPATH com.metamatrix.common.util.crypto.CryptoUtil -genkey $KEYSTORE_FILE
	echo "A new key with keystore generated at $KEYSTORE_FILE"
fi

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
   if [ "x$LAUNCH_TEIID_IN_BACKGROUND" = "x" ]; then
      # Execute the JVM in the foreground
      "$JAVA" $JAVA_OPTS \
         -classpath "$TEIID_CLASSPATH" \
         -server \
         org.teiid.Server $TEIID_HOME/deploy.properties "$@"
      _STATUS=$?      
   else
      # Execute the JVM in the background
      "$JAVA" $JAVA_OPTS \
         -classpath "$TEIID_CLASSPATH" \
         -server \
         org.teiid.Server $TEIID_HOME/deploy.properties "$@" &
      _PID=$!
      echo $_PID > $TEIID_HOME/work/teiid.pid
      # Trap common signals and relay them to the process
      trap "kill -HUP  $_PID" HUP
      trap "kill -TERM $_PID" INT
      trap "kill -QUIT $_PID" QUIT
      trap "kill -PIPE $_PID" PIPE
      trap "kill -TERM $_PID" TERM
      # Wait until the background process exits
      WAIT_STATUS=128
      while [ "$WAIT_STATUS" -ge 128 ]; do
         wait $_PID 2>/dev/null
         WAIT_STATUS=$?
         if [ "${WAIT_STATUS}" -gt 128 ]; then
            SIGNAL=`expr ${WAIT_STATUS} - 128`
            SIGNAL_NAME=`kill -l ${SIGNAL}`
            echo "*** Teiid process (${_PID}) received ${SIGNAL_NAME} signal ***" >&2
         fi          
      done
      if [ "${WAIT_STATUS}" -lt 127 ]; then
         _STATUS=$WAIT_STATUS
      else
         _STATUS=0
      fi      
   fi

   if [ "$_STATUS" -eq 10 ]; then
      echo "Restarting ..."
   else
      exit $_STATUS
   fi
done

