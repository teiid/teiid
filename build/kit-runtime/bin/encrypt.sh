#!/bin/sh

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
    TEIID_HOME=`cd $DIRNAME/..; pwd`
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

if [ "x$1" = "x" ]; then
	echo "Provide text phrase to encrypt."
else
	"$JAVA" -classpath $TEIID_CLASSPATH com.metamatrix.common.util.crypto.CryptoUtil -key $KEYSTORE_FILE -encrypt $1
fi