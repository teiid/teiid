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

prompt() {
	echo "usage:$0 -create"
	echo "usage:$0 -encrypt <plain-text-password> "
}

KEYSTORE_FILE=$TEIID_HOME/deploy/teiid.keystore 

if [ $# -eq 0 ]
then
    prompt;
else
	if [ "${1}" = "-create" ]
	then
		# generate teiid.keystore if does not exist.
		if [ ! -f $KEYSTORE_FILE ]; then	
			"$JAVA" -classpath $TEIID_CLASSPATH com.metamatrix.common.util.crypto.CryptoUtil -genkey $KEYSTORE_FILE
			echo "A new key with keystore generated at $KEYSTORE_FILE"
		else
			echo "$KEYSTORE_FILE already exists. Delete the current one if you would like to create a new keystore"
		fi	
	else 
		if [ "${1}" = "-encrypt" -a "${2}" != "" ]; then
		    if [ -f $KEYSTORE_FILE ]; then	
				"$JAVA" -classpath $TEIID_CLASSPATH com.metamatrix.common.util.crypto.CryptoUtil -key $KEYSTORE_FILE -encrypt $2
            else 
                echo "$KEYSTORE_FILE not found. Create a keystore first by using "
                echo "$0 -create"
            fi
		else
            prompt
		fi
	fi
fi
