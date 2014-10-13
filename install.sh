#!/bin/bash
## Usage: ./install.sh
## Description: Build Teiid and run install steps to produce standalone server.

set -e

## Parse Teiid version out of POM.
TEIID_VERSION=$(xmllint --xpath '/*/*[local-name()="version"]/text()' pom.xml)
TEIID_DIST=teiid-${TEIID_VERSION}-jboss-dist.zip

SOURCES=(
    "jboss-eap.zip"
    "teiid-console-dist.zip"
)

declare -A SOURCE_URL SOURCE_SHA1

SOURCE_URL["jboss-eap.zip"]="http://maven.repository.redhat.com/techpreview/eap6/6.3.0.Alpha/maven-repository/org/jboss/as/jboss-as-dist/7.4.0.Final-redhat-4/jboss-as-dist-7.4.0.Final-redhat-4.zip"
SOURCE_SHA1["jboss-eap.zip"]="15978363d25acee751afa35af582548dace95480"

SOURCE_URL["teiid-console-dist.zip"]="http://sourceforge.net/projects/teiid/files/webconsole/1.2/Final/teiid-console-dist-1.2.0.Final-jboss-as7.zip"
SOURCE_SHA1["teiid-console-dist.zip"]="7b57b77520f2894b0f48a385f7dcff03898bb514"

echo "Making install for Teiid $TEIID_VERSION..."
echo

if [ ! -e build/target/$TEIID_DIST ];
then
    echo "Running Maven because $TEIID_DIST not found."
    mvn clean install -P release -s settings.xml
    echo
else
    echo "Skipping Maven because $TEIID_DIST found."
    echo
fi

mkdir -p target/install
cd target/install

## Download extra packages for server.
echo "Downloading packages..."
echo
for NAME in "${SOURCES[@]}";
do
    URL="${SOURCE_URL[$NAME]}"
    
    ## Download if doesn't exist.
    if [ ! -e "$NAME" ];
    then	
	echo "Downloading $NAME..."
	wget -O "$NAME" "$URL"
	echo
    fi    
done

## Verify sources match checksums.
echo "Verifying checksums..."
echo
for NAME in "${SOURCES[@]}";
do
    SHA1="${SOURCE_SHA1[$NAME]}"
    
    ## Verify checksum.
    echo "$SHA1 $NAME" | sha1sum -c -
    echo
done

## Create softlink for Teiid dist.
ln -sf ../../build/target/$TEIID_DIST .

rm -rf temp
mkdir temp
cd temp

## Unpack JBoss container.
echo "Unpacking JBoss..."
unzip -q ../jboss-eap.zip
echo

## Install Teiid.
echo "Installing Teiid..."
cd jboss-eap-6.3
unzip -q -o ../../teiid-console-dist.zip
unzip -q -o ../../$TEIID_DIST
cd ..
echo

## Package server.
echo "Zipping up install..."
mv -f jboss-eap-6.3 teiid-${TEIID_VERSION}
rm -f ../teiid-${TEIID_VERSION}.zip
zip -r -q ../teiid-${TEIID_VERSION}.zip teiid-${TEIID_VERSION}
cd ..
echo

echo "Install is located here:"
echo "$(pwd)/teiid-${TEIID_VERSION}.zip"
echo
echo "Extract it somewhere and run this to start:"
echo "bin/standalone.sh -c standalone-teiid.xml"
echo
