#!/bin/bash
## Usage: ./install.sh
## Description: Build Teiid and run install steps to produce standalone server.

## Parse Teiid version out of POM.
TEIID_VERSION=$(xmllint --xpath '/*/*[local-name()="version"]/text()' pom.xml)
TEIID_DIST=teiid-${TEIID_VERSION}-jboss-dist.zip

SOURCES=(
    "jboss-eap.zip"
    "resteasy-jaxrs.zip"
    "teiid-console-dist.zip"
)

declare -A SOURCE_URL SOURCE_SHA1

SOURCE_URL["jboss-eap.zip"]="http://download.jboss.org/jbosseap/6/jboss-eap-6.1.0.Alpha/jboss-eap-6.1.0.Alpha.zip"
SOURCE_SHA1["jboss-eap.zip"]="630d81f83b851077e3ad129924502bbdf0c1552a"

SOURCE_URL["resteasy-jaxrs.zip"]="http://sourceforge.net/projects/resteasy/files/Resteasy%20JAX-RS/2.3.6.Final/resteasy-jaxrs-2.3.6.Final-all.zip"
SOURCE_SHA1["resteasy-jaxrs.zip"]="cfcb2aaa60cd954d04e73cc7e99509ec38b5538a"

SOURCE_URL["teiid-console-dist.zip"]="http://sourceforge.net/projects/teiid/files/webconsole/1.2/Final/teiid-console-dist-1.2.0.Final-jboss-as7.zip"
SOURCE_SHA1["teiid-console-dist.zip"]="7b57b77520f2894b0f48a385f7dcff03898bb514"

set -e

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

mkdir -p install
cd install

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
ln -sf ../build/target/$TEIID_DIST .

rm -rf temp
mkdir temp
cd temp

## Unpack JBoss container.
echo "Unpacking JBoss..."
unzip -q ../jboss-eap-6.1.0.Alpha.zip
echo

## Patch JAX-RS.
echo "Patching JAX-RS module..."
rm -rf jboss-eap-6.1/modules/system/layers/base/org/jboss/resteasy/resteasy-jaxrs
unzip -q ../resteasy-jaxrs-2.3.6.Final-all.zip resteasy-jaxrs-2.3.6.Final/resteasy-jboss-modules-2.3.6.Final.zip
unzip -q resteasy-jaxrs-2.3.6.Final/resteasy-jboss-modules-2.3.6.Final.zip
mv -f org/jboss/resteasy/resteasy-jaxrs jboss-eap-6.1/modules/system/layers/base/org/jboss/resteasy/resteasy-jaxrs
echo

## Install Teiid.
echo "Installing Teiid..."
cd jboss-eap-6.1
unzip -q -o ../../teiid-console-dist-1.2.0.Final-jboss-as7.zip
unzip -q -o ../../$TEIID_DIST
cd ..
echo

## Package server.
echo "Zipping up install..."
mv -f jboss-eap-6.1 teiid-${TEIID_VERSION}
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
