#!/bin/bash

## Parse Teiid version out of POM.
TEIID_VERSION=$(xmllint --xpath '/*/*[local-name()="version"]/text()' pom.xml)
#TEIID_VERSION=8.9.0.Beta1

SOURCES=(
    #"https://sourceforge.net/projects/teiid/files/teiid-${TEIID_VERSION}-jboss-dist.zip"
    "http://download.jboss.org/jbosseap/6/jboss-eap-6.1.0.Alpha/jboss-eap-6.1.0.Alpha.zip"
    "http://sourceforge.net/projects/resteasy/files/Resteasy%20JAX-RS/2.3.6.Final/resteasy-jaxrs-2.3.6.Final-all.zip"
    "http://sourceforge.net/projects/teiid/files/webconsole/1.2/Final/teiid-console-dist-1.2.0.Final-jboss-as7.zip"
)

set -e

echo "Building install for Teiid $TEIID_VERSION."

## Build Teiid.
#mvn clean install -P release -s settings.xml

mkdir -p install
cd install

## Download extra packages for server.
echo "Downloading extra packages..."
wget -nc ${SOURCES[@]}
ln -sf ../build/target/teiid-${TEIID_VERSION}-jboss-dist.zip .

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
unzip -q -o ../../teiid-${TEIID_VERSION}-jboss-dist.zip
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
