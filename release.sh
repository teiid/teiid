#!/bin/bash
set -e
# simple release script
# ./release.sh [full]
# full will also build/release the javadocs and releasenotes

version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
version=`echo $version | cut -d'-' -f 1`

mvn -B -DreleaseVersion=$version -P release release:prepare
mvn release:perform

if [ "$1" = "full" ]; then
  cd target/checkout
  mvn -P final-release -pl '!build,!wildfly/wildfly-build' javadoc:aggregate
  rsync --recursive --protocol=29 --exclude='.*' target/site/apidocs teiid@filemgmt.jboss.org:/docs_htdocs/teiid/$version
fi
