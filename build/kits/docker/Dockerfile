#######################################################################
#                                                                     #
# Creates a base CentOS image JDK 7                                   #
# Adds JBoss EAP, Teiid Runtime, Web Console                          #
#                                                                     #
#######################################################################

# Use the centos base image
FROM jboss/base-jdk:7

MAINTAINER Ramesh Reddy <rareddy@jboss.org>


ENV WORKING_DIR /opt/jboss
ENV JBOSS_EAP jboss-eap-6.3
ENV JBOSS_HOME $WORKING_DIR/$JBOSS_EAP

USER root

RUN mkdir $WORKING_DIR/temp && cd  $WORKING_DIR/temp

ADD jboss-as-dist-${version.org.jboss.as}.zip ${WORKING_DIR}/temp/jboss-eap.zip
ADD teiid-console-dist-${version.teiid.console}-jboss-as7.zip ${WORKING_DIR}/temp/console.zip
ADD teiid-${project.version}-jboss-dist.zip ${WORKING_DIR}/temp/teiid.zip

RUN unzip -q -o -d $WORKING_DIR $WORKING_DIR/temp/jboss-eap.zip && \
    cd $JBOSS_HOME && \
    unzip -q -o $WORKING_DIR/temp/teiid.zip && \
    unzip -q -o $WORKING_DIR/temp/console.zip && \
    rm -fdR $WORKING_DIR/temp

RUN chown -R jboss:jboss ${JBOSS_HOME}/ #\

VOLUME ["/opt/jboss/$JBOSS_EAP/standalone", "/opt/jboss/$JBOSS_EAP/domain"]

USER jboss

EXPOSE 9990 8080 31000 35432

#Run Teiid Server
CMD ["/bin/sh", "-c", "/opt/jboss/jboss-eap-6.3/bin/standalone.sh", "-c", "standalone-teiid.xml", "-b", "0.0.0.0", "-bmanagement", "0.0.0.0"]

