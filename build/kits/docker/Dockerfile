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
ENV JBOSS_EAP teiid-${project.version}
ENV JBOSS_HOME $WORKING_DIR/$JBOSS_EAP

USER root

COPY teiid-${project.version}-wildfly-server.zip $WORKING_DIR/teiid.zip

RUN cd $WORKING_DIR && unzip -q teiid.zip && rm teiid.zip && chown -R jboss:jboss ${JBOSS_HOME}/ #\

VOLUME ["$JBOSS_HOME/standalone", "$JBOSS_HOME/domain"]

USER jboss

EXPOSE 9990 8080 31000 35432

#Run Teiid Server
CMD ["/bin/sh", "-c", "$JBOSS_HOME/bin/standalone.sh -c standalone-teiid.xml -b 0.0.0.0 -bmanagement 0.0.0.0"]
