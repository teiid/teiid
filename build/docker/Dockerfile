#######################################################################
#                                                                     #
# Creates a base CentOS image JDK 8                                   #
# Adds JBoss EAP, Teiid Runtime, Web Console                          #
#                                                                     #
#######################################################################

# Use the centos base image
FROM jboss/base-jdk:8

MAINTAINER Ramesh Reddy <rareddy@jboss.org>

ENV WORKING_DIR /opt/jboss
ENV JBOSS_EAP teiid-@project.version@
ENV JBOSS_HOME $WORKING_DIR/$JBOSS_EAP

USER root

COPY maven/ $WORKING_DIR

RUN cd $WORKING_DIR && chown -R jboss:jboss ${JBOSS_HOME}/ #\

VOLUME ["$JBOSS_HOME/standalone", "$JBOSS_HOME/domain"]

USER jboss

ENV LAUNCH_JBOSS_IN_BACKGROUND true

EXPOSE 9990 8080 31000 35432

#Run Teiid Server
CMD ["/bin/sh", "-c", "$JBOSS_HOME/bin/standalone.sh -c standalone-teiid.xml -b 0.0.0.0 -bmanagement 0.0.0.0"]
