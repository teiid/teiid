TEIID Translator Arche Type
================

This maven project is use to create the TEIID Translator archetype.

To package and install the arche type, run:

mvn -s ../settings.xml install 


After the arche type is installed, then to generate a translator project, do the following:

-  'cd' into the connectors directory
-  run the following:

mvn archetype:generate                                  \
  -DarchetypeGroupId=org.jboss.teiid                \
  -DarchetypeArtifactId=translator-archetype          \
  -DarchetypeVersion=${archetype-version}               \
  -DgroupId=${my.groupid}                               \
  -DartifactId=${my-artifactId}			\
  -Dversion=${my-pomversion}

where:
	${archetype-version} - is the Teiid version of the arche type to use
	${my.groupid} - is your new group id to be inserted in the newly created pom.xml
		This will also be your default package name for java classes.
	${my-artifactId} - is your new artifact id to be inserted in the newly created pom.xml
        ${my-pomversion} -  is the version to be inserted in the pom

Example:

mvn archetype:generate  -s ../settings.xml                                 \
  -DarchetypeGroupId=org.jboss.teiid               \
  -DarchetypeArtifactId=translator-archetype          \
  -DarchetypeVersion=8.7.0.Alpha1-SNAPSHOT   	\
  -DgroupId=org.teiid.translator   	\
  -DartifactId=translator-custom	\
  -Dversion=8.7.0.Alpha1-SNAPSHOT



When executed, you will be asked to confirm the package property

Confirm properties configuration:
groupId: org.teiid.translator
artifactId: translator-custom/
version: 8.7.0.Alpha1-SNAPSHOT
package: org.teiid.translator
 Y: : 

type Y (yes) and press enter, and the creation of the translator project will be done
type N (no) and you will be prompted to change the package property. 
