connector readme

The coherence_connector is an example of how an Oracle Coherence cache can be exposed for access by the coherence_translator. 

Setup:

-	add the coherence.jar to the coherence_connector/lib directory

Coherence can be downloaded from:   http://www.oracle.com/technetwork/middleware/coherence/downloads/index.html



Build:

run mvn clean install



Deployment:

deploy the following files to the jboss as server

-  copy target/coherence_connector-0.1.rar  to  server/<profile>/deploy/teiid directory
-  copy src/main/resources/coherence-ds.xml to  server/<profile>/deploy  directory
-  copy the coherence.jar to the <profile>/lib directory

-  see the coherence_translator for deployment
