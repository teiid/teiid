connector readme

The coherence_connector is an example of how an Oracle Coherence cache can be exposed for access by the translator-coherence. 

Design Note:
- 	The cacheName is defined in the coherence-ds.xml.   This ties the cache to the physical model.  If a different cache is needed,
 	then a new/different coherence-ds.xml will need to be deployed.  And that will also require a new physical model to be created that
 	is assigned to this new datasource in the vdb.
 	
 	Why?  Coherence, at the time of writing this, doesn't support doing joins across caches.  However, Teiid can enable this by this design.
 	
-   The CacheTranslatorClassName is defined in the coherence-ds.xml.   This is an implementation that extends org.teiid.translator.coherence.SourceCacheAdapter,
	which is located in the tranlator-coherence project.  The parameter setting has been set in the coherence-ds.xml so that it can be changed without
	having to update the vdb in designer and redeploy the vdb.	

-------------------------
To compile and run tests:
-------------------------

-	add the coherence.jar to the connector_coherence/lib directory

Coherence can be downloaded from:   http://www.oracle.com/technetwork/middleware/coherence/downloads/index.html

Build:

run mvn clean install


-------------------------
Deployment:
-------------------------

deploy the following files to the jboss as server

-  copy target/connector-coherence-<version>.rar  to  server/<profile>/deploy/teiid directory
-  copy src/main/resources/coherence-ds.xml to  server/<profile>/deploy  directory
	a. Set the CacheName in the coherence-ds.xml
	b. Set the CacheTranslatorClassName in the coherence-ds.xml
-  copy the coherence.jar to the <profile>/lib directory

-  see the translator_coherence to deploy the translator deployment
