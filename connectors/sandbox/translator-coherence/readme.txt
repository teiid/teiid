translator readme

The coherence_translator is very simple implementation of mapping a Coherence cache to relational sql request.    


PREREQUSITE:
-  build the connector-coherence project.
-  Need to implement/extend SourceCacheAdapter, primarily to add the source metadata.  See TradesCacheSource in the tests as an example.


BUILD:
---------
run  mvn clean install



DEPLOYMENT
--------------------

setup 

	1.	see coherence_connector for deployment
	2.	copy the translator-coherence-<version>.jar to server/<profile>/deploy/teiid/connectors directory



Exmaple: To use the example vdb (Trade.vdb), do the following (with the server shutdown)

	1.	set above "setup"
	2.	copy the Trade.vdb, located in src/test/resources/Coherence_Designer_Project, to <profile>/deploy directory
	3.	in coherence-ds.xml, set the CacheName property to "Trades" and CacheTranslatorClassName property to "org.teiid.translator.coherence.TradesCacheSource"
		TradeCacheSource will load the cache upon initial use
	4.  start JBoss server
	5.  Then use a sql tool (i.e., SQuirreL) to access the Trade.vdb and view the Trade information



Other notes:
-	the coherence translator has the translator name of "coherence", which must match the translator defined in the vdb that's mapped to the Coherence physical source.

