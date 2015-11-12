package org.teiid.translator.infinispan.cache;

import java.lang.annotation.ElementType;
import java.util.Properties;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.transaction.TransactionMode;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.testdata.annotated.Trade;
import org.teiid.translator.object.testdata.annotated.TradesAnnotatedCacheSource;
import org.teiid.translator.object.testdata.trades.TradesCacheSource;

public class TestInfinispanConnection  {
	
	private static int cnt = 0;

	public static ObjectConnection createNonAnnotatedConnection(String configFile)
			throws Exception {

		DefaultCacheManager container = new DefaultCacheManager(configFile);
		
		container.startCache(TradesCacheSource.TRADES_CACHE_NAME);
		TradesCacheSource.loadCache(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));
		ObjectConnection conn = TradesCacheSource.createConnection(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));

		return conn;  

		
	}

	public static ObjectConnection createConnection(String configFile)
			throws Exception {

		DefaultCacheManager container = new DefaultCacheManager(configFile);

		return createConnection(container);   	
		
	}
		
	public static ObjectConnection createConnection(boolean useLucene)
				throws Exception {
		DefaultCacheManager container = null;
		
		if (useLucene) {
			container = createContainerForLucene();

		} else {
			container = createContainer();
		}
		
		container.startCache(TradesAnnotatedCacheSource.TRADES_CACHE_NAME);
		TradesAnnotatedCacheSource.loadCache(container.getCache(TradesAnnotatedCacheSource.TRADES_CACHE_NAME));
		return TradesAnnotatedCacheSource.createConnection(container.getCache(TradesAnnotatedCacheSource.TRADES_CACHE_NAME));

	}
	
	public static DefaultCacheManager createContainer() throws Exception {
		SearchMapping mapping = new SearchMapping();
		mapping.entity(Trade.class).indexed().providedId().
		property("name", ElementType.FIELD).field().analyze(Analyze.NO).
		property("settled", ElementType.FIELD).field().analyze(Analyze.NO).
		property("tradeDate", ElementType.FIELD).field().analyze(Analyze.NO);

		Properties properties = new Properties();
		properties.put(org.hibernate.search.cfg.Environment.MODEL_MAPPING, mapping);
		properties.put("lucene_version", "LUCENE_CURRENT");		
		properties.put("hibernate.search.default.directory_provider", "ram");
		
		
		GlobalConfiguration glob = new GlobalConfigurationBuilder()
        	.nonClusteredDefault() //Helper method that gets you a default constructed GlobalConfiguration, preconfigured for use in LOCAL mode
        	.globalJmxStatistics().enable() //This method allows enables the jmx statistics of the global configuration.
        	.jmxDomain("org.infinispan.trades." + ++cnt)  //prevent collision
        	.allowDuplicateDomains(true)
        	.build(); //Builds  the GlobalConfiguration object
		
		Configuration loc = new ConfigurationBuilder()
    		.indexing().enable().addProperty("hibernate.search.default.directory_provider", "filesystem").addProperty("hibernate.search.default.indexBase", "./target/lucene/indexes" + cnt)
    		.enable()
    		.indexLocalOnly(true)
    		.withProperties(properties)
		        .persistence().passivation(false).addSingleFileStore().purgeOnStartup(true).location("./target/localcache/indexing/trades" + cnt) //Disable passivation and adds a SingleFileStore that is purged on Startup
    		.build();

		return new DefaultCacheManager(glob, loc);
		
	}
	
	public static DefaultCacheManager createContainerForLucene()
			throws Exception {

	    GlobalConfiguration glob = new GlobalConfigurationBuilder()
	    .nonClusteredDefault() //Helper method that gets you a default constructed GlobalConfiguration, preconfigured for use in LOCAL mode
	    .globalJmxStatistics().enable() //This method allows enables the jmx statistics of the global configuration.
	   .jmxDomain("org.infinispan.trades.annotated." + ++cnt)  //prevent collision with non-transactional carmart
	    .build(); //Builds  the GlobalConfiguration object
	    Configuration loc = new ConfigurationBuilder()
		.indexing().enable().addProperty("hibernate.search.default.directory_provider", "filesystem").addProperty("hibernate.search.default.indexBase", "./target/lucene/indexes " + cnt)
	    .clustering().cacheMode(CacheMode.LOCAL) //Set Cache mode to LOCAL - Data is not replicated.
	    .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
	    .eviction().maxEntries(100).strategy(EvictionStrategy.LIRS) //Sets  4 as maximum number of entries in a cache instance and uses the LIRS strategy - an efficient low inter-reference recency set replacement policy to improve buffer cache performance
	    .persistence().passivation(false).addSingleFileStore().purgeOnStartup(true).location("./target/localcache/indexing/trades" + cnt) //Disable passivation and adds a SingleFileStore that is purged on Startup
	    .build(); //Builds the Configuration object
		DefaultCacheManager container = new DefaultCacheManager(glob, loc, true);
	
		return container;
	}
	
	private static ObjectConnection createConnection(DefaultCacheManager container) {
		container.startCache(TradesAnnotatedCacheSource.TRADES_CACHE_NAME);
		TradesAnnotatedCacheSource.loadCache(container.getCache(TradesAnnotatedCacheSource.TRADES_CACHE_NAME));
		ObjectConnection conn = TradesAnnotatedCacheSource.createConnection(container.getCache(TradesAnnotatedCacheSource.TRADES_CACHE_NAME));

		return conn;  
	}


}

