package org.teiid.translator.object.infinispan;

import java.lang.annotation.ElementType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.transaction.TransactionMode;
import org.teiid.translator.object.CacheContainerWrapper;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.TestObjectConnection;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.testdata.TradesCacheSource;

public class TestInfinispanConnection extends TestObjectConnection {
	
	private static int cnt = 0;

	private DefaultCacheManager container;

	public TestInfinispanConnection(CacheContainerWrapper wrapper,
			DefaultCacheManager container) {
		super(wrapper);
		this.container = container;
	}

	@Override
	public void cleanUp() {
		this.container.stop();
	}

	public static ObjectConnection createConnection(String configFile)
			throws Exception {

		DefaultCacheManager container = new DefaultCacheManager(configFile);

		return createConnection(container);   	
		
	}
		
	public static ObjectConnection createConnection()
				throws Exception {
		
    	DefaultCacheManager container = createContainer();
    	container.startCache(TradesCacheSource.TRADES_CACHE_NAME);
  
		return createConnection(container);   	
	}
	
	public static DefaultCacheManager createContainer() throws Exception {
		SearchMapping mapping = new SearchMapping();
		mapping.entity(Trade.class).indexed().providedId().
		property("name", ElementType.METHOD).field().analyze(Analyze.NO).
		property("settled", ElementType.METHOD).field().analyze(Analyze.NO).
		property("tradeDate", ElementType.METHOD).field().analyze(Analyze.NO).
		property("tradeId", ElementType.METHOD).field().analyze(Analyze.NO);

		Properties properties = new Properties();
		properties.put(org.hibernate.search.Environment.MODEL_MAPPING, mapping);
		properties.put("lucene_version", "LUCENE_CURRENT");		
		properties.put("hibernate.search.default.directory_provider", "ram");
		
		GlobalConfiguration glob = new GlobalConfigurationBuilder()
        	.nonClusteredDefault() //Helper method that gets you a default constructed GlobalConfiguration, preconfigured for use in LOCAL mode
        	.globalJmxStatistics().enable() //This method allows enables the jmx statistics of the global configuration.
        	.jmxDomain("org.infinispan.trades." + ++cnt)  //prevent collision
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
		TradesCacheSource.loadCache(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));
	
		CacheContainerWrapper wrapper = new TestInfinispanCacheWrapper(
				container);
		
		ObjectConnection conn =  new TestInfinispanConnection(wrapper, container);
	
		return conn;  
	}

}

class TestInfinispanCacheWrapper extends CacheContainerWrapper {
	DefaultCacheManager dcm = null;

	public TestInfinispanCacheWrapper(DefaultCacheManager cacheMgr) {
		dcm = cacheMgr;
	}

	@Override
	public BasicCache getCache(String cacheName) {
		return dcm.getCache(cacheName);
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public Collection<Object> getAll(String cacheName) {
		Collection<Object> objs = new ArrayList<Object>();
		
		Map<String, Object> c = getCache(cacheName);

		Iterator s = c.keySet().iterator();
		for (String k : c.keySet()) {
			objs.add( c.get(k));
		}
		return objs;
	}
}
