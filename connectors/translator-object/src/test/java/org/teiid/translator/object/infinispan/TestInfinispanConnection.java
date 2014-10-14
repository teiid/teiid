package org.teiid.translator.object.infinispan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

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
import org.teiid.translator.object.util.TradesCacheSource;

public class TestInfinispanConnection extends TestObjectConnection {

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
//		container.removeCache(TradesCacheSource.TRADES_CACHE_NAME);
//		container.getCache(TradesCacheSource.TRADES_CACHE_NAME, true);

		return createConnection(container);   	
		
	}
		
		public static ObjectConnection createConnection()
				throws Exception {
	
        GlobalConfiguration glob = new GlobalConfigurationBuilder()
        .nonClusteredDefault() //Helper method that gets you a default constructed GlobalConfiguration, preconfigured for use in LOCAL mode
        .globalJmxStatistics().enable() //This method allows enables the jmx statistics of the global configuration.
       .jmxDomain("org.infinispan.trades")  //prevent collision with non-transactional carmart
        .build(); //Builds  the GlobalConfiguration object
    Configuration loc = new ConfigurationBuilder()
//        .jmxStatistics().enable() //Enable JMX statistics
    	.indexing().enable().addProperty("hibernate.search.default.directory_provider", "filesystem").addProperty("hibernate.search.default.indexBase", "./target/lucene/indexes")
        .clustering().cacheMode(CacheMode.LOCAL) //Set Cache mode to LOCAL - Data is not replicated.
        .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
        //.autoCommit(false) //Enable Transactional mode with autocommit false
//        .lockingMode(LockingMode.OPTIMISTIC).transactionManagerLookup(new GenericTransactionManagerLookup()) //uses GenericTransactionManagerLookup - This is a lookup class that locate transaction managers in the most  popular Java EE application servers. If no transaction manager can be found, it defaults on the dummy transaction manager.
//        .locking().isolationLevel(IsolationLevel.REPEATABLE_READ) //Sets the isolation level of locking
        .eviction().maxEntries(100).strategy(EvictionStrategy.LIRS) //Sets  4 as maximum number of entries in a cache instance and uses the LIRS strategy - an efficient low inter-reference recency set replacement policy to improve buffer cache performance
        .persistence().passivation(false).addSingleFileStore().purgeOnStartup(true).location("./target/localcache/indexing/trades") //Disable passivation and adds a SingleFileStore that is purged on Startup
        .build(); //Builds the Configuration object
    	DefaultCacheManager container = new DefaultCacheManager(glob, loc, true);
  
		return createConnection(container);   	
	}

	private static ObjectConnection createConnection(DefaultCacheManager container) {
		TradesCacheSource.loadCache(container
				.getCache(TradesCacheSource.TRADES_CACHE_NAME));
	
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
