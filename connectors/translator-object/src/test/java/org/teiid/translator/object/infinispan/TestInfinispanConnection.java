package org.teiid.translator.object.infinispan;

import java.util.Map;

import org.infinispan.manager.DefaultCacheManager;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.CacheContainerWrapper;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.util.TradesCacheSource;

public class TestInfinispanConnection  implements ObjectConnection {
	
		private  CacheContainerWrapper wrapper;
		private DefaultCacheManager container;

		public TestInfinispanConnection(CacheContainerWrapper wrapper, DefaultCacheManager container) {
			this.wrapper = wrapper;
			this.container = container;
		}

		@Override
		public Class<?> getType(String name) throws TranslatorException {
			return Trade.class;
		}
		
		@Override
		public String getPkField(String name) {
			return  "tradeId";
		}

		@Override
		public CacheContainerWrapper getCacheContainer()
				throws TranslatorException {
			return this.wrapper;
		}

		@Override
		public Map<String, Class<?>> getCacheNameClassTypeMapping() {
			return TradesCacheSource.mapOfCaches;
		}
		
		  public void cleanUp(){
			  this.container.stop();
		  }
		  
		public static ObjectConnection createConnection(String configFile) throws Exception {
				
				DefaultCacheManager container = new DefaultCacheManager(configFile);
				
				TradesCacheSource.loadCache(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));

				CacheContainerWrapper  wrapper = new TestInfinispanCacheWrapper(container);

				return  new TestInfinispanConnection(wrapper, container);
			}	
		  
}

class TestInfinispanCacheWrapper extends InfinispanCacheWrapper {
	DefaultCacheManager dcm = null;

	public TestInfinispanCacheWrapper(DefaultCacheManager cacheMgr) {
		dcm = cacheMgr;
	}

	@Override
	public Object getCache(String cacheName) {
		return dcm.getCache(cacheName);
	}
}
