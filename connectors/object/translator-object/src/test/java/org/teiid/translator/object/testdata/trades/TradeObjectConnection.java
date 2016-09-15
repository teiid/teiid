package org.teiid.translator.object.testdata.trades;

import java.util.Map;

import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;
import org.teiid.util.Version;

public class TradeObjectConnection extends SimpleMapCacheConnection {

	public static ObjectConnection createConnection(Map<Object,Object> map, boolean staging, Version version, boolean annotations) {
		CacheNameProxy proxy = null;
		
		if (staging) {
			proxy = new CacheNameProxy(TradesCacheSource.TRADES_CACHE_NAME, "st_" + TradesCacheSource.TRADES_CACHE_NAME, "aliasCacheName");
		} else { 
			proxy = new CacheNameProxy(TradesCacheSource.TRADES_CACHE_NAME);
		}

		TradeObjectConnection conn =  new TradeObjectConnection(map, TradesCacheSource.CLASS_REGISTRY, proxy);
		conn.setVersion(version);
		conn.setConfiguredUsingAnnotations(annotations);
		return conn;
	}

	public TradeObjectConnection(Map<Object,Object> map, ClassRegistry registry, CacheNameProxy proxy) {
		super(map, registry, proxy);
		
		setPkField("tradeId");
		setCacheKeyClassType(Long.class);
//		this.setCacheName(TradesCacheSource.TRADES_CACHE_NAME);
		this.setCacheClassType(Trade.class);

	}

	
}


