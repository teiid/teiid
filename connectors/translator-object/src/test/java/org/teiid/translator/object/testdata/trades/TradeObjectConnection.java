package org.teiid.translator.object.testdata.trades;

import java.util.Map;

import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;

public class TradeObjectConnection extends SimpleMapCacheConnection {

	public static ObjectConnection createConnection(Map<Object,Object> map) {
		return new TradeObjectConnection(map, TradesCacheSource.CLASS_REGISTRY);
	}

	public TradeObjectConnection(Map<Object,Object> map, ClassRegistry registry) {
		super(map, registry);
		
		setPkField("tradeId");
		setCacheKeyClassType(Long.class);
		this.setCacheName(TradesCacheSource.TRADES_CACHE_NAME);
		this.setCacheClassType(Trade.class);

	}

	
}


