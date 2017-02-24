package org.teiid.translator.object.testdata.annotated;

import java.util.Map;

import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;
import org.teiid.util.Version;


public class TestObjectConnection extends SimpleMapCacheConnection {

	public static ObjectConnection createConnection(Map<Object,Object> map, Version version) {
		CacheNameProxy proxy = new CacheNameProxy(TradesAnnotatedCacheSource.TRADES_CACHE_NAME);

		TestObjectConnection conn = new TestObjectConnection(map, TradesAnnotatedCacheSource.METHOD_REGISTRY, proxy);
		conn.setVersion(version);
		conn.setConfiguredUsingAnnotations(true);
		return conn;
	}

	public TestObjectConnection(Map<Object,Object> map, ClassRegistry registry, CacheNameProxy proxy) {
		super(map, registry, proxy);
		
		setPkField("tradeId");
		setCacheKeyClassType(java.lang.Integer.class);
		this.setCacheClassType(Trade.class);

	}	
	
	public TestObjectConnection(Map<Object,Object> primary, Map<Object,Object> staging, Map<Object,Object> alias, ClassRegistry registry, CacheNameProxy proxy) {
		super(primary, staging, alias, registry, proxy);
		
		setPkField("tradeId");
		setCacheKeyClassType(java.lang.Integer.class);
		this.setCacheClassType(Trade.class);

	}	
	
}
