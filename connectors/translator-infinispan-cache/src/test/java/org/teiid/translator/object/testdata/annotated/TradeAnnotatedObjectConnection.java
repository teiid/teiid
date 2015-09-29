package org.teiid.translator.object.testdata.annotated;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.infinispan.cache.InfinispanCacheConnection;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;

public class TradeAnnotatedObjectConnection extends SimpleMapCacheConnection implements InfinispanCacheConnection {

	public static ObjectConnection createConnection(Map<Object,Object> map) {
		return new TradeAnnotatedObjectConnection(map, TradesAnnotatedCacheSource.CLASS_REGISTRY);
	}

	public TradeAnnotatedObjectConnection(Map<Object,Object> map, ClassRegistry registry) {
		super(map, registry);
	}

	@Override
	public String getPkField() {
		return TradesAnnotatedCacheSource.TRADE_PK_KEY_NAME;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheKeyClassType()
	 */
	@Override
	public Class<?> getCacheKeyClassType()  {
		return TradesAnnotatedCacheSource.TRADE_PK_KEY_CLASS;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheName()
	 */
	@Override
	public String getCacheName()  {
		return TradesAnnotatedCacheSource.TRADES_CACHE_NAME;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheClassType()
	 */
	@Override
	public Class<?> getCacheClassType()  {
		return TradesAnnotatedCacheSource.TRADE_CLASS_TYPE;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.infinispan.cache.InfinispanCacheConnection#getQueryFactory()
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public QueryFactory getQueryFactory() {
		return Search.getQueryFactory((Cache) getCache());

	}
	
}


