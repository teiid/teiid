package org.teiid.resource.adapter.infinispan;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.libmode.InfinispanCacheConnection;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.SearchType;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;
import org.teiid.translator.object.testdata.annotated.TradesAnnotatedCacheSource;

public class TradeAnnotatedObjectConnection extends SimpleMapCacheConnection implements InfinispanCacheConnection {

	
	public TradeAnnotatedObjectConnection(Map<Object,Object> map, ClassRegistry registry, CacheNameProxy proxy) {
		super(map, registry, proxy);
		setPkField(TradesAnnotatedCacheSource.TRADE_PK_KEY_NAME);
		setCacheKeyClassType(TradesAnnotatedCacheSource.TRADE_PK_KEY_CLASS);
		this.setCacheClassType(TradesAnnotatedCacheSource.TRADE_CLASS_TYPE);
	}

	/**
	 * {@inheritDoc}
	 *
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {
		return Search.getQueryFactory((Cache) getCache());

	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.simpleMap.SimpleMapCacheConnection#getSearchType()
	 */
	@Override
	public SearchType getSearchType() {
		return new DSLSearch(this);
	}

	
	
}


