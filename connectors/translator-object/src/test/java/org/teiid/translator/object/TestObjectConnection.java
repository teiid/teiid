package org.teiid.translator.object;

import java.util.Map;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.infinispan.DSLSearch;
import org.teiid.translator.object.infinispan.InfinispanExecutionFactory;
import org.teiid.translator.object.infinispan.LuceneSearch;
import org.teiid.translator.object.testdata.Leg;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.testdata.TradesCacheSource;
import org.teiid.translator.object.testdata.Transaction;

public class TestObjectConnection implements ObjectConnection {
	private ClassRegistry methodUtil = new ClassRegistry();

	private CacheContainerWrapper wrapper;

	public TestObjectConnection(CacheContainerWrapper wrapper) {
		this.wrapper = wrapper;
		
		try {
			methodUtil.registerClass(Trade.class);
			methodUtil.registerClass(Leg.class);
			methodUtil.registerClass(Transaction.class);
		} catch (TranslatorException e) {
			e.printStackTrace();
		}

	}

	@Override
	public Class<?> getType(String name) throws TranslatorException {
		return Trade.class;
	}

	@Override
	public String getPkField(String name) {
		return "tradeId";
	}

	@Override
	public CacheContainerWrapper getCacheContainer() throws TranslatorException {
		return this.wrapper;
	}

	@Override
	public Map<String, Class<?>> getCacheNameClassTypeMapping() {
		return TradesCacheSource.mapOfCaches;
	}

	@Override
	public void cleanUp() {
		wrapper = null;
		methodUtil = null;
	}

	@Override
	public ClassRegistry getClassRegistry() {
		return methodUtil;
	}

	
}
