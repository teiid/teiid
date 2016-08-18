/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.infinispan.hotrod;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.simpleMap.SimpleMapCacheConnection;
import org.teiid.translator.object.testdata.annotated.TestObjectConnection;
import org.teiid.translator.object.testdata.annotated.Trade;
import org.teiid.translator.object.testdata.annotated.TradesAnnotatedCacheSource;

/**
 * @author vanhalbert
 *
 */
public class TestInfinispanHotRodConnection extends SimpleMapCacheConnection implements InfinispanHotRodConnection {
	protected String version;
	
	public static ObjectConnection createConnection(Map<Object,Object> map) {
		CacheNameProxy proxy = new CacheNameProxy(TradesAnnotatedCacheSource.TRADES_CACHE_NAME);

		return new TestObjectConnection(map, TradesAnnotatedCacheSource.METHOD_REGISTRY, proxy);
	}

	public TestInfinispanHotRodConnection(Map<Object,Object> map, ClassRegistry registry, CacheNameProxy proxy) {
		super(map, registry, proxy);
		
		setPkField("tradeId");
		setCacheKeyClassType(java.lang.Integer.class);
		this.setCacheClassType(Trade.class);

	}	


	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {
		return null;
	}



	@Override
	public Collection<Object> getAll() throws TranslatorException {
		Map<Object,Object> objects = getCache();
		List<Object> results = new ArrayList<Object>();
		for (Object k:objects.keySet()) {
			Object v = objects.get(k);
			results.add(v);
			
		}
		return results;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.infinispan.hotrod.InfinispanHotRodConnection#getDescriptor()
	 */
	@Override
	public Descriptor getDescriptor() throws TranslatorException {
		return null;
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.simpleMap.SimpleMapCacheConnection#getVersion()
	 */
	@Override
	public String getVersion() {
		return version;
	}
	
	public void setVersion(String v) {
		this.version = v;
	}
}
