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
package org.jboss.teiid.jdg_remote.pojo;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.InfinispanHotRodConnection;
import org.teiid.translator.infinispan.hotrod.TestInfinispanHotRodConnection;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;

/**
 * @author vanhalbert
 *
 */
public class AllTypeCacheConnection extends TestInfinispanHotRodConnection {
	
	protected Descriptor descriptor;
	

	public static InfinispanHotRodConnection createConnection(RemoteCache map, boolean useKeyClassType, Descriptor descriptor) {
		CacheNameProxy proxy = new CacheNameProxy(AllTypesCacheSource.ALLTYPES_CACHE_NAME);

		return new AllTypeCacheConnection(map, AllTypesCacheSource.CLASS_REGISTRY, proxy, useKeyClassType, descriptor);
	}

	
	/**
	 * @param map
	 * @param registry
	 * @param proxy
	 * @param useKeyClassType 
	 * @param desc 
	 */
	public AllTypeCacheConnection(RemoteCache map,
			ClassRegistry registry, CacheNameProxy proxy, boolean useKeyClassType, Descriptor desc) {
		super(map, registry, proxy);
		this.descriptor = desc;
		
		setPkField("intKey");
		if (useKeyClassType) {
			setCacheKeyClassType(Integer.class);
		}
		this.setCacheClassType(AllTypes.class);
	}

		@Override
		public Descriptor getDescriptor() throws TranslatorException {
			return descriptor;
		}

		
		@Override
		public QueryFactory getQueryFactory() throws TranslatorException {
			return null;
		}

}
