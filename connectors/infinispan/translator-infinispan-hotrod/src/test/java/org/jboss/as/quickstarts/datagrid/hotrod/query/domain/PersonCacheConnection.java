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
package org.jboss.as.quickstarts.datagrid.hotrod.query.domain;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.InfinispanHotRodConnection;
import org.teiid.translator.infinispan.hotrod.TestInfinispanHotRodConnection;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.util.Version;

/**
 * @author vanhalbert
 *
 */
public class PersonCacheConnection extends TestInfinispanHotRodConnection {
	
	public static InfinispanHotRodConnection createConnection(RemoteCache map, final String keyField, boolean useKeyClassType, boolean staging, Version version) {
		CacheNameProxy proxy = null;
		
		if (staging) {
			proxy = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME, "ST_" + PersonCacheSource.PERSON_CACHE_NAME, "aliasCacheName");
		} else { 
			proxy = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME);
		}

		PersonCacheConnection conn = new PersonCacheConnection(map, PersonCacheSource.CLASS_REGISTRY, proxy, useKeyClassType) {
			
			@Override
			public void setPkField(String keyfield) {
				super.setPkField(keyField);
			}
		};
		conn.setVersion(version);
		conn.setConfiguredUsingAnnotations(true);
		return conn;
	}
	
	public static InfinispanHotRodConnection createConnection(RemoteCache map, boolean useKeyClassType, Version version) {
		return createConnection(map, "id", useKeyClassType, version);
//		CacheNameProxy proxy = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME);
//
//		PersonCacheConnection conn = new PersonCacheConnection(map, PersonCacheSource.CLASS_REGISTRY, proxy, useKeyClassType);
//		conn.setVersion(version);
//		conn.setConfiguredUsingAnnotations(true);
//		return conn;
	}
	
	public static InfinispanHotRodConnection createConnection(RemoteCache map, final String keyField, boolean useKeyClassType, Version version) {
		return createConnection(map, keyField, useKeyClassType, false, version);
//		CacheNameProxy proxy = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME);
//		PersonCacheConnection conn = new PersonCacheConnection(map, PersonCacheSource.CLASS_REGISTRY, proxy, useKeyClassType) {
//			
//			@Override
//			public void setPkField(String keyfield) {
//				super.setPkField(keyField);
//			}
//		};
//		conn.setVersion(version);
//		conn.setConfiguredUsingAnnotations(true);
//		return conn;

	}
	
	/**
	 * @param map
	 * @param registry
	 * @param proxy
	 * @param useKeyClassType 
	 */
	public PersonCacheConnection(RemoteCache map,
			ClassRegistry registry, CacheNameProxy proxy, boolean useKeyClassType) {
		super(map, registry, proxy);
		
		setPkField("id");
		if (useKeyClassType) {
			setCacheKeyClassType(int.class);
		}
		
		String p = "org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person";
		Class<?> c = loadClass(p);
		ProtoField ax = c.getAnnotation(ProtoField.class);
		this.setCacheClassType(c);
	}
	
	protected Class<?> loadClass(String className)  {
		try {
			return Class.forName(className, false, this.getClass().getClassLoader());
		} catch (ClassNotFoundException e) {
			return null;
		}
	}
	

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.infinispan.hotrod.InfinispanHotRodConnection#getDescriptor(java.lang.Class)
	 */
	@Override
	public Descriptor getDescriptor(Class<?> clz) throws TranslatorException {
		return PersonCacheSource.DESCRIPTORS.get(clz.getName());
	}

	@Override
	public Descriptor getDescriptor() throws TranslatorException {
		return PersonCacheSource.DESCRIPTORS.get(PersonCacheSource.PERSON_CLASS_NAME);
	}

	
	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {
		return null;
	}

}
