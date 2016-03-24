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

import java.lang.annotation.Annotation;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.InfinispanDSLConnection;
import org.teiid.translator.infinispan.dsl.TestInfinispanDSLConnection;
import org.teiid.translator.object.CacheNameProxy;
import org.teiid.translator.object.ClassRegistry;

/**
 * @author vanhalbert
 *
 */
public class PersonCacheConnection extends TestInfinispanDSLConnection {
	
	protected Descriptor descriptor;
	

	public static InfinispanDSLConnection createConnection(RemoteCache map, boolean useKeyClassType, Descriptor descriptor) {
		CacheNameProxy proxy = new CacheNameProxy(PersonCacheSource.PERSON_CACHE_NAME);

		return new PersonCacheConnection(map, PersonCacheSource.CLASS_REGISTRY, proxy, useKeyClassType, descriptor);
	}
	
	/**
	 * @param map
	 * @param registry
	 * @param proxy
	 * @param useKeyClassType 
	 * @param desc 
	 */
	public PersonCacheConnection(RemoteCache map,
			ClassRegistry registry, CacheNameProxy proxy, boolean useKeyClassType, Descriptor desc) {
		super(map, registry, proxy);
		this.descriptor = desc;
		
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

		@Override
		public Descriptor getDescriptor() throws TranslatorException {
			return descriptor;
		}

		
		@Override
		public QueryFactory getQueryFactory() throws TranslatorException {
			return null;
		}

}
