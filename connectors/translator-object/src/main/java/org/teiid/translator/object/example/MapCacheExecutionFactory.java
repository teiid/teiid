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
package org.teiid.translator.object.example;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import javax.resource.cci.ConnectionFactory;

import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.object.ObjectCacheConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectMethodManager;
import org.teiid.translator.object.ObjectSourceProxy;
import org.teiid.translator.object.util.ObjectMethodUtil;

@Translator(name="mapCacheExample", description="The Example Map Cache Factory")
public class MapCacheExecutionFactory extends  ObjectExecutionFactory {
	private static final String LOADCACHE_METHOD_NAME = "loadCache";
	private String cacheLoaderClassName = null;
	
	private Map<Object, Object> cache = null;
	
	@Override
	public void start() throws TranslatorException {
		if (cacheLoaderClassName == null) {
			throw new TranslatorException("CacheLoaderClassName has not been set");
		}
		
		super.start();

		Class<?> clzz = ObjectMethodManager.loadClass(cacheLoaderClassName,
				this.getClass().getClassLoader());
		Method m = null;
		try {
			// because the method is static, pass in null for the object api
			Class<?>[] parms = null;
			m = clzz.getMethod(LOADCACHE_METHOD_NAME, parms);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			throw new TranslatorException("Method " + LOADCACHE_METHOD_NAME
					+ " was not defined on class " + cacheLoaderClassName);
		}
		
		try {
			cache = (Map) ObjectMethodUtil.executeMethod(m, null, Collections.EMPTY_LIST);
		} catch (Throwable e) {
			throw new TranslatorException(e);
		}

	}
	 
	
	/**
	 * <p>
	 * Returns the name of the class used to load the map cache.
	 * </P
	 * @return String name of class to use to load the map cache
	 */
	@TranslatorProperty(display="CacheLoaderClassName", advanced=true)
	public String getCacheLoaderClassName() {
		return this.cacheLoaderClassName;
	}
	
	/**
	 * <p>
	 * Set the name of the class used to load the cache map.  The only requirement of the class
	 * is it must define a <b>static method</b> called  <code>{@literal #LOADCACHE_METHOD_NAME}</code>
	 * and return <code>Map</code> of the cache.
	 * </p>
	 * @param String name of class to use to load the map cache
	 */
	public void setCacheLoaderClassName(String cacheLoaderClassName) {
		this.cacheLoaderClassName = cacheLoaderClassName;
	}
	
	protected Map<Object, Object> getCache() {
		return this.cache;
	}

	@Override
	protected ObjectSourceProxy createProxy(ObjectCacheConnection connection)
			throws TranslatorException {

		return new MapCacheProxy(connection, this);
	}

}
