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
package org.teiid.resource.adapter.coherence;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import javax.resource.ResourceException;

public class CoherenceManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private static final long serialVersionUID = -1832915223199053471L;
	private static final String DEFAULT_CACHE_ADAPTER_CLASS_NAME = "org.teiid.translator.coherence.SourceCacheAdapter";
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CoherenceManagedConnectionFactory.class);

	private String cacheName;
	private String cacheTranslatorClassName = null;

	
	
	@Override
	public BasicConnectionFactory createConnectionFactory() throws ResourceException {
		if (cacheName == null) {
			throw new ResourceException(UTIL.getString("CoherenceManagedConnectionFactory.cachename_not_set")); //$NON-NLS-1$
		}
		
		if (cacheTranslatorClassName == null) {
			cacheTranslatorClassName = DEFAULT_CACHE_ADAPTER_CLASS_NAME;
		}	
		
		return new BasicConnectionFactory() {

			private static final long serialVersionUID = 1L;

			@Override
			public CoherenceConnectionImpl getConnection() throws ResourceException {
				return new CoherenceConnectionImpl(CoherenceManagedConnectionFactory.this);
			}
		};
	}	
	
	public String getCacheName() {
		return this.cacheName;
	}
	
	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}
	
    
	public String getCacheTranslatorClassName() {
		return cacheTranslatorClassName;
	}
	
	public void setCacheTranslatorClassName(String className) {
		this.cacheTranslatorClassName = className;
	}
	
}
