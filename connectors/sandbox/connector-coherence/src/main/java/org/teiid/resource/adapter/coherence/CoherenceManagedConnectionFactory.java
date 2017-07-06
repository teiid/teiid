/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
