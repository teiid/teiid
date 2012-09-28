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
package org.teiid.translator.object.mapcache;

import java.util.Map;

import javax.resource.cci.ConnectionFactory;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.JavaBeanMetadataProcessor;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectPlugin;

/**
 * The MapCacheExecutionFactory provides a translator that supports a cache of type Map.
 * The cache will be looked up using a @link {@link #setCacheJndiName(String) JNDI name};
 * 
 * @author vhalbert
 *
 */
@Translator(name = "map-cache", description = "The Map Cache Factory")
public class MapCacheExecutionFactory extends ObjectExecutionFactory {
	
	private volatile Map<?,?> cache = null;

	public MapCacheExecutionFactory() {
		this.setSourceRequired(false);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		
	      String jndiName = getCacheJndiName();
          if (jndiName == null || jndiName.trim().length() == 0) {
	    			String msg = ObjectPlugin.Util
	    			.getString(
	    					"MapCacheExecutionFactory.undefinedJndiName"); //$NON-NLS-1$
	        		throw new TranslatorException(msg); 

          }				

	}

    protected synchronized Map<?,?> getCache() throws TranslatorException {
    	if (this.cache != null) return this.cache;
    	
    	Object object = findCacheUsingJNDIName();
    	
		if (object instanceof Map<?,?>) {
		    
			cache = (Map<?,?>)object;
			
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, "=== Using CacheManager (obtained from JNDI ==="); //$NON-NLS-1$

		} else {
			String msg = ObjectPlugin.Util.getString(
					"MapCacheExecutionFactory.unexpectedCacheType", //$NON-NLS-1$
					new Object[] { (object == null ? "nullObject" : object.getClass().getName()), "Map" }); //$NON-NLS-1$ //$NON-NLS-2$ 
			throw new TranslatorException(msg); 
		}
    	
    	return this.cache;
    }
	
 
    
	@Override
	public ObjectConnection getConnection(ConnectionFactory factory,
			ExecutionContext executionContext) throws TranslatorException {

		return new MapCacheConnection(this);

	}
	
	   @Override
		public void getMetadata(MetadataFactory metadataFactory, ObjectConnection conn)
				throws TranslatorException {
	 		JavaBeanMetadataProcessor processor = new JavaBeanMetadataProcessor();
			processor.getMetadata(metadataFactory, this);
		}
}
