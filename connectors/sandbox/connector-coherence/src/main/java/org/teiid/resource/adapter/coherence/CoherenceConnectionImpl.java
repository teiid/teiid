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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.BundleUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.ConverterCollections;
import com.tangosol.util.Filter;
import com.tangosol.util.TransactionMap;

import javax.resource.ResourceException;

/** 
 * Represents an implementation for the connection to a Coherence data source. 
 */
public class CoherenceConnectionImpl extends BasicConnection implements CoherenceConnection { 
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CoherenceConnection.class);

	private String translatorName = null;
	private String cacheName = null;
	
	
	public CoherenceConnectionImpl(CoherenceManagedConnectionFactory config) throws ResourceException {
		this.cacheName = config.getCacheName();
		translatorName = config.getCacheTranslatorClassName();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Coherence Connection has been newly created"); //$NON-NLS-1$
	}
	
	public String getCacheTranslatorClassName() {
		return  this.translatorName;
		
	}

	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Coherence NamedCache " + cacheName + " has been released."); //$NON-NLS-1$
	}

	/** 
	 * Currently, this method always returns alive. We assume the connection is alive,
	 * and rely on proper timeout values to automatically clean up connections before
	 * any server-side timeout occurs. Rather than incur overhead by rebinding,
	 * we'll assume the connection is always alive, and throw an error when it is actually used,
	 * if the connection fails. This may be a more efficient way of handling failed connections,
	 * with the one tradeoff that stale connections will not be detected until execution time. In
	 * practice, there is no benefit to detecting stale connections before execution time.
	 * 
	 * One possible extension is to implement a UnsolicitedNotificationListener.
	 * (non-Javadoc)
	 */
	public boolean isAlive() {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Coherence Connection is alive for namedCache " + cacheName); //$NON-NLS-1$
		return true;
	}
	
	public List<Object> get(Filter criteria) throws ResourceException {
		List<Object> objects = new ArrayList<Object>();
		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Coherence Connection: Filter - " + (criteria != null ? criteria.toString() : "NULL"));
		
		Set<ConverterCollections.ConverterEntrySet> mapResult = (Set<ConverterCollections.ConverterEntrySet>)  getCache()
		.entrySet(criteria);
		

		for (Iterator it = mapResult.iterator(); it.hasNext();) {
			Map.Entry o = (Map.Entry) it.next();
			objects.add(o.getValue());
		}
		return objects;

	}
	
	public void add(Object key, Object value) throws ResourceException {
		 
			NamedCache sourceCache =  getCache();
			if (sourceCache.containsKey(key)) {
				throw new ResourceException("Unable to add object for key: " + key + " to cache " + this.cacheName + ", because it already exist");
			}
			
			TransactionMap tmap = CacheFactory.getLocalTransaction(sourceCache);

			tmap.setTransactionIsolation(TransactionMap.TRANSACTION_REPEATABLE_GET);
			tmap.setConcurrency(TransactionMap.CONCUR_PESSIMISTIC);
			
			tmap.begin();
			try
			    {
			    tmap.put(key, value);
			    tmap.prepare();
			    tmap.commit();
			    }
			catch (Exception e) {
				throw new ResourceException(e);
			}
			
			sourceCache = getCache();
			if (!sourceCache.containsKey(key)) {
				throw new ResourceException("Problem adding object for key: " + key + " to the cache " + this.cacheName +", object not found after add");
			}
		
	}
	
	public void update(Object key, Object object) throws ResourceException {
		NamedCache sourceCache =  getCache();
		if (!sourceCache.containsKey(key)) {
			throw new ResourceException("Unable to update object for key: " + key + " to cache " + this.cacheName + ", because it already exist");
		}
		
		TransactionMap tmap = CacheFactory.getLocalTransaction(sourceCache);

		tmap.setTransactionIsolation(TransactionMap.TRANSACTION_REPEATABLE_GET);
		tmap.setConcurrency(TransactionMap.CONCUR_PESSIMISTIC);
		
		tmap.begin();
		try
		    {
		    tmap.put(key, object);
		    tmap.prepare();
		    tmap.commit();
		    }
		catch (Exception e) {
			throw new ResourceException(e);
		}
		
		sourceCache = getCache();
		if (!sourceCache.containsKey(key)) {
			throw new ResourceException("Problem updating object for key: " + key + " to the cache " + this.cacheName +", object not found after add");
		}

		
	}
	
	public void remove(Object key) throws ResourceException {
		System.out.println("Remove: " + key);
		NamedCache sourceCache =  getCache();
		if (!sourceCache.containsKey(key)) {
			throw new ResourceException("Unable to remove object for key: " + key + " from cache " + this.cacheName + ", because it doesn't exist");
		}
		
		TransactionMap tmap = CacheFactory.getLocalTransaction(sourceCache);

		tmap.setTransactionIsolation(TransactionMap.TRANSACTION_REPEATABLE_GET);
		tmap.setConcurrency(TransactionMap.CONCUR_OPTIMISTIC);
		
		tmap.begin();
		try
		    {
		    tmap.remove(key);
		    tmap.prepare();
		    tmap.commit();
		    }
		catch (Exception e) {
			throw new ResourceException(e);

		}
		
		if (getCache().containsKey(key)) {
			throw new ResourceException("Unable to remove object for key: " + key + " from the cache " + this.cacheName );
		}

	}
	

	private NamedCache getCache() {
		
		NamedCache sourceCache = CacheFactory.getCache(this.cacheName, this.getClass().getClassLoader());
		
		LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Coherence NamedCache " + cacheName + " has been obtained."); //$NON-NLS-1$

		return sourceCache;
		
	}

}
