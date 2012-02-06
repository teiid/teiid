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

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.ConverterCollections;
import com.tangosol.util.Filter;

/** 
 * Represents an implementation for the connection to a Coherence data source. 
 */
public class CoherenceConnectionImpl extends BasicConnection implements CoherenceConnection { 
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(CoherenceConnection.class);

	private NamedCache sourceCache  = null;
	private String translatorName = null;
	private String cacheName = null;
	
	
	public CoherenceConnectionImpl(CoherenceManagedConnectionFactory config) throws ResourceException {
		
		sourceCache = getCache(config.getCacheName());
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
		CacheFactory.releaseCache( sourceCache);
		sourceCache = null;

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
//		System.out.println("CoherenceConnection cacheName: " +  cacheName + " filter: " + (criteria != null ? criteria.toString() : "NULL" ));
		List<Object> objects = new ArrayList<Object>();
		
		Set<ConverterCollections.ConverterEntrySet> mapResult = (Set<ConverterCollections.ConverterEntrySet>) sourceCache
		.entrySet(criteria);
		

		for (Iterator it = mapResult.iterator(); it.hasNext();) {
			Map.Entry o = (Map.Entry) it.next();
			objects.add(o.getValue());
//			System.out.println("CoherenceConnection: loaded result " + o.getValue().toString() );

		}
		
//		System.out.println("CoherenceConnection: loaded " + objects.size() + " results ");

		return objects;

	}
	
	private NamedCache getCache(String name) {
		this.cacheName = name;
		
		sourceCache = CacheFactory.getCache(name);
		
		LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Coherence NamedCache " + cacheName + " has been obtained."); //$NON-NLS-1$

		return sourceCache;
		
	}


}
