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

package org.teiid.resource.adapter.infinispan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCache;
import org.teiid.resource.adapter.custom.spi.BasicConnection;
import org.teiid.translator.object.SearchCriterion;
import org.teiid.translator.object.infinispan.InfinispanCacheConnection;


/** 
 * Represents an implementation of the connection to an Infinispan cache. 
 */
public class InfinispanConnectionImpl extends BasicConnection implements InfinispanCacheConnection { 

	private InfinispanManagedConnectionFactory config;
	
	public InfinispanConnectionImpl(InfinispanManagedConnectionFactory config) throws ResourceException {
		this.config = config;
	}

	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		config = null;
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
		return (config == null ? false : config.getRemoteCacheManager().isStarted());
	}
	
    @Override
	public List<Object> get(SearchCriterion criterion, String cacheName, Class<?> rootNodeType) throws Exception {
		List<Object> results = null;
		
		if (! criterion.isRootTableInSelect() ) {
			return Collections.EMPTY_LIST;
		}
    	
		RemoteCache<Object, Object> cache = config.getRemoteCacheManager().getCache(cacheName);
		
		if (criterion.getOperator() == SearchCriterion.Operator.ALL) {
    		Map<Object, Object> c = cache.getBulk();
    		results = new ArrayList<Object>();
    		for (Iterator it = c.keySet().iterator(); it.hasNext();) {
    			Object v = cache.get(it.next());
    			addValue(v, results, rootNodeType);	

    		}	
    		
    		return results;
		}
		
		if (criterion.getAddCondition() != null) {
			results = get(criterion.getAddCondition(), cacheName, rootNodeType);
		}
		
		if (results == null) {
	    	results = new ArrayList<Object>();
		}
		
		if (criterion.getOperator().equals(SearchCriterion.Operator.EQUALS)) {
		
				Object v = cache.get(criterion.getValue());
				if (v != null) {
					addValue(v, results, rootNodeType);	
				}
    	} else if (criterion.getOperator().equals(SearchCriterion.Operator.IN)) {
    		
    		List<Object> parms = (List) criterion.getValue();
			for (Iterator<Object> it=parms.iterator(); it.hasNext();) {
				Object arg = it.next();
				Object v = cache.get(arg);
				if (v != null) {
					addValue(v, results, rootNodeType);	
				}
			}
    	
    	} 
//    	else if (criterion.getOperator().equals(SearchCriterion.Operator.LIKE)) {
//    	// not supported yet
//    	}
    	
    	return results;
		
	}
	
	private void addValue(Object value, List<Object> results, Class rootNodeType) {
		if (value != null && value.getClass().equals(rootNodeType)) {
			
			if (value.getClass().isArray()) {
				List<Object> listRows = Arrays.asList((Object[]) value);
				results.addAll(listRows);
				return;
			}
			
			if (value instanceof Collection) {
				results.addAll((Collection) value); 
				return;
			} 
			
			if (value instanceof Map) {
				Map<?,Object> mapRows = (Map) value;
				results.addAll(mapRows.values());
				return;
			}
			
			results.add(value);
		}

	}
	
}
