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

package org.teiid.coherence.connector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.resource.ResourceException;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.ConverterCollections;
import com.tangosol.util.Filter;
import com.tangosol.util.QueryHelper;

/** 
 * Represents a connection to a Coherence data source. 
 */
public class CoherenceConnectionImpl extends BasicConnection implements CoherenceConnection { 
	
	private NamedCache tradesCache = null;
	
		
	public CoherenceConnectionImpl(CoherenceManagedConnectionFactory config) throws ResourceException {
		
		tradesCache = CacheFactory.getCache(config.getCacheName());

		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Coherence Connection has been newly created."); //$NON-NLS-1$
	}
	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
//		LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Coherence Connector has been closed."); //$NON-NLS-1$
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
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Coherence Connection is alive."); //$NON-NLS-1$
		return true;
	}
	
//	original example getting the legs in a trade
//	public List<Leg> getLegs(List<Long>  legIds) throws ResourceException {
//		
//		NamedCache tradesCache = CacheFactory.getCache("Trades");
//		Map legsMap = new HashMap();
//		
//	     Leg leg = new Leg();
//	     Set legsSet;
//	     Map.Entry legsEntry;
//	     
//	     List<Leg> legs = new ArrayList<Leg>();
//
//		Trade trade = null;
//	     for (int i=1; i<=3; i++){
//	         trade = (Trade)tradesCache.get(i);
//	         legsMap = trade.getLegs();
//	         legsSet = legsMap.entrySet();
//	         Iterator k = legsSet.iterator();
//	           while(k.hasNext()){
//	             legsEntry = (Map.Entry)k.next();
//	             leg = (Leg)legsEntry.getValue();
//	             if (matchId(leg, legIds)) {
//	             
//	            	 System.out.print("Leg Id: " + leg.getId() + " , notional value: " + leg.getNotional() + "\n");
//	            	 legs.add(leg);
//	             }
//	           }
//	       }
//	
//		return legs;
//	}
	
//	private boolean matchId(BaseID baseID, List<Long>  ids) {
//		Iterator<Long> it = ids.iterator();
//		while (it.hasNext()) {
//			Long id = it.next();
//			if (id.longValue() == baseID.getId()) return true;
//		}
//		return false;
//	}


	// original example passing in ids to find
//	public List<Object> getTrades(List<Long> ids) throws ResourceException {
//
//		List<Object> trades = new ArrayList();
//
//		if (ids == null || ids.isEmpty()) {
//			trades.addAll(tradesCache.values());
//
//		} else {
//
//			String parm = null;
//			for (Iterator<Long> it = ids.iterator(); it.hasNext();) {
//				Long t = it.next();
//				if (parm != null) {
//					parm += ",";
//				}
//				parm = String.valueOf(t);
//
//			}
//
//			// filter wouldn't work until the long "L" indicater was added to
//			// the parm
//			// the examples showed using a float 7.0f
//			Filter filter = QueryHelper.createFilter("Id = " + parm + "l");
//
//			Set mapResult = (Set) tradesCache.entrySet(filter);
//
//			for (Iterator it = mapResult.iterator(); it.hasNext();) {
//				Map.Entry o = (Map.Entry) it.next();
//				trades.add(o.getValue());
//			}
//
//		}
//
//		return trades;
//
//	}

	public List<Object> get(String criteria) throws ResourceException {
		List<Object> objects = null;
		if (criteria == null || criteria.trim().length() == 0) {
			objects = new ArrayList(tradesCache.size());
			objects.addAll(tradesCache.values());
			return objects;

		}

		objects = new ArrayList();
		Filter filter = QueryHelper.createFilter(criteria);

		Set<ConverterCollections.ConverterEntrySet> mapResult = (Set<ConverterCollections.ConverterEntrySet>) tradesCache
				.entrySet(filter);

		for (Iterator it = mapResult.iterator(); it.hasNext();) {
			Map.Entry o = (Map.Entry) it.next();
			objects.add(o.getValue());
		}
		return objects;

	}


}
