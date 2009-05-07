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

package org.teiid.dqp.internal.datamgr.impl;

import java.util.Arrays;

import org.teiid.connector.api.ConnectorException;
import org.teiid.dqp.internal.cache.CacheID;
import org.teiid.dqp.internal.cache.CacheResults;
import org.teiid.dqp.internal.cache.ResultSetCache;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.sql.lang.Command;

public class ConnectorWorkItemFactory {
	
    private final static char DELIMITER = '.';

	private ResultSetCache rsCache;
	private ConnectorManager manager;
	private boolean synchWorkers;

	/**
	 * A work item that can get results from cache.
	 */
	private final class CachedResultsConnectorWorkItem extends
			AsynchConnectorWorkItem {
		private final CacheID cacheID;

		private CachedResultsConnectorWorkItem(AtomicRequestMessage message,
				ConnectorManager manager,
				ResultsReceiver<AtomicResultsMessage> resultsReceiver,
				CacheID cacheID) {
			super(message, manager, resultsReceiver);
			this.cacheID = cacheID;
		}

		@Override
		protected void createExecution()
				throws MetaMatrixComponentException,
				ConnectorException {
		}
		
		@Override
		protected void processNewRequest() throws ConnectorException {
			handleBatch();
		}

		@Override
		protected void handleBatch() throws ConnectorException {
			int firstRow = rowCount + 1;
			//already in cache
			CacheResults results = rsCache.getResults(cacheID, new int[]{firstRow, firstRow + requestMsg.getFetchSize() -1});
			this.rowCount = rowCount + results.getResults().length;
			if(results.isFinal()){
				this.lastBatch = true;
			}

		    LogManager.logTrace(LogConstants.CTX_DQP, 
		                        new Object[] { "CacheSynchQueryExecution - returnning batch from cache, startRow =",  //$NON-NLS-1$
		                                       new Integer(firstRow),
		                                       ", endRow =", //$NON-NLS-1$
		                                       new Integer(rowCount)});
		    sendResults(Arrays.asList(results.getResults()));
		}
	}

	/**
	 * Intercepts results for cachable commands
	 */
	public class CachedResultsReceiver implements ResultsReceiver<AtomicResultsMessage> {
		
		private ResultsReceiver<AtomicResultsMessage> actual;
		private AtomicRequestID requestId;
		private CacheID cacheID;
		private int firstRow = 1;
		
		public CachedResultsReceiver(ResultsReceiver<AtomicResultsMessage> actual,
				CacheID cacheID, AtomicRequestID requestId) {
			this.actual = actual;
			this.cacheID = cacheID;
			this.requestId = requestId;
		}

		@Override
		public void receiveResults(AtomicResultsMessage results) {
			boolean isFinal = results.getFinalRow() >= 0;
			if (results.isRequestClosed()) {
				rsCache.removeTempResults(cacheID, requestId);
			} else {
				CacheResults cr = new CacheResults(results.getResults(), firstRow, isFinal);
				firstRow += results.getResults().length;
				rsCache.setResults(cacheID, cr, requestId);
			}
			actual.receiveResults(results);
		}

		@Override
		public void exceptionOccurred(Throwable e) {
			rsCache.removeTempResults(cacheID, requestId);
			actual.exceptionOccurred(e);
		}

	}
	
	public ConnectorWorkItemFactory(ConnectorManager manager,
			ResultSetCache rsCache, boolean synchWorkers) {
		this.manager = manager;
		this.rsCache = rsCache;
		this.synchWorkers = synchWorkers;
	}
	
	public ConnectorWorkItem createWorkItem(AtomicRequestMessage message, ResultsReceiver<AtomicResultsMessage> receiver) {
    	if (this.rsCache != null && message.useResultSetCache()) {
        	final CacheID cacheID = createCacheID(message);

        	if (cacheID != null) {
	        	if (rsCache.hasResults(cacheID)) {
	        		return new CachedResultsConnectorWorkItem(message, manager,
							receiver, cacheID);
	        	}
        		receiver = new CachedResultsReceiver(receiver, cacheID, message.getAtomicRequestID());
        	}
    	}
    	
    	if (synchWorkers) {
    		return new SynchConnectorWorkItem(message, manager, receiver);
    	} 
    	return new AsynchConnectorWorkItem(message, manager, receiver);
	}
	
	private CacheID createCacheID(AtomicRequestMessage message) {
		Command command = message.getCommand();
		if (!command.areResultsCachable()) {
			return null;
		}
		String scope = rsCache.getCacheScope();
		String scopeId = null;
		if(ResultSetCache.RS_CACHE_SCOPE_VDB.equalsIgnoreCase(scope)){
			scopeId = message.getWorkContext().getVdbName() + DELIMITER + message.getWorkContext().getVdbVersion();
		}else{
			scopeId = message.getWorkContext().getConnectionID();
		}
		return new CacheID(scopeId, command.toString());
	}

}
