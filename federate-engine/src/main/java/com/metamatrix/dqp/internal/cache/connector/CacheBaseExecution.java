/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.cache.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.BatchedExecution;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.dqp.internal.cache.CacheID;
import com.metamatrix.dqp.internal.cache.CacheResults;
import com.metamatrix.dqp.internal.cache.ResultSetCache;
import com.metamatrix.dqp.util.LogConstants;

public abstract class CacheBaseExecution {
	private final static char DELIMITER = '.';
	
	private Execution actualExec;
	private ResultSetCache cache;
	private CacheID cacheID; 
	private int maxBatchSize;
	private int firstRow = 1;
	private boolean hasResults;
	private Object requestID;
	private String cacheScopeID;
	private boolean useResultSetCache;
	private List outParameter;
	private int outParamsterCnt;
	
	public CacheBaseExecution(Execution actualExec, ResultSetCache cache, ExecutionContext executionContext){
		this.actualExec = actualExec;
		this.cache = cache;
		useResultSetCache = executionContext.useResultSetCache();
		if(useResultSetCache){
			String scope = cache.getCacheScope();
			if(ResultSetCache.RS_CACHE_SCOPE_VDB.equalsIgnoreCase(scope)){
				cacheScopeID = executionContext.getVirtualDatabaseName() + DELIMITER + executionContext.getVirtualDatabaseVersion();
			}else{
				cacheScopeID = executionContext.getConnectionIdentifier();
			}
			requestID = executionContext.getRequestIdentifier() + DELIMITER + executionContext.getPartIdentifier();
		}
	}

	public void close() throws ConnectorException {
		removeTempCacheResults();
		actualExec.close();
	}

	public void cancel() throws ConnectorException {
		removeTempCacheResults();
		actualExec.cancel();
	}

	public Batch nextBatch() throws ConnectorException {
		if(hasResults){
			//already in cache
			CacheResults results = cache.getResults(cacheID, new int[]{firstRow, firstRow + maxBatchSize - 1});
			BasicBatch batch = new BasicBatch();
			List[] rows = results.getResults();
			for(int i=0; i< rows.length; i++){
				batch.addRow(rows[i]);
			}
			if(results.isFinal()){
				batch.setLast();
			}

            LogManager.logTrace(LogConstants.CTX_DQP, 
                                new Object[] { "CacheSynchQueryExecution - returnning batch from cache, startRow =",  //$NON-NLS-1$
                                               new Integer(firstRow),
                                               ", endRow =", //$NON-NLS-1$
                                               new Integer(firstRow + rows.length -1)});
	        
			firstRow += rows.length;
			return batch;
		}
		
		Batch batch = null;
		try{
			batch = ((BatchedExecution)actualExec).nextBatch();
		}catch(ConnectorException e){
			removeTempCacheResults();
			throw e;
		}
		if(useResultSetCache){
			boolean isFinal = batch.isLast() && (outParameter == null || outParameter.isEmpty());
			CacheResults cr = new CacheResults(batch.getResults(), firstRow, isFinal);
			cache.setResults(cacheID, cr, requestID);
			firstRow += batch.getRowCount();
		}
		return batch;
	}

	protected boolean areResultsInCache(String command){
		if(useResultSetCache){
			firstRow = 1;
			cacheID = new CacheID(cacheScopeID, command);
			if(cache.hasResults(cacheID)){
				hasResults = true;
				return true;
			}
		}
		return false;
	}
	
	protected void setMaxBatchSize(int maxBatchSize) {
		this.maxBatchSize = maxBatchSize;
	}
	
	private void removeTempCacheResults() {
		if(useResultSetCache){
			cache.removeTempResults(cacheID);
		}
	}
	
	protected boolean hasResults() {
		return hasResults;
	}

	protected void setParameters(List outParameter) {
		this.outParameter = outParameter;
	}
	
	protected void setOutputValue(String parameter, Object value) {
		if(useResultSetCache){
			outParamsterCnt++;
			Map paramValue = new HashMap();
			paramValue.put(parameter, value);
			CacheResults results = new CacheResults(paramValue, outParameter.size() == outParamsterCnt);
			cache.setResults(cacheID, results, requestID);
		}
	}
	
	protected Object getOutputValue(String parameter) {
		CacheResults results = cache.getResults(cacheID, new int[]{1, 1});
		return results.getParamValues().get(parameter);
	}
}
