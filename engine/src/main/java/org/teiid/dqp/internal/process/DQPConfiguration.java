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
package org.teiid.dqp.internal.process;

import org.teiid.cache.CacheConfiguration;
import org.teiid.client.RequestMessage;


public class DQPConfiguration{
	
    //Constants
    static final int DEFAULT_FETCH_SIZE = RequestMessage.DEFAULT_FETCH_SIZE * 10;
    static final int DEFAULT_PROCESSOR_TIMESLICE = 2000;
    static final int DEFAULT_MAX_RESULTSET_CACHE_ENTRIES = 1024;
    static final int DEFAULT_QUERY_THRESHOLD = 600;
    static final String PROCESS_PLAN_QUEUE_NAME = "QueryProcessorQueue"; //$NON-NLS-1$
    public static final int DEFAULT_MAX_PROCESS_WORKERS = 64;
	public static final int DEFAULT_MAX_SOURCE_ROWS = -1;
	public static final int DEFAULT_MAX_ACTIVE_PLANS = 20;
	public static final int DEFAULT_USER_REQUEST_SOURCE_CONCURRENCY = 0;
    
	private int maxThreads = DEFAULT_MAX_PROCESS_WORKERS;
	private int timeSliceInMilli = DEFAULT_PROCESSOR_TIMESLICE;
	private int maxRowsFetchSize = DEFAULT_FETCH_SIZE;
	private int lobChunkSizeInKB = 100;
	private int queryThresholdInSecs = DEFAULT_QUERY_THRESHOLD;
	private boolean exceptionOnMaxSourceRows = true;
	private int maxSourceRows = -1;
	private int maxActivePlans = DEFAULT_MAX_ACTIVE_PLANS;
	private CacheConfiguration resultsetCacheConfig;
	private CacheConfiguration preparedPlanCacheConfig = new CacheConfiguration();
	private int maxODBCLobSizeAllowed = 5*1024*1024; // 5 MB
    private int userRequestSourceConcurrency = DEFAULT_USER_REQUEST_SOURCE_CONCURRENCY;
    private boolean detectingChangeEvents = true;
    
    private transient AuthorizationValidator authorizationValidator;

	public int getMaxActivePlans() {
		return maxActivePlans;
	}
	
	public void setMaxActivePlans(int maxActivePlans) {
		this.maxActivePlans = maxActivePlans;
	}
	
	public int getUserRequestSourceConcurrency() {
		return userRequestSourceConcurrency;
	}
	
	public void setUserRequestSourceConcurrency(int userRequestSourceConcurrency) {
		this.userRequestSourceConcurrency = userRequestSourceConcurrency;
	}
	
	public int getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	public int getTimeSliceInMilli() {
		return timeSliceInMilli;
	}

	public void setTimeSliceInMilli(int timeSliceInMilli) {
		this.timeSliceInMilli = timeSliceInMilli;
	}
	
	public int getMaxRowsFetchSize() {
		return maxRowsFetchSize;
	}

	public void setMaxRowsFetchSize(int maxRowsFetchSize) {
		this.maxRowsFetchSize = maxRowsFetchSize;
	}

	public int getLobChunkSizeInKB() {
		return this.lobChunkSizeInKB;
	}

	public void setLobChunkSizeInKB(int lobChunkSizeInKB) {
		this.lobChunkSizeInKB = lobChunkSizeInKB;
	}

	public CacheConfiguration getResultsetCacheConfig() {
		return this.resultsetCacheConfig;
	}	
	
	public void setResultsetCacheConfig(CacheConfiguration config) {
		this.resultsetCacheConfig = config;
	}
	
	public boolean isResultSetCacheEnabled() {
		return this.resultsetCacheConfig != null;
	}
	
    /**
     * Determine whether role checking is enabled on the server.
     * @return <code>true</code> if server-side role checking is enabled.
     */
    public boolean getUseDataRoles() {
        return this.authorizationValidator != null && this.authorizationValidator.isEnabled();
    }

	public void setUseDataRoles(boolean useEntitlements) {
		if (this.authorizationValidator != null) {
			this.authorizationValidator.setEnabled(useEntitlements);
		}
	}

	public int getQueryThresholdInSecs() {
		return queryThresholdInSecs;
	}

	public void setQueryThresholdInSecs(int queryThresholdInSecs) {
		this.queryThresholdInSecs = queryThresholdInSecs;
	}
		
	/**
	 * Throw exception if there are more rows in the result set than specified in the MaxSourceRows setting.
	 * @return
	 */
	public boolean isExceptionOnMaxSourceRows() {
		return exceptionOnMaxSourceRows;
	}
	
	public void setExceptionOnMaxSourceRows(boolean exceptionOnMaxSourceRows) {
		this.exceptionOnMaxSourceRows = exceptionOnMaxSourceRows;
	}

	/**
	 * Maximum source set rows to fetch
	 * @return
	 */
	public int getMaxSourceRows() {
		return maxSourceRows;
	}

	public void setMaxSourceRows(int maxSourceRows) {
		this.maxSourceRows = maxSourceRows;
	}
	
	public int getMaxODBCLobSizeAllowed() {
		return this.maxODBCLobSizeAllowed;
	}
	
	public void setMaxODBCLobSizeAllowed(int lobSize) {
		this.maxODBCLobSizeAllowed = lobSize;
	}
	
	public AuthorizationValidator getAuthorizationValidator() {
		return authorizationValidator;
	}
	
	public void setAuthorizationValidator(
			AuthorizationValidator authorizationValidator) {
		this.authorizationValidator = authorizationValidator;
	}
	
	public CacheConfiguration getPreparedPlanCacheConfig() {
		return preparedPlanCacheConfig;
	}
	
	public void setPreparedPlanCacheConfig(
			CacheConfiguration preparedPlanCacheConfig) {
		this.preparedPlanCacheConfig = preparedPlanCacheConfig;
	}

	public boolean isDetectingChangeEvents() {
		return detectingChangeEvents;
	}
	
	public void setDetectingChangeEvents(boolean detectingChangeEvents) {
		this.detectingChangeEvents = detectingChangeEvents;
	}

}
