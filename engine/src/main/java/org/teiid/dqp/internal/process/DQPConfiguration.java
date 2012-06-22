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

import org.teiid.client.RequestMessage;


public class DQPConfiguration{
	
    //Constants
    static final int DEFAULT_FETCH_SIZE = RequestMessage.DEFAULT_FETCH_SIZE * 10;
    static final int DEFAULT_PROCESSOR_TIMESLICE = 2000;
    static final int DEFAULT_MAX_RESULTSET_CACHE_ENTRIES = 1024;
    static final int DEFAULT_QUERY_THRESHOLD = 600000;
    static final String PROCESS_PLAN_QUEUE_NAME = "QueryProcessorQueue"; //$NON-NLS-1$
    public static final int DEFAULT_MAX_PROCESS_WORKERS = 64;
	public static final int DEFAULT_MAX_SOURCE_ROWS = -1;
	public static final int DEFAULT_MAX_ACTIVE_PLANS = 20;
	public static final int DEFAULT_USER_REQUEST_SOURCE_CONCURRENCY = 0;
    
	private int maxThreads = DEFAULT_MAX_PROCESS_WORKERS;
	private int timeSliceInMilli = DEFAULT_PROCESSOR_TIMESLICE;
	private int maxRowsFetchSize = DEFAULT_FETCH_SIZE;
	private int lobChunkSizeInKB = 100;
	private long queryThresholdInMilli = DEFAULT_QUERY_THRESHOLD;
	private boolean exceptionOnMaxSourceRows = true;
	private int maxSourceRows = -1;
	private int maxActivePlans = DEFAULT_MAX_ACTIVE_PLANS;
	
    private int userRequestSourceConcurrency = DEFAULT_USER_REQUEST_SOURCE_CONCURRENCY;
    private boolean detectingChangeEvents = true;
    private long queryTimeout;
    
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
	
	public int getQueryThresholdInSecs() {
		return (int)queryThresholdInMilli/1000;
	}
	
	public long getQueryThresholdInMilli() {
		return queryThresholdInMilli;
	}
	
	public void setQueryThresholdInMilli(long queryThreshold) {
		this.queryThresholdInMilli = queryThreshold;
	}

	public void setQueryThresholdInSecs(int queryThresholdInSecs) {
		this.queryThresholdInMilli = queryThresholdInSecs * 1000;
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
		
	public AuthorizationValidator getAuthorizationValidator() {
		return authorizationValidator;
	}
	
	public void setAuthorizationValidator(
			AuthorizationValidator authorizationValidator) {
		this.authorizationValidator = authorizationValidator;
	}
	
	public boolean isDetectingChangeEvents() {
		return detectingChangeEvents;
	}
	
	public void setDetectingChangeEvents(boolean detectingChangeEvents) {
		this.detectingChangeEvents = detectingChangeEvents;
	}
	
	public void setQueryTimeout(long queryTimeout) {
		this.queryTimeout = queryTimeout;
	}
	
	public long getQueryTimeout() {
		return queryTimeout;
	}

	public TeiidExecutor getTeiidExecutor() {
		return new ThreadReuseExecutor(DQPConfiguration.PROCESS_PLAN_QUEUE_NAME, getMaxThreads());
	}

}
