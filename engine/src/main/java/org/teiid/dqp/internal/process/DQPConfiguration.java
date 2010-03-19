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

import org.jboss.managed.api.annotation.ManagementProperty;

import com.metamatrix.dqp.message.RequestMessage;

public class DQPConfiguration{
	
    //Constants
    static final int DEFAULT_MAX_CODE_TABLE_RECORDS = 10000;
    static final int DEFAULT_MAX_CODE_TABLES = 200;
    static final int DEFAULT_MAX_CODE_RECORDS = 200000;
    static final int DEFAULT_FETCH_SIZE = RequestMessage.DEFAULT_FETCH_SIZE * 10;
    static final int DEFAULT_PROCESSOR_TIMESLICE = 2000;
    static final int DEFAULT_MAX_RESULTSET_CACHE_ENTRIES = 1024;
    static final String PROCESS_PLAN_QUEUE_NAME = "QueryProcessorQueue"; //$NON-NLS-1$
    public static final int DEFAULT_MAX_PROCESS_WORKERS = 16;
	
    
	private String processName = "localhost"; //$NON-NLS-1$
	private int maxThreads = DEFAULT_MAX_PROCESS_WORKERS;
	private int timeSliceInMilli = DEFAULT_PROCESSOR_TIMESLICE;
	private boolean processDebugAllowed;
	private int maxRowsFetchSize = DEFAULT_FETCH_SIZE;
	private int lobChunkSizeInKB = 100;
	private int preparedPlanCacheMaxCount = SessionAwareCache.DEFAULT_MAX_SIZE_TOTAL;
	private int codeTablesMaxCount = DEFAULT_MAX_CODE_TABLES;
	private int codeTablesMaxRowsPerTable = DEFAULT_MAX_CODE_TABLE_RECORDS;
	private int codeTablesMaxRows = DEFAULT_MAX_CODE_RECORDS;
	private boolean resultSetCacheEnabled = true;
	private int maxResultSetCacheEntries = DQPConfiguration.DEFAULT_MAX_RESULTSET_CACHE_ENTRIES;
	private boolean useEntitlements = false;
	
	@ManagementProperty (description="Name of the process that uniquely identifies this process")
	public String getProcessName() {
		return processName;
	}

	public void setProcessName(String processName) {
		this.processName = processName;
	}

	@ManagementProperty(description="Process pool maximum thread count. (default 16) Increase this value if your load includes a large number of XQueries or if the system's available processors is larger than 8")
	public int getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	@ManagementProperty(description="Query processor time slice, in milliseconds. (default 2000)")
	public int getTimeSliceInMilli() {
		return timeSliceInMilli;
	}

	public void setTimeSliceInMilli(int timeSliceInMilli) {
		this.timeSliceInMilli = timeSliceInMilli;
	}
	
	@ManagementProperty(description="Process debug allowed")
	public boolean isProcessDebugAllowed() {
		return processDebugAllowed;
	}

	public void setProcessDebugAllowed(boolean processDebugAllowed) {
		this.processDebugAllowed = processDebugAllowed;
	}

	@ManagementProperty(description="Maximum allowed fetch size, set via JDBC. User requested value ignored above this value. (default 20480)")
	public int getMaxRowsFetchSize() {
		return maxRowsFetchSize;
	}

	public void setMaxRowsFetchSize(int maxRowsFetchSize) {
		this.maxRowsFetchSize = maxRowsFetchSize;
	}

	@ManagementProperty(description="The max lob chunk size in KB transferred each time when processing blobs, clobs(100KB default)")
	public int getLobChunkSizeInKB() {
		return this.lobChunkSizeInKB;
	}

	public void setLobChunkSizeInKB(int lobChunkSizeInKB) {
		this.lobChunkSizeInKB = lobChunkSizeInKB;
	}

	@ManagementProperty(description="The maximum number of query plans that are cached. Note: this is a memory based cache. (default 250)")
	public int getPreparedPlanCacheMaxCount() {
		return this.preparedPlanCacheMaxCount;
	}

	public void setPreparedPlanCacheMaxCount(int preparedPlanCacheMaxCount) {
		this.preparedPlanCacheMaxCount = preparedPlanCacheMaxCount;
	}

	@ManagementProperty(description="Maximum number of cached lookup tables. Note: this is a memory based cache. (default 200)")
	public int getCodeTablesMaxCount() {
		return this.codeTablesMaxCount;
	}

	public void setCodeTablesMaxCount(int codeTablesMaxCount) {
		this.codeTablesMaxCount = codeTablesMaxCount;
	}

	@ManagementProperty(description="Maximum number of records in a single lookup table (default 10000)")
	public int getCodeTablesMaxRowsPerTable() {
		return codeTablesMaxRowsPerTable;
	}

	public void setCodeTablesMaxRowsPerTable(int codeTablesMaxRowsPerTable) {
		this.codeTablesMaxRowsPerTable = codeTablesMaxRowsPerTable;
	}

	@ManagementProperty(description="Maximum number of records in all lookup tables (default 200000)")
	public int getCodeTablesMaxRows() {
		return this.codeTablesMaxRows;
	}

	public void setCodeTablesMaxRows(int codeTablesMaxRows) {
		this.codeTablesMaxRows = codeTablesMaxRows;
	}

	@ManagementProperty(description="The maximum number of result set cache entries. 0 indicates no limit. (default 1024)")
	public int getResultSetCacheMaxEntries() {
		return this.maxResultSetCacheEntries;
	}
	
	public void setResultSetCacheMaxEntries(int value) {
		this.maxResultSetCacheEntries = value;
	}

	@ManagementProperty(description="Denotes whether or not result set caching is enabled. (default true)")
	public boolean isResultSetCacheEnabled() {
		return this.resultSetCacheEnabled;
	}
	
	public void setResultSetCacheEnabled(boolean value) {
		this.resultSetCacheEnabled = value;
	}		
	
    /**
     * Determine whether entitlements checking is enabled on the server.
     * @return <code>true</code> if server-side entitlements checking is enabled.
     */
    @ManagementProperty(description="Turn on checking the entitlements on resources based on the roles defined in VDB")
    public boolean useEntitlements() {
        return useEntitlements;
    }

	public void setUseEntitlements(Boolean useEntitlements) {
		this.useEntitlements = useEntitlements.booleanValue();
	}	
}
