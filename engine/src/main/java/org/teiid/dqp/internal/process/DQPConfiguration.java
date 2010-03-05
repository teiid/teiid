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
    static final int DEFAULT_MAX_PROCESS_WORKERS = 15;
	
    
	private String processName = "localhost";
	private int maxThreads = DEFAULT_MAX_PROCESS_WORKERS;
	private int timeSliceInMilli = DEFAULT_PROCESSOR_TIMESLICE;
	private boolean processDebugAllowed;
	private int maxRowsFetchSize = DEFAULT_FETCH_SIZE;
	private int lobChunkSizeInKB = 100;
	private int preparedPlanCacheMaxCount = SessionAwareCache.DEFAULT_MAX_SIZE_TOTAL;
	private int codeTablesMaxCount = DEFAULT_MAX_CODE_TABLES;
	private int codeTablesMaxRowsPerTable = DEFAULT_MAX_CODE_TABLE_RECORDS;
	private int codeTablesMaxRows = DEFAULT_MAX_CODE_RECORDS;
	private String bindAddress = "localhost";
	private int portNumber = 31000;
	private boolean resultSetCacheEnabled = false;
	private int maxResultSetCacheEntries = DQPConfiguration.DEFAULT_MAX_RESULTSET_CACHE_ENTRIES;
	
	public String getProcessName() {
		return processName;
	}

	public void setProcessName(String processName) {
		this.processName = processName;
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
	
	public boolean isProcessDebugAllowed() {
		return processDebugAllowed;
	}

	public void setProcessDebugAllowed(boolean processDebugAllowed) {
		this.processDebugAllowed = processDebugAllowed;
	}

	public int getMaxRowsFetchSize() {
		return maxRowsFetchSize;
	}

	public void setMaxRowsFetchSize(int maxRowsFetchSize) {
		this.maxRowsFetchSize = maxRowsFetchSize;
	}

	public int getLobChunkSizeInKB() {
		return lobChunkSizeInKB;
	}

	public void setLobChunkSizeInKB(int lobChunkSizeInKB) {
		this.lobChunkSizeInKB = lobChunkSizeInKB;
	}

	public int getPreparedPlanCacheMaxCount() {
		return preparedPlanCacheMaxCount;
	}

	public void setPreparedPlanCacheMaxCount(int preparedPlanCacheMaxCount) {
		this.preparedPlanCacheMaxCount = preparedPlanCacheMaxCount;
	}

	public int getCodeTablesMaxCount() {
		return codeTablesMaxCount;
	}

	public void setCodeTablesMaxCount(int codeTablesMaxCount) {
		this.codeTablesMaxCount = codeTablesMaxCount;
	}

	public int getCodeTablesMaxRowsPerTable() {
		return codeTablesMaxRowsPerTable;
	}

	public void setCodeTablesMaxRowsPerTable(int codeTablesMaxRowsPerTable) {
		this.codeTablesMaxRowsPerTable = codeTablesMaxRowsPerTable;
	}

	public int getCodeTablesMaxRows() {
		return codeTablesMaxRows;
	}

	public void setCodeTablesMaxRows(int codeTablesMaxRows) {
		this.codeTablesMaxRows = codeTablesMaxRows;
	}

	public String getBindAddress() {
		return bindAddress;
	}

	public void setBindAddress(String bindAddress) {
		this.bindAddress = bindAddress;
	}

	public int getPortNumber() {
		return portNumber;
	}

	public void setPortNumber(int portNumber) {
		this.portNumber = portNumber;
	}

	public int getResultSetCacheMaxEntries() {
		return this.maxResultSetCacheEntries;
	}
	
	public void setResultSetCacheMaxEntries(int value) {
		this.maxResultSetCacheEntries = value;
	}

	public boolean isResultSetCacheEnabled() {
		return this.resultSetCacheEnabled;
	}
	
	public void setResultSetCacheEnabled(boolean value) {
		this.resultSetCacheEnabled = value;
	}		
}
