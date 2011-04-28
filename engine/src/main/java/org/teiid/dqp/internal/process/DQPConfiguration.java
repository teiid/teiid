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
import org.teiid.cache.CacheConfiguration;
import org.teiid.client.RequestMessage;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.metadata.MetadataRepository;


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
	private boolean useDataRoles = true;
	private boolean allowCreateTemporaryTablesByDefault = true;
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
	private boolean allowFunctionCallsByDefault;

	@ManagementProperty(description="Max active plans (default 20).  Increase this value, and max threads, on highly concurrent systems - but ensure that the underlying pools can handle the increased load without timeouts.")
	public int getMaxActivePlans() {
		return maxActivePlans;
	}
	
	public void setMaxActivePlans(int maxActivePlans) {
		this.maxActivePlans = maxActivePlans;
	}
	
	@ManagementProperty(description="Max source query concurrency per user request (default 0).  " +
			"0 indicates use the default calculated value based on max active plans and max threads - approximately 2*(max threads)/(max active plans). " +
			"1 forces serial execution in the processing thread, just as is done for a transactional request. " +
			"Any number greater than 1 limits the maximum number of concurrently executing source requests accordingly.")
	public int getUserRequestSourceConcurrency() {
		return userRequestSourceConcurrency;
	}
	
	public void setUserRequestSourceConcurrency(int userRequestSourceConcurrency) {
		this.userRequestSourceConcurrency = userRequestSourceConcurrency;
	}
	
	@ManagementProperty(description="Process pool maximum thread count. (default 64)")
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
	
	@ManagementProperty(description="Maximum allowed fetch size, set via JDBC. User requested value ignored above this value. (default 20480)")
	public int getMaxRowsFetchSize() {
		return maxRowsFetchSize;
	}

	public void setMaxRowsFetchSize(int maxRowsFetchSize) {
		this.maxRowsFetchSize = maxRowsFetchSize;
	}

	@ManagementProperty(description="The max lob chunk size in KB transferred to the client for xml, blobs, clobs (default 100KB)")
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
	
	@ManagementProperty(description="Denotes whether or not result set caching is enabled. (default true)")
	public boolean isResultSetCacheEnabled() {
		return this.resultsetCacheConfig != null && this.resultsetCacheConfig.isEnabled();
	}
	
    /**
     * Determine whether role checking is enabled on the server.
     * @return <code>true</code> if server-side role checking is enabled.
     */
    @ManagementProperty(description="Turn on role checking based upon the data roles defined in VDBs. (default true)")
    public boolean getUseDataRoles() {
        return useDataRoles;
    }

	public void setUseDataRoles(boolean useEntitlements) {
		this.useDataRoles = useEntitlements;
	}

	/**
     * Whether temporary table usage is enabled by default.
     * @return <code>true</code> if temporary table usage is enabled by default.
     */
    @ManagementProperty(description="Sets whether temporary table usage is allowed by default with data roles enabled. If false, the user must have a role that grants creates temporary table rights to use temporary tables. (default true)")
    public boolean isAllowCreateTemporaryTablesByDefault() {
		return allowCreateTemporaryTablesByDefault;
	}
	
	public void setAllowCreateTemporaryTablesByDefault(
			boolean allowCreateTemporaryTablesByDefault) {
		this.allowCreateTemporaryTablesByDefault = allowCreateTemporaryTablesByDefault;
	}
	
	/**
     * Whether functions are callable by default
     * @return <code>true</code> if function usage is enabled by default.
     */
    @ManagementProperty(description="Sets whether functions may be called by default with data roles enabled. If false, a specific permission must exist to call the function. (default true)")
    public boolean isAllowFunctionCallsByDefault() {
		return allowFunctionCallsByDefault;
	}
	
    public void setAllowFunctionCallsByDefault(boolean allowFunctionCallsDefault) {
		this.allowFunctionCallsByDefault = allowFunctionCallsDefault;
	}
	
	@ManagementProperty(description="Long running query threshold, after which a alert can be generated by tooling if configured")
	public int getQueryThresholdInSecs() {
		return queryThresholdInSecs;
	}

	public void setQueryThresholdInSecs(int queryThresholdInSecs) {
		this.queryThresholdInSecs = queryThresholdInSecs;
	}
	
	@ManagementProperty(description="Teiid runtime version", readOnly=true)
	public String getRuntimeVersion() {
		return ApplicationInfo.getInstance().getBuildNumber();
	}
	
	/**
	 * Throw exception if there are more rows in the result set than specified in the MaxSourceRows setting.
	 * @return
	 */
	@ManagementProperty(description="Indicates if an exception should be thrown if the specified value for Maximum Source Rows is exceeded; only up to the maximum rows will be consumed.")
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
	@ManagementProperty(description="Maximum rows allowed from a source query. -1 indicates no limit. (default -1)")
	public int getMaxSourceRows() {
		return maxSourceRows;
	}

	public void setMaxSourceRows(int maxSourceRows) {
		this.maxSourceRows = maxSourceRows;
	}
	
	@ManagementProperty(description="Maximum Lob Size allowed over ODBC (default 5MB)")
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
	
	public MetadataRepository getMetadataRepository() {
		return null;
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
	
	@ManagementProperty(description="Set to true for the engine to detect local change events. Should be disabled if using external change data capture tools. (default true)")
	public void setDetectingChangeEvents(boolean detectingChangeEvents) {
		this.detectingChangeEvents = detectingChangeEvents;
	}

}
