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

package com.metamatrix.common.application;

import java.util.Properties;

/**
 * This interface represents a source of DQP configuration information.
 */
public interface DQPConfigSource {

	public static final String MAX_FETCH_SIZE = "MaxFetchSize"; //$NON-NLS-1$
	public static final String MAX_CODE_TABLE_RECORDS = "MaxCodeTableRecords"; //$NON-NLS-1$
	public static final String MAX_CODE_TABLES = "MaxCodeTables"; //$NON-NLS-1$
	public static final String MAX_RESULTSET_CACHE_SIZE = "ResultSetCacheMaxSize"; //$NON-NLS-1$
	public static final String MAX_RESULTSET_CACHE_AGE = "ResultSetCacheMaxAge"; //$NON-NLS-1$
	public static final String MAX_PLAN_CACHE_SIZE = "MaxPlanCacheSize"; //$NON-NLS-1$
	public static final String PROCESSOR_TIMESLICE = "ProcessorTimeslice"; //$NON-NLS-1$
	public static final String USE_RESULTSET_CACHE = "ResultSetCacheEnabled"; //$NON-NLS-1$
	public static final String RESULTSET_CACHE_SCOPE = "ResultSetCacheScope"; //$NON-NLS-1$
	public static final String STREAMING_BATCH_SIZE     = "metamatrix.server.streamingBatchSize"; //$NON-NLS-1$

	public static final String PROCESS_POOL_MAX_THREADS = "ProcessPoolMaxThreads"; //$NON-NLS-1$
	public static final String PROCESSOR_DEBUG_ALLOWED = "ProcessorDebugAllowed"; //$NON-NLS-1$

	/**
     * Get the DQP properties, as described in {@link DQPProperties}.
     * @return Set of properties to configure DQP
     */
    public Properties getProperties();
    
    public ApplicationService getServiceInstance(Class<? extends ApplicationService> type);
}
