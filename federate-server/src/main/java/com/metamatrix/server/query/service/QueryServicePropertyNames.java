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

package com.metamatrix.server.query.service;

public class QueryServicePropertyNames {

    public static final String PROCESS_POOL_MAX_THREADS = "ProcessPoolMaxThreads"; //$NON-NLS-1$
    public static final String PROCESS_POOL_THREAD_TTL = "ProcessPoolThreadTTL"; //$NON-NLS-1$

	public static final String MIN_FETCH_SIZE = "MinFetchSize"; //$NON-NLS-1$
	public static final String MAX_FETCH_SIZE = "MaxFetchSize"; //$NON-NLS-1$
    
	public static final String MAX_CODE_TABLE_RECORDS = "MaxCodeTableRecords"; //$NON-NLS-1$
	public static final String MAX_CODE_TABLES = "MaxCodeTables"; //$NON-NLS-1$
	
    public static final String PROCESSOR_TIMESLICE = "ProcessorTimeslice"; //$NON-NLS-1$
    
    public static final String UDF_SOURCE = "UDFSource"; //$NON-NLS-1$
    
//    public static final String SOCKET_WORKER_POOL_MAX_THREADS = "SocketWorkerPoolMaxThreads"; //$NON-NLS-1$
//    public static final String SOCKET_WORKER_POOL_THREAD_TTL = "SocketWorkerPoolThreadTTL"; //$NON-NLS-1$
//    
//    public static final String SOCKET_PORT = "SocketPort"; //$NON-NLS-1$

    public static final String USE_RESULTSET_CACHE = "ResultSetCacheEnabled"; //$NON-NLS-1$
    public static final String MAX_RESULTSET_CACHE_SIZE = "ResultSetCacheMaxSize"; //$NON-NLS-1$
    public static final String MAX_RESULTSET_CACHE_AGE = "ResultSetCacheMaxAge"; //$NON-NLS-1$
    public static final String RESULTSET_CACHE_SCOPE = "ResultSetCacheScope"; //$NON-NLS-1$

	public static final String MAX_PLAN_CACHE_SIZE = "MaxPlanCacheSize"; //$NON-NLS-1$
}
