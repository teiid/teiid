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

package com.metamatrix.server.query.service;

import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;

public class QueryServicePropertyNames {

    public static final String PROCESS_POOL_MAX_THREADS = DQPEmbeddedProperties.PROCESS_POOL_MAX_THREADS;

    public static final String MAX_FETCH_SIZE = DQPEmbeddedProperties.MAX_FETCH_SIZE;
    
	public static final String MAX_CODE_TABLE_RECORDS = DQPEmbeddedProperties.MAX_CODE_TABLE_RECORDS;

	public static final String MAX_CODE_TABLES = DQPEmbeddedProperties.MAX_CODE_TABLES;
	
    public static final String PROCESSOR_TIMESLICE = DQPEmbeddedProperties.PROCESS_TIMESLICE;
    
    public static final String UDF_SOURCE = "UDFSource"; //$NON-NLS-1$

    public static final String USE_RESULTSET_CACHE = DQPEmbeddedProperties.USE_RESULTSET_CACHE;

    public static final String MAX_RESULTSET_CACHE_SIZE = DQPEmbeddedProperties.MAX_RESULTSET_CACHE_SIZE;

    public static final String MAX_RESULTSET_CACHE_AGE = DQPEmbeddedProperties.MAX_RESULTSET_CACHE_AGE;
 
    public static final String RESULTSET_CACHE_SCOPE = DQPEmbeddedProperties.RESULTSET_CACHE_SCOPE;

	public static final String MAX_PLAN_CACHE_SIZE = DQPEmbeddedProperties.MAX_PLAN_CACHE_SIZE;
}
