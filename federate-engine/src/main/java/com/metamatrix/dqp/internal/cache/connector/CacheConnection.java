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

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.api.SynchQueryCommandExecution;
import com.metamatrix.data.api.SynchQueryExecution;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.dqp.internal.cache.ResultSetCache;

public class CacheConnection implements Connection{
	private Connection actualConn;
	private ResultSetCache cache;
	
	public CacheConnection(Connection actualConn, ResultSetCache cache){
		this.actualConn = actualConn;
		this.cache = cache;
	}

	public ConnectorCapabilities getCapabilities() {
		return actualConn.getCapabilities();
	}

	public Execution createExecution(int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		Execution execution = actualConn.createExecution(executionMode, executionContext, metadata);
		if(executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY){
			return new CacheSynchQueryExecution((SynchQueryExecution)execution, cache, executionContext);
		} else if(executionMode == ConnectorCapabilities.EXECUTION_MODE.PROCEDURE){
			return new CacheProcedureExecution((ProcedureExecution)execution, cache, executionContext);
		} else if (executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERYCOMMAND) {
			return new CacheSynchQueryCommandExecution((SynchQueryCommandExecution)execution, cache, executionContext);
		}
		return execution;
	}

	public ConnectorMetadata getMetadata() {
		return actualConn.getMetadata();
	}
	
	public void release() {
		actualConn.release();
	}
}
