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

import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SynchQueryCommandExecution;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQueryCommand;
import com.metamatrix.dqp.internal.cache.ResultSetCache;

public class CacheSynchQueryCommandExecution extends CacheBaseExecution implements SynchQueryCommandExecution{
	private SynchQueryCommandExecution actualExec;

	public CacheSynchQueryCommandExecution(SynchQueryCommandExecution actualExec, ResultSetCache cache, ExecutionContext executionContext){
		super(actualExec, cache, executionContext);
		this.actualExec = actualExec;
	}

	public void execute(IQueryCommand query, int maxBatchSize) throws ConnectorException {
		super.setMaxBatchSize(maxBatchSize);
		if(!super.areResultsInCache(query.toString())){
			actualExec.execute(query, maxBatchSize);
		}
	}
}
