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

package com.metamatrix.connector.basic;

import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.Execution;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.api.UpdateExecution;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.pool.PoolAwareConnection;

public abstract class BasicConnection implements PoolAwareConnection {

	@Override
	public Execution createExecution(ICommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		if (command instanceof IQueryCommand) {
			return createResultSetExecution((IQueryCommand)command, executionContext, metadata);
		}
		if (command instanceof IProcedure) {
			return createProcedureExecution((IProcedure)command, executionContext, metadata);
		}
		return createUpdateExecution(command, executionContext, metadata);
	}

	public ResultSetExecution createResultSetExecution(IQueryCommand command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}

	public ProcedureExecution createProcedureExecution(IProcedure command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}

	public UpdateExecution createUpdateExecution(ICommand command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}
	
	@Override
	public boolean isAlive() {
		return true;
	}
	
	@Override
	public void closeCalled() {
		
	}
	
	@Override
	public ConnectorCapabilities getCapabilities() {
		return null;
	}
	
}
