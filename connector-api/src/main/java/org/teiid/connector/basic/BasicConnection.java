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

package org.teiid.connector.basic;

import javax.resource.spi.LocalTransaction;
import javax.transaction.xa.XAResource;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.Command;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

/**
 * Provides a default implementation of a {@link PoolAwareConnection} for a Connector
 * that supports global capabilities.  Extensions of this class should implement
 * {@link #createProcedureExecution(Call, ExecutionContext, RuntimeMetadata)}
 * {@link #createResultSetExecution(Call, ExecutionContext, RuntimeMetadata)}
 * {@link #createUpdateExecution(Call, ExecutionContext, RuntimeMetadata)}
 * as necessary.
 */
public abstract class BasicConnection implements Connection {


	@Override
	public Execution createExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		if (command instanceof QueryExpression) {
			return createResultSetExecution((QueryExpression)command, executionContext, metadata);
		}
		if (command instanceof Call) {
			return createProcedureExecution((Call)command, executionContext, metadata);
		}
		return createUpdateExecution(command, executionContext, metadata);
	}

	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}

	public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}

	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}
		
	@Override
	public boolean isAlive() {
		return true;
	}
	
	@Override
	public ConnectorCapabilities getCapabilities() {
		return null;
	}
		
	@Override
	public LocalTransaction getLocalTransaction() {
		return null;
	}
	
	@Override
	public XAResource getXAResource() throws ConnectorException {
		return null;
	}
}
