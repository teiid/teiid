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

package org.teiid.translator.salesforce.execution;

import java.util.List;

import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.salesforce.SalesforceConnection;


public class ProcedureExecutionParentImpl implements ProcedureExecution, ProcedureExecutionParent {

	private Call command;
	private ExecutionContext executionContext;
	private RuntimeMetadata metadata;
	private SalesforceProcedureExecution execution;
	private SalesforceConnection connection;
	
	public ProcedureExecutionParentImpl(Call command,
			SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext executionContext) {
		this.setCommand(command);
		this.setConnection(connection);
		this.setMetadata(metadata);
		this.setExecutionContext(executionContext);
	}

	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return execution.getOutputParameterValues();
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		return execution.next();
	}

	@Override
	public void cancel() throws TranslatorException {
		execution.cancel();
	}

	@Override
	public void close() {
		execution.close();
	}

	@Override
	public void execute() throws TranslatorException {
		String name = getCommand().getMetadataObject().getNameInSource();
		if (name == null) {
			name = getCommand().getProcedureName();
		}
		if("GetUpdated".equalsIgnoreCase(name)) { //$NON-NLS-1$
			execution = new GetUpdatedExecutionImpl(this);
		} else if("GetDeleted".equalsIgnoreCase(name)) { //$NON-NLS-1$
			execution = new GetDeletedExecutionImpl(this);
		} else {
			throw new AssertionError("Unknown procedure " + getCommand().getProcedureName() + " with name in source " + getCommand().getMetadataObject().getNameInSource()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		execution.execute(this);
	}

	public void setCommand(Call command) {
		this.command = command;
	}

	public Call getCommand() {
		return command;
	}
	
	private void setConnection(SalesforceConnection connection) {
		this.connection = connection;
	}

	public SalesforceConnection getConnection() {
		return connection;
	}

	private void setExecutionContext(ExecutionContext executionContext) {
		this.executionContext = executionContext;
	}

	public ExecutionContext getExecutionContext() {
		return executionContext;
	}

	private void setMetadata(RuntimeMetadata metadata) {
		this.metadata = metadata;
	}

	public RuntimeMetadata getMetadata() {
		return metadata;
	}
}
