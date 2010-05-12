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

package org.teiid.translator.salesforce;

import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ConnectorCapabilities;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProvider;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.salesforce.execution.DeleteExecutionImpl;
import org.teiid.translator.salesforce.execution.InsertExecutionImpl;
import org.teiid.translator.salesforce.execution.ProcedureExecutionParentImpl;
import org.teiid.translator.salesforce.execution.QueryExecutionImpl;
import org.teiid.translator.salesforce.execution.UpdateExecutionImpl;


public class SalesForceExecutionFactory extends org.teiid.translator.BasicExecutionFactory implements MetadataProvider {

	private String connectorStateClass;
	private boolean auditModelFields = false;	

	
	public String getConnectorStateClass() {
		return this.connectorStateClass;
	}
	public void setConnectorStateClass(String connectorStateClass) {
		this.connectorStateClass = connectorStateClass;
	}
	
	@TranslatorProperty(name="ModelAuditFields", display="Audit Model Fields", advanced=true, defaultValue="false")
	public boolean isModelAuditFields() {
		return this.auditModelFields;
	}
	
	public void setModelAuditFields(boolean modelAuditFields) {
		this.auditModelFields = modelAuditFields;
	}

	@Override
	public void start() throws ConnectorException {
		super.start();
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Started"); //$NON-NLS-1$
	}


	@Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return SalesforceCapabilities.class;
    }	
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
			throws ConnectorException {
		return new QueryExecutionImpl(command, (SalesforceConnection)connectionFactory, metadata, executionContext);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory) throws ConnectorException {
		UpdateExecution result = null;
		if(command instanceof org.teiid.language.Delete) {
			result = new DeleteExecutionImpl(command, (SalesforceConnection)connectionFactory, metadata, executionContext);
		} else if (command instanceof org.teiid.language.Insert) {
			result = new InsertExecutionImpl(command, (SalesforceConnection)connectionFactory, metadata, executionContext);
		} else if (command instanceof org.teiid.language.Update) {
			result = new UpdateExecutionImpl(command, (SalesforceConnection)connectionFactory, metadata, executionContext);
		}
		return result;

	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
			throws ConnectorException {
		return new ProcedureExecutionParentImpl(command, (SalesforceConnection)connectionFactory, metadata, executionContext);
	}
	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory, Object connectionFactory) throws ConnectorException {
		MetadataProcessor processor = new MetadataProcessor((SalesforceConnection)connectionFactory,metadataFactory, this);
		processor.processMetadata();
	}	
}
