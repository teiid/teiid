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
package com.metamatrix.connector.salesforce.connection;

import java.util.List;

import javax.security.auth.Subject;
import javax.xml.datatype.XMLGregorianCalendar;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MetadataProvider;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.Command;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.connector.salesforce.MetadataProcessor;
import com.metamatrix.connector.salesforce.SalesForceManagedConnectionFactory;
import com.metamatrix.connector.salesforce.connection.impl.ConnectionImpl;
import com.metamatrix.connector.salesforce.execution.DataPayload;
import com.metamatrix.connector.salesforce.execution.DeleteExecutionImpl;
import com.metamatrix.connector.salesforce.execution.DeletedResult;
import com.metamatrix.connector.salesforce.execution.InsertExecutionImpl;
import com.metamatrix.connector.salesforce.execution.ProcedureExecutionParentImpl;
import com.metamatrix.connector.salesforce.execution.QueryExecutionImpl;
import com.metamatrix.connector.salesforce.execution.UpdateExecutionImpl;
import com.metamatrix.connector.salesforce.execution.UpdatedResult;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

public class SalesforceConnection extends BasicConnection implements MetadataProvider {

	private SalesForceManagedConnectionFactory connectorEnv;
	private ConnectionImpl connection;
	
	public SalesforceConnection(Subject subject,  SalesForceManagedConnectionFactory env) {
		throw new MetaMatrixRuntimeException("not supported yet..");
	}
	
	public SalesforceConnection(SalesForceManagedConnectionFactory env) throws ConnectorException {
		try {
			connectorEnv = env;
			
			long pingInterval = env.getSourceConnectionTestInterval();
	
			//600000 - 10 minutes
			int timeout = env.getSourceConnectionTimeout();
			
			connection = new ConnectionImpl(env.getUsername(), env.getPassword(), env.getURL(), pingInterval, env.getLogger(), timeout);
		} catch(Throwable t) {
			env.getLogger().logError("SalesforceConnection() ErrorMessage: " + t.getMessage());
			if(t instanceof ConnectorException) {
				// don't wrap it again
				throw (ConnectorException) t;
			} 
			throw new ConnectorException(t);
		}
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new QueryExecutionImpl(command, this, metadata, executionContext, connectorEnv);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		UpdateExecution result = null;
		if(command instanceof org.teiid.connector.language.Delete) {
			result = new DeleteExecutionImpl(command, this, metadata, executionContext, connectorEnv);
		} else if (command instanceof org.teiid.connector.language.Insert) {
			result = new InsertExecutionImpl(command, this, metadata, executionContext, connectorEnv);
		} else if (command instanceof org.teiid.connector.language.Update) {
			result = new UpdateExecutionImpl(command, this, metadata, executionContext, connectorEnv);
		}
		return result;

	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new ProcedureExecutionParentImpl(command, this, metadata, executionContext, connectorEnv);
	}

	@Override
	public void close() {
	}

	public QueryResult query(String queryString, int maxBatchSize, Boolean queryAll) throws ConnectorException {
		if(maxBatchSize > 2000) {
			maxBatchSize = 2000;
			connectorEnv.getLogger().logInfo(
					Messages.getString("SalesforceQueryExecutionImpl.reduced.batch.size"));
		}
		return connection.query(queryString, maxBatchSize, queryAll);
	}

	public QueryResult queryMore(String queryLocator, int batchSize) throws ConnectorException {
		return connection.queryMore(queryLocator, batchSize);
	}
	
	@Override
	public boolean isAlive() {
		return connection.isAlive();
	}
	
	public int delete(String[] ids) throws ConnectorException {
		return connection.delete(ids);
	}

	public int create(DataPayload data) throws ConnectorException {
		return connection.create(data);
	}

	public int update(List<DataPayload> updateDataList) throws ConnectorException {
		return connection.update(updateDataList);
	}

	public UpdatedResult getUpdated(String objectName, XMLGregorianCalendar startCalendar,
			XMLGregorianCalendar endCalendar) throws ConnectorException {
		return connection.getUpdated(objectName, startCalendar, endCalendar);
	}

	public DeletedResult getDeleted(String objectName, XMLGregorianCalendar startCalendar,
			XMLGregorianCalendar endCalendar) throws ConnectorException {
		return connection.getDeleted(objectName, startCalendar, endCalendar);
	}
	
	public QueryResult retrieve(String fieldList, String sObjectType, List<String> ids) throws ConnectorException {
		List<SObject> objects = connection.retrieve(fieldList, sObjectType, ids);
		QueryResult result = new QueryResult();
		result.getRecords().addAll(objects);
		result.setSize(objects.size());
		return result;
	}
	
	public DescribeGlobalResult getObjects() throws ConnectorException {
		return connection.getObjects();
	}
	
	public DescribeSObjectResult getObjectMetaData(String objectName) throws ConnectorException {
		return connection.getObjectMetaData(objectName);
	}
	
	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory)
			throws ConnectorException {
		MetadataProcessor processor = new MetadataProcessor(this,metadataFactory, connectorEnv);
		processor.processMetadata();
	}	
}
