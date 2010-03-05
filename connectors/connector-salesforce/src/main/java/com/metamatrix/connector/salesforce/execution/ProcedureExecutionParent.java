package com.metamatrix.connector.salesforce.execution;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.Call;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.salesforce.connection.SalesforceConnection;

public interface ProcedureExecutionParent {

	public Call getCommand();

	public ExecutionContext getExecutionContext();

	public RuntimeMetadata getMetadata();

	public SalesforceConnection getConnection();

	public ConnectorEnvironment getConectorEnvironment();

}
