package org.teiid.resource.adapter.salesforce.execution;

import org.teiid.connector.language.Call;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.resource.adapter.salesforce.SalesforceConnection;
import org.teiid.resource.cci.ExecutionContext;


public interface ProcedureExecutionParent {

	public Call getCommand();

	public ExecutionContext getExecutionContext();

	public RuntimeMetadata getMetadata();

	public SalesforceConnection getConnection();
}
