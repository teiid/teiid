package org.teiid.resource.adapter.salesforce.execution;

import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.salesforce.SalesforceConnection;
import org.teiid.translator.ExecutionContext;


public interface ProcedureExecutionParent {

	public Call getCommand();

	public ExecutionContext getExecutionContext();

	public RuntimeMetadata getMetadata();

	public SalesforceConnection getConnection();
}
