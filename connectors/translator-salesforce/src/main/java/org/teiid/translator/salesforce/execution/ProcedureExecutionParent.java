package org.teiid.translator.salesforce.execution;

import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesforceConnection;


public interface ProcedureExecutionParent {

	public Call getCommand();

	public ExecutionContext getExecutionContext();

	public RuntimeMetadata getMetadata();

	public SalesforceConnection getConnection();
}
