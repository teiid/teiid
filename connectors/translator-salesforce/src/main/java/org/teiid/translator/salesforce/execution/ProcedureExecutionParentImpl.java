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
	public void close() throws TranslatorException {
		execution.close();
	}

	@Override
	public void execute() throws TranslatorException {
		if(getCommand().getProcedureName().endsWith("getUpdated")) {
			execution = new GetUpdatedExecutionImpl(this);
			execution.execute(this);
		}
		else if(getCommand().getProcedureName().endsWith("getDeleted")) {
			execution = new GetDeletedExecutionImpl(this);
			execution.execute(this);
		}
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
