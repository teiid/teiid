package com.metamatrix.connector.salesforce.execution;

import java.util.List;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.salesforce.connection.SalesforceConnection;

public class ProcedureExecutionParentImpl implements ProcedureExecution, ProcedureExecutionParent {

	private IProcedure command;
	private ExecutionContext executionContext;
	private RuntimeMetadata metadata;
	private SalesforceProcedureExecution execution;
	private SalesforceConnection connection;
	private ConnectorEnvironment connectorEnv;
	
	public ProcedureExecutionParentImpl(IProcedure command,
			SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext executionContext, ConnectorEnvironment connectorEnv) {
		this.setCommand(command);
		this.setConnection(connection);
		this.setMetadata(metadata);
		this.setExecutionContext(executionContext);
		this.setConnectorEnvironment(connectorEnv);
	}

	@Override
	public List<?> getOutputParameterValues() throws ConnectorException {
		return execution.getOutputParameterValues();
	}

	@Override
	public List<?> next() throws ConnectorException, DataNotAvailableException {
		return execution.next();
	}

	@Override
	public void cancel() throws ConnectorException {
		execution.cancel();
	}

	@Override
	public void close() throws ConnectorException {
		execution.close();
	}

	@Override
	public void execute() throws ConnectorException {
		if(getCommand().getProcedureName().endsWith("getUpdated")) {
			execution = new GetUpdatedExecutionImpl(this);
			execution.execute(this);
		}
		else if(getCommand().getProcedureName().endsWith("getDeleted")) {
			execution = new GetDeletedExecutionImpl(this);
			execution.execute(this);
		}
	}

	public void setCommand(IProcedure command) {
		this.command = command;
	}

	public IProcedure getCommand() {
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
	
	private void setConnectorEnvironment(ConnectorEnvironment connectorEnv) {
		this.connectorEnv = connectorEnv;
	}

	public ConnectorEnvironment getConectorEnvironment() {
		return connectorEnv;
	}

}
