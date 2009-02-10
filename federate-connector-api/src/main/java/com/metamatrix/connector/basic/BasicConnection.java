package com.metamatrix.connector.basic;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Execution;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.api.UpdateExecution;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

public abstract class BasicConnection implements Connection {

	@Override
	public Execution createExecution(ICommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		if (command instanceof IQueryCommand) {
			return createResultSetExecution((IQueryCommand)command, executionContext, metadata);
		}
		if (command instanceof IProcedure) {
			return createProcedureExecution((IProcedure)command, executionContext, metadata);
		}
		return createUpdateExecution(command, executionContext, metadata);
	}

	public ResultSetExecution createResultSetExecution(IQueryCommand command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}

	public ProcedureExecution createProcedureExecution(IProcedure command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}

	public UpdateExecution createUpdateExecution(ICommand command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
		throw new ConnectorException("Unsupported Execution");
	}
	
}
