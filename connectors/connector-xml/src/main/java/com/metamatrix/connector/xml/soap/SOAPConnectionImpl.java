package com.metamatrix.connector.xml.soap;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;

public class SOAPConnectionImpl extends XMLConnectionImpl {

	public SOAPConnectionImpl(CachingConnector connector,
			ExecutionContext context, ConnectorEnvironment connectorEnv)
			throws ConnectorException {
		super(connector, context, connectorEnv);
	}

	///////////////////////////////////////////////////////////////
	//Connection API Implementation
	@Override
	public ResultSetExecution createResultSetExecution(IQueryCommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new SOAPExecution((IQuery)command, this, metadata, executionContext, getConnectorEnv());
	}

	//End Connection API Implementation
	///////////////////////////////////////////////////////////////
}
