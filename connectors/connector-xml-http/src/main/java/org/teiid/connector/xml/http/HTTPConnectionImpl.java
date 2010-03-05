package org.teiid.connector.xml.http;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.xml.StatefulConnector;
import org.teiid.connector.xml.base.XMLConnectionImpl;

import com.metamatrix.connector.xml.base.XMLBaseManagedConnectionFactory;

public class HTTPConnectionImpl extends XMLConnectionImpl {

	public HTTPConnectionImpl(StatefulConnector connector, XMLBaseManagedConnectionFactory connectorEnv) throws ConnectorException {
		super(connector,connectorEnv);
	}
	
	///////////////////////////////////////////////////////////////
	//Connection API Implementation
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new HTTPExecution((Select)command, this, metadata, executionContext, getConnectorEnv());
	}

	//End Connection API Implementation
	///////////////////////////////////////////////////////////////
}
