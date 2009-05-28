package com.metamatrix.connector.xml.soap;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;
import com.metamatrix.connector.xml.http.HTTPExecution;

public class SOAPExecution extends HTTPExecution {

	public SOAPExecution(IQuery query, XMLConnectionImpl conn,
			RuntimeMetadata metadata, ExecutionContext exeContext,
			ConnectorEnvironment connectorEnv) {
		super(query, conn, metadata, exeContext, connectorEnv);
	}

	@Override
	public ResultProducer getStreamProducer() throws ConnectorException {
		return new SOAPExecutor((SOAPConnectorState) connection.getState(),
				this, exeInfo);
	}
}
