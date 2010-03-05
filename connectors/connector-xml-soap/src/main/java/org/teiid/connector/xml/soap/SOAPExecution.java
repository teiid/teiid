package org.teiid.connector.xml.soap;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.xml.SOAPConnectorState;
import org.teiid.connector.xml.base.XMLConnectionImpl;
import org.teiid.connector.xml.http.HTTPExecution;

import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.base.XMLBaseManagedConnectionFactory;

public class SOAPExecution extends HTTPExecution {

	public SOAPExecution(Select query, XMLConnectionImpl conn, RuntimeMetadata metadata, ExecutionContext exeContext, XMLBaseManagedConnectionFactory connectorEnv) {
		super(query, conn, metadata, exeContext, connectorEnv);
	}

	@Override
	public ResultProducer getStreamProducer() throws ConnectorException {
		return new SOAPExecutor((SOAPConnectorState) connection.getState(), getExecutionInfo(), analyzer, (SOAPManagedConnectionFactory)this.config, this.context);
	}
}
