package org.teiid.connector.xml.file;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.basic.BasicConnector;

public class FileConnector extends BasicConnector {

    private FileManagedConnectionFactory config;
    boolean initialized = false;

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);

		this.config = (FileManagedConnectionFactory)env;
		
		ConnectorLogger logger = config.getLogger();
		logger.logInfo("Loaded for SoapConnector"); //$NON-NLS-1$
		initialized = true;
	}
    

    public Connection getConnection() throws ConnectorException {
    	if (!initialized) {
    		throw new ConnectorException(XMLSourcePlugin.Util.getString("Connector_not_initialized"));
    	}
        return new FileConnection(this.config);
    }

    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return FileSourceCapabilities.class;
    }
}