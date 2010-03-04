package org.teiid.connector.basic;

import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;

public abstract class BasicConnector implements Connector {

	protected ConnectorEnvironment config;
	protected ConnectorCapabilities capabilities;
	
	@Override
	public void initialize(ConnectorEnvironment config) throws ConnectorException {
		this.config = config;
	}	
	
	@Override
	public ConnectorEnvironment getConnectorEnvironment(){
		return this.config;
	}
	
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return BasicConnectorCapabilities.class;
    }	
    
    @Override
    public ConnectorCapabilities getCapabilities() throws ConnectorException {
    	if (capabilities == null) {
			// create Capabilities
    		capabilities = BasicManagedConnectionFactory.getInstance(ConnectorCapabilities.class, this.config.getCapabilitiesClass(), null, getDefaultCapabilities());
    	}
    	return capabilities;
	}    
    
}
