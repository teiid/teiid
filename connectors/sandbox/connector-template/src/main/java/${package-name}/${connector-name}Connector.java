/*
 * ${license}
 */
package ${package-name};

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.basic.BasicConnector;


public class ${connector-name}Connector extends BasicConnector {

    private ${connector-name}ManagedConnectionFactory config;

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);

		this.config = (${connector-name}ManagedConnectionFactory)env;
		
		// TODO: do connector initialization here..
	}
    

    public Connection getConnection() throws ConnectorException {
    	// TODO: create the connector connection here.
        return new ${connector-name}Connection(this.config);
    }

    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	// TODO: if you not already defined the Capabilities class in "ra.xml" define it here.
    	return ${connector-name}Capabilities.class;
    }
}
