/*
 * ${license}
 */
package ${package-name};

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

/**
 * Connection to the resource. You must define ${connector-name}Connection interface, that 
 * extends the "javax.resource.cci.Connection"
 */
public class ${connector-name}ConnectionImpl extends BasicConnection implements ${connector-name}Connection {

    private ${connector-name}ManagedConnectionFactory config;

    public ${connector-name}ConnectionImpl(${connector-name}ManagedConnectionFactory env) {
        this.config = env;
        // todo: connect to your source here
    }
    
    @Override
    public void close() {
    	
    }
}
