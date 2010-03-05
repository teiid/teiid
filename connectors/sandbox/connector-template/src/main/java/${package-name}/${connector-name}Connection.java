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
 * Connection to the resource.
 */
public class ${connector-name}Connection extends BasicConnection {

    private ${connector-name}ManagedConnectionFactory config;

    public ${connector-name}Connection(${connector-name}ManagedConnectionFactory env) {
        this.config = env;
    }
    
    @Override
    public Execution createExecution(ICommand command, ExecutionContext executionContext, RuntimeMetadata metadata) throws ConnectorException {
        return new ${connector-name}Execution(command, config, metadata);
    }
    
    @Override
    public void close() {
    	
    }
}
