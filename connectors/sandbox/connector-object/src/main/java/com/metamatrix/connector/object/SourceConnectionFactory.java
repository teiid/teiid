package com.metamatrix.connector.object;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.identity.ConnectorIdentity;
import com.metamatrix.connector.identity.SingleIdentity;

public interface SourceConnectionFactory {
	
    /**
     * Set the environment that this factory is being run in - typically used 
     * to obtain properties when creating connections and to log messages as 
     * necessary.
     * @param env The environment passed to the connector by the connector manager
     */
    void initialize(ConnectorEnvironment env) throws ConnectorException;

    /**
     * Create the source-specific connection based on an identity.
     * @param id The identity object
     * @return The source-specific connection
     * @throws ConnectorException If an error occurs while creating the connection
     */
    Connection createConnection(ConnectorIdentity id) throws ConnectorException;

    /**
     * Create an identity object based on a security context.  This method determines
     * how different security contexts are treated within the connection pool.  For 
     * example, using a {@link SingleIdentity} specifies that ALL contexts are treated
     * equally and thus use the same pool.  A {@link CredentialMapIdentity} specifies that contexts
     * are differentiated based on user name, thus creating a per-user pool of connections.
     * Implementors of this class may use a different implementation of the 
     * {@link ConnectorIdentity} interface to similarly affect pooling. 
     * @param context The context provided by the Connector Manager
     * @return The associated connector identity
     * @throws ConnectorException If an error occurs while creating the identity
     */
    ConnectorIdentity createIdentity(ExecutionContext context) throws ConnectorException;
    
}
