package com.metamatrix.connector.object;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorIdentity;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.SingleIdentity;

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
