/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator;

import java.io.Serializable;
import java.sql.Statement;

import javax.security.auth.Subject;

import org.teiid.adminapi.Session;



/**
 * The security context provides information about the user context in which
 * this query is being run.
 * 
 * As of 4.2, the SecurityContext is a sub-interface of ExecutionContext such
 * that both interfaces contain all of the methods from the prior independent
 * interfaces. Thus, these interfaces can now be used interchangeably.
 * 
 */
public interface ExecutionContext {

	/**
     * Get the identifier for the current connector running the command
     * @return Connector identifier; never null
     */
    String getConnectorIdentifier();
    
    /**
     * Get the identifier for the command being executed.  This can be
     * correlated back to identifiers exposed in other parts of the system.
     * @return command identifier
     */
    String getRequestIdentifier();
    
    /**
     * This specifies the node id for the atomic request in the relational plan of query. 
     * when combined with the request identifier, uniquely identifies a command sent to a connector.    
     */
    String getPartIdentifier();
    
    /**
     * Execution count defines an id; where every access to the connector from  
     * the server in a given command execution boundary is uniquely defined;
     * Like for example in the case of "batched execution" of commands, each execution of 
     * command gets new identifier.  
     */
    String getExecutionCountIdentifier();

    /**
     * Get the name of the VDB this query is being run against.
     * @return VDB name, never null
     */
    String getVirtualDatabaseName();

    /**
     * Get the version of the VDB this query is being run against.
     * @return VDB version, never null
     */
    int getVirtualDatabaseVersion();

    /**
     * Get the user for the user running this query.
     * @return User, never null
     */
    Subject getSubject();
    
    /**
     * Get the trusted payload passed when the user statement was executed.
     * Teiid has no knowledge about what the payload contains - it is merely
     * passed through the system.  It is most often used to pass security
     * information such as credentials.
     * 
     * <p>Given that the Execution Payload is not authenticated by the Teiid
     * system, connector writers are responsible for ensuring its validity. </p>
     * 
     * @return Trusted execution payload if one exists, otherwise null
     * @since 4.2
     */
    Serializable getExecutionPayload();
        
    /**
     * Get the identifier for the connection through which 
     * the command is being executed. This represents the original JDBC user
     * connection to the Teiid system
     * @return Connection identifier
     */
    String getConnectionIdentifier();
    
    /**
     * When the execution is turned on with "alive=true", the execution object will not
     * be implicitly closed at the end of the last batch.  It will only be closed at end
     * of the user query. 
     * <p>
     * The engine will already detect situations when the connection should stay open for 
     * LOB (clob/blob/xml) streaming.
     * <p>
     * Keeping the execution alive unnecessarily may cause issues with connection usage
     * as the connection instance may not be usable by other queries.
     * 
     * @param alive
     */
    void keepExecutionAlive(boolean alive);
    
    /**
     * Return the current connector batch size.  This may be used as a hint to the underlying source query.
     * @return the Connector batch size.
     */
    int getBatchSize();
    
    /**
     * Add an exception as a warning to this Execution.  If the exception is not an instance of a SQLWarning
     * it will be wrapped by a SQLWarning for the client.  The warnings can be consumed through the 
     * {@link Statement#getWarnings()} method.  
     * @param ex
     */
    void addWarning(Exception ex);
    
    /**
     * Flag indicates that the operation needs to be executed in a XA transaction.
     * @return
     */
    boolean isTransactional();
    
    /**
     * Get the current session.
     * @return
     */
    Session getSession();
    
    /**
     * Signal the engine that data is available and processing should be
     * resumed.
     */
    void dataAvailable();
}
