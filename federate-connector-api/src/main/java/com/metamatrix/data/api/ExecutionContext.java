/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.data.api;

import java.io.Serializable;

/**
 * Represents information provided to the Connector about the context of this 
 * execution.  In particular, provides information about which MetaMatrix command
 * is causing this command to be executed.  Also provides access to the
 * {@link SecurityContext}.
 * 
 * @see SecurityContext
 */
public interface ExecutionContext {

    /**
     * Get the identifier for the current connector running the command
     * @return Connector identifier; never null
     */
    String getConnectorIdentifier();
    
    /**
     * Get the identifier for the MetaMatrix command being executed.  This can be
     * correlated back to identifiers exposed in other parts of the MetaMatrix system.
     * @return MetaMatrix command identifier
     */
    String getRequestIdentifier();
    
    /**
     * This specifies the node id for the atomic request in the relational plan of query. 
     * when combined with the request identifier, uniquely identifies a command sent to a connector.    
     */
    String getPartIdentifier();
    
    /**
     * Execution count defines an id; where every access to the connector from  
     * the MetaMatrix server in a given command execution boundary is uniquely defined;
     * Like for example in the case of "batched execution" of commands, each execution of 
     * command gets new identifier; usally this this always zero in single command executions.  
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
    String getVirtualDatabaseVersion();

    /**
     * Get the user name for the user running this query.
     * @return User name, never null
     */
    String getUser();

    /**
     * Get the trusted payload passed when the user connected.  MetaMatrix has no
     * knowledge about what the payload contains - it is merely passed through
     * the system.  It is most often used to pass security information such as
     * credentials.  It is available in the connector, as is the Execution Payload,
     * for connector developers to utilize.
     * @return Trusted payload if one exists, otherwise null
     */
    Serializable getTrustedPayload();

    /**
     * Get the trusted payload passed when the user statement was executed.
     * MetaMatrix has no knowledge about what the payload contains - it is merely
     * passed through the system.  It is most often used to pass security
     * information such as credentials.
     * 
     * <p>The execution payload differs from the Trusted Payload in that it
     * is set on the Statement and so may not be constant over the Connection lifecycle
     * and may be changed upon each statement execution.  The Execution Payload is
     * <em>not</em> authenticated or validated by the MetaMatrix system.</p>
     * 
     * <p>Given that the Execution Payload is not authenticated by the MetaMatrix
     * system, connector writers are responsible for ensuring its validity.  This
     * can possibly be accomplished by comparing it against the Trusted Payload.</p>
     * 
     * @return Trusted execution payload if one exists, otherwise null
     * @since 4.2
     */
    Serializable getExecutionPayload();
    
    /**
     * Get the identifier for the connection through which 
     * the command is being executed. This represents the original JDBC user
     * connection to the MetaMatrix system
     * @return Connection identifier
     */
    String getConnectionIdentifier();
    
    /**
     * Whether to use ResultSet cache if it is enabled.
     * @return True if use ResultSet cache; false otherwise.
     */
    boolean useResultSetCache();
    
    /**
     * When the execution is turned on with "alive=true", the execution object will not
     * be implicitly closed at the end of the last batch, but will only be closed at end
     * of user query. This is useful in keeping the connection open/checked out until all
     * the underlying LOB (clob/blob/xml) kind of data streamed out. The "close" on 
     * execution object will be only caleed when processing has been ended.
     * @param alive
     */
    void keepExecutionAlive(boolean alive);    
}
