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
import java.util.Collection;

import javax.security.auth.Subject;

import org.teiid.CommandContext;
import org.teiid.adminapi.Session;
import org.teiid.jdbc.TeiidSQLWarning;
import org.teiid.metadata.RuntimeMetadata;



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
     * shortcut for {@link #getCommandContext()}.getRequestId()
     * @return command identifier
     */
    String getRequestId();
    
    /**
     * @deprecated see {@link #getRequestId()}
     * @return
     */
    String getRequestID();
    
    /**
     * This specifies the node id for the atomic request in the relational plan of query. 
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
     * shortcut for {@link #getCommandContext()}.getVdbName()
     * @return VDB name, never null
     */
    String getVdbName();
    
    /**
     * @deprecated see {@link #getVdbName()}
     * @return
     */
    String getVirtualDatabaseName();

    /**
     * Get the version of the VDB this query is being run against.
     * shortcut for {@link #getCommandContext()}.getVdbVersion()
     * @return VDB version
     */
    int getVdbVersion();
    
    /**
     * @deprecated see {@link #getVdbVersion()}
     * @return
     */
    int getVirtualDatabaseVersion();

    /**
     * Get the user for the user running this query.
     * @return User, never null
     */
    Subject getSubject();

    /**
     * Get the command payload
     * shortcut for {@link #getCommandContext()}.getCommandPayload()
     * @return the payload or null if one was not set by the client
     */
    Serializable getCommandPayload();
    
    /**
     * @deprecated see {@link #getCommandPayload()
     * @return
     */
    Serializable getExecutionPayload();
    
    /**
     * Get the collection of general hints as a space concatinated string.
     * @return the general hint or null if none was specified
     */
    String getGeneralHint();
   
    /**
     * Get the collection of hints designated for this source as a space concatinated string.
     * @return the source hint or null if none was specified
     */
    String getSourceHint();
    
    /**
     * Get the general hints.
     * @return the general hint or null if none was specified
     */
    Collection<String> getGeneralHints();
   
    /**
     * Get the hints designated for this source.
     * @return the source hint or null if none was specified
     */
    Collection<String> getSourceHints();
    
    /**
     * Get the identifier for the connection through which 
     * the command is being executed. This represents the original user
     * connection to the Teiid system
     * shortcut for {@link #getCommandContext()}.getConnectionId()
     * @return Connection identifier
     */
    String getConnectionId();
    
    /**
     * @deprecated see {@link #getConnectionId()}
     * @return
     */
    String getConnectionID();
    
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
     * Add an exception as a warning to this Execution.  The exception will be wrapped by a {@link TeiidSQLWarning} for the client. 
     * The warnings can be consumed through the {@link Statement#getWarnings()} method.  
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
     * shortcut for {@link #getCommandContext()}.getSession()
     * @return
     */
    Session getSession();
    
    /**
     * Signal the engine that data is available and processing should be
     * resumed.
     */
    void dataAvailable();
    
    /**
     * Get the CommandContext
     * @return
     */
    CommandContext getCommandContext();

    /**
     * Get the {@link CacheDirective}
     * @return
     */
    CacheDirective getCacheDirective();
    
    /**
     * 
     * @return the {@link RuntimeMetadata}
     */
    RuntimeMetadata getRuntimeMetadata();
}
