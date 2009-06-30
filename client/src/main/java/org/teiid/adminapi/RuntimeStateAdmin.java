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

package org.teiid.adminapi;

import javax.transaction.xa.Xid;

import com.metamatrix.admin.RolesAllowed;


/**
 * This interface defines the methods to interact with the MetaMatrix system
 * during runtime.
 *
 * <p>As a <i>core</i> interface,
 * this administration is common to both the MetaMatrix server and MM Query.</p>
 *
 * @since 4.3
 */
@RolesAllowed(value=AdminRoles.RoleName.ADMIN_PRODUCT)
public interface RuntimeStateAdmin {

    /**
     * Start Connector Binding
     *
     * @param connectorBindingIdentifier  identifier for {@link org.teiid.adminapi.ConnectorBinding}
     * <ul>
     *      <li> <code>"*"</code> - for all connector bindings in the system
     *      <li> <code>"name*"</code> - for all connector bindings that begin with given name
     *      <li><code>"name"</code> - for single connector binding by the given name
     * </ul>
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void startConnectorBinding(String connectorBindingIdentifier) throws AdminException;

    /**
     * Stop Connector Binding
     *
     * @param connectorBindingIdentifier  identifier for {@link org.teiid.adminapi.ConnectorBinding}
     * <ul>
     *      <li> <code>"*"</code> - for all connector bindings in the system
     *      <li> <code>"name*"</code> - for all connector bindings that begin with given name
     *      <li><code>"name"</code> - for single connector binding by the given name
     * </ul>
     * @param stopNow  If true, stop the process forcefully. If false, wait until any pending work is done.
     * @throws AdminException - if there's a system error.
     * @since 4.3
     */
    void stopConnectorBinding(String connectorBindingIdentifier,
                              boolean stopNow) throws AdminException;

    /**
     * Clear the cache or caches specified by the cacheIdentifier.
     * @param cacheIdentifier  Cache name identifier {@link org.teiid.adminapi.Cache}.
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    @RolesAllowed(value=AdminRoles.RoleName.ADMIN_SYSTEM)
    void clearCache(String cacheIdentifier) throws AdminException;

    /**
     * Terminate the Session
     *
     * @param identifier  Session Identifier {@link org.teiid.adminapi.Session}.
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void terminateSession(String identifier) throws AdminException;

    /**
     * Cancel Request
     *
     * @param identifier  The request identifier defined by {@link org.teiid.adminapi.Request}
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void cancelRequest(String identifier) throws AdminException;

    /**
     * Cancel Source Request
     *
     * @param identifier  The request identifier defined by {@link org.teiid.adminapi.Request}
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void cancelSourceRequest(String identifier) throws AdminException;

    /**
     * Change the status of a Deployed VDB
     *
     * @param name  Name of the Virtual Database
     * @param version  Version of the Virtual Database
     * @param status  Active, InActive, Delete
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    public void changeVDBStatus(String name, String version, int status)
        throws AdminException;
    
    /**
     * Mark the given global transaction as rollback only.
     * @param transactionId
     * @throws AdminException
     */
    void terminateTransaction(Xid transactionId) throws AdminException;
    
    /**
     * Mark the given transaction as rollback only.
     * @param identifier
     * 		The exact identifier of the transaction.  Wild card is not supported.
     * @param the session the transaction is associated with.
     * @throws AdminException
     */
    void terminateTransaction(String transactionId, String sessionId) throws AdminException;
    
    /**
     * Set the log listener to install into MM Query.  This log listener will receive all log messages
     * written by the MM Query at it's current log level and log contexts.
     *
     * @param listener The listener component
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    void setLogListener(EmbeddedLogger listener) throws AdminException;    
    
    /**
     * Stop the MM Query.  If millisToWait is >0, then close to incoming queries, wait the time period
     * for work to stop, then stop the MM Query.  Otherwise, stop immediately, aborting all running queries.
     * @param millisToWait Milliseconds to wait (if >0) or <=0 for no wait before stopping
     * @throws AdminException
     * @since 4.3
     */
    void shutdown(int millisToWait) throws AdminException;

    /**
     * Restart System
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    void restart() throws AdminException;    
}
