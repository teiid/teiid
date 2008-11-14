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

package com.metamatrix.admin.api.core;

import com.metamatrix.admin.api.exception.AdminException;


/**
 * This interface defines the methods to interact with the MetaMatrix system
 * during runtime.
 *
 * <p>As a <i>core</i> interface,
 * this administration is common to both the MetaMatrix server and MM Query.</p>
 *
 * @since 4.3
 */
public interface CoreRuntimeStateAdmin {

    /**
     * Start Connector Binding
     *
     * @param connectorBindingIdentifier  identifier for {@link com.metamatrix.admin.api.objects.ConnectorBinding}
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
     * @param connectorBindingIdentifier  identifier for {@link com.metamatrix.admin.api.objects.ConnectorBinding}
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
     * @param cacheIdentifier  Cache name identifier {@link com.metamatrix.admin.api.objects.Cache}.
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void clearCache(String cacheIdentifier) throws AdminException;

    /**
     * Terminate the Session
     *
     * @param identifier  Session Idenitfier {@link com.metamatrix.admin.api.objects.Session}.
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void terminateSession(String identifier) throws AdminException;

    /**
     * Cancel Request
     *
     * @param identifier  The request identifer defined by {@link com.metamatrix.admin.api.objects.Request}
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void cancelRequest(String identifier) throws AdminException;

    /**
     * Cancel Source Request
     *
     * @param identifier  The request identifer defined by {@link com.metamatrix.admin.api.objects.Request}
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    void cancelSourceRequest(String identifier) throws AdminException;

    /**
     * Change the status of a Deployed VDB
     *
     * @param name  Name of the Virtial Database
     * @param version  Version of the Virtial Database
     * @param status  Active, InActive, Delete
     * @throws AdminException  if there's a system error.
     * @since 4.3
     */
    public void changeVDBStatus(String name, String version, int status)
        throws AdminException;

}
