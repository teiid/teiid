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

import java.util.Collection;

import com.metamatrix.admin.RolesAllowed;


/**
 * Used to access the monitorable components of the Teiid system.
 * 
 * <p>See the particular admin object in question for an example of
 * allowed identifier patterns.</p>
 *
 * @since 4.3
 */
@RolesAllowed(value=AdminRoles.RoleName.ADMIN_READONLY)
public interface MonitoringAdmin {

    /**
     * Get the Connector Types that correspond to the specified identifier pattern.
     *
     * @param connectorTypeIdentifier the unique identifier for for a {@link ConnectorType}
     * <ul>
     *      <li> <code>"*"</code> - for all connector types in the system
     *      <li> <code>"name*"</code> - for all the connector types that begin with given name
     *      <li> <code>"name"</code> - for the single connector type identified by name
     * </ul>
     * @return Collection of {@link ConnectorType}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<ConnectorType> getConnectorTypes(String connectorTypeIdentifier) throws AdminException;

    /**
     * Get the VDBs that correspond to the specified identifier pattern.
     *
     * @param vdbIdentifier the unique identifier for for a {@link VDB} in the system
     * <ul>
     *      <li> <code>"*"</code> - for all VDBs in the system
     *      <li> <code>"name"</code> or <code>"name*"</code> - for all the VDBs that begin with given name
     *      <li><code>"name<{@link AdminObject#DELIMITER_CHAR}>version"</code> - for single VDB
     * </ul>
     * @return Collection of {@link VDB}s.  There could be multiple VDBs with the
     * same name in the Collection but they will differ by VDB version.
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    @RolesAllowed(value=AdminRoles.RoleName.ANONYMOUS)
    Collection<VDB> getVDBs(String vdbIdentifier) throws AdminException;

    /**
     * Get the Connector Bindings that correspond to the specified identifier pattern.
     *
     * @param connectorBindingIdentifier the unique identifier pattern of {@link ConnectorBinding}
     * <ul>
     *      <li> <code>"*"</code> - for all connector bindings in the system
     *      <li> <code>"name*"</code> - for all connector bindings that begin with given name
     *      <li><code>"name"</code> - for single connector binding by the given name
     * </ul>
     * @return Collection of {@link ConnectorBinding}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<ConnectorBinding> getConnectorBindings(String connectorBindingIdentifier) throws AdminException;

    /**
     * Get all the Connector Bindings for the given VDB identifier pattern
	 * @param vdbName - Name of the VDB
	 * @param vdbVersion - version of the VDB
     * @return Collection of {@link ConnectorBinding}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<ConnectorBinding> getConnectorBindingsInVDB(String vdbName, String vdbVersion) throws AdminException;

    /**
     * Get the Extension Modules that correspond to the specified identifier pattern
     * @param extensionModuleIdentifier - the unique identifier for {@link ExtensionModule}
     * <ul>
     *      <li> <code>"*"</code> - for all extension modules in the system
     *      <li> <code>"name*"</code> - for all the extension modules in that begin with given name
     *      <li><code>"name"</code> - for a single extension module identified by given name
     * </ul>
     * @return Collection of {@link ExtensionModule}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<ExtensionModule> getExtensionModules(String extensionModuleIdentifier) throws AdminException;

    /**
     * Get the Queue Worker Pools that correspond to the specified identifier pattern.
     *
     * @param identifier - an identfier for the queues {@link QueueWorkerPool}
     * <ul>
     *      <li> <code>"*"</code> - for all Queue workers in the system
     *      <li> <code>"name*"</code> - for all the Queue workers in that begin with given name
     *      <li><code>"name"</code> - for a single queue in the system
     * </ul>
     * for example, In MM Query - "dqp" will return the Stats for MM Query Worker Pool. Also any Connector Binding
     * name will return the stats for that connector binding.
     * @return Collection of {@link QueueWorkerPool}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<QueueWorkerPool> getQueueWorkerPools(String identifier) throws AdminException;
    
    
    /**
     * Get the Connection Pool Stats that correspond to the specified identifier pattern.
     * If the {@link ConnectionPool ConnectionPool} represents an XA connection, there
     * will be 2 {@link ConnectionPool ConnectionPool}s.  
     *
     * @param identifier - an identfier that corresponds to the ConnectorBinding that is
     * 		running in a process {@link ConnectionPool}
     * <ul>
     *      <li> <code>"*"</code> - for all Connection Pools in the system
     *      <li> <code>"name*"</code> - for all the Connection Pools that begin with given name
     *      <li><code>"name"</code> - for a single Connection Pool in the system
     * </ul>
      * @return Collection of {@link ConnectionPool}
     * @throws AdminException if there's a system error.
     * @since 6.1
     */
    Collection<? extends ConnectionPool> getConnectionPoolStats(String identifier) throws AdminException;
        

    /**
     * Get the Caches that correspond to the specified identifier pattern
     * @param identifier - an identifier for the cache in {@link Cache}
     * <ul>
     *      <li> <code>"*"</code> - for all different caches in the system
     *      <li> <code>"name*"</code> - for all the caches that begin with given name
     *      <li><code>"name"</code> - for a single cache in the system
     * </ul>
     * @return Collection of {@link Cache}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<Cache> getCaches(String identifier) throws AdminException;

    /**
     * Get the Sessions that correspond to the specified identifier pattern
     * @param identifier - an unique identifier for {@link Session}
     * <ul>
     *      <li> <code>"*"</code> - for all current sessions of the system
     *      <li> <code>"number*"</code> - for all the sessions that begin with given number
     *      <li><code>"number"</code> - for a single current session in the system
     * </ul>
     * @return Collection of {@link Session}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<Session> getSessions(String identifier) throws AdminException;

    /**
     * Get the Requests that correspond to the specified identifier pattern
     * @param identifier - An Identifier for {@link Request}
     * <ul>
     *      <li> <code>"*"</code> - for all current in process requests of the system
     *      <li> <code>"number* or number<{@link AdminObject#DELIMITER_CHAR}>*"</code> - for all the sessions
     *      that begin with given number, or all the requests for particular session etc.
     *      <li><code>"number<{@link AdminObject#DELIMITER_CHAR}>number"</code> - for a single request in the system
     * </ul>
     * @return Collection of {@link Request}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */

    Collection<Request> getRequests(String identifier) throws AdminException;

    /**
     * Get the Source Request that correspond to the specified identifier pattern
     * @param identifier An Identifier for {@link Request}
     * <ul>
     *      <li> <code>"*"</code> - for all current in process requests of the system
     *      <li> <code>"number* or number<{@link AdminObject#DELIMITER_CHAR}>* or number.number.*"</code> - for all the sessions
     *      that begin with given number, or all the requests for particular session etc.
     *      <li><code>"number<{@link AdminObject#DELIMITER_CHAR}>number<{@link AdminObject#DELIMITER_CHAR}>number"</code> - for a single source request in the system
     * </ul>
     * @return Collection of {@link Request}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection<Request> getSourceRequests(String identifier) throws AdminException;
    

    /**
     * Get all of the available Configuration Properties for the specified AdminObject, and details about them.
     * @param connectorTypeIdentifier
     * @return
     * @throws AdminException
     */
    Collection<PropertyDefinition> getConnectorTypePropertyDefinitions(String connectorTypeIdentifier) throws AdminException;
    
    
    /**
     * Get all transaction matching the identifier.
     * @return
     * @throws AdminException
     */
    Collection<Transaction> getTransactions() throws AdminException;
    
    /**
     * Get the processes that correspond to the specified identifier pattern.
     *
     * @param processIdentifier the unique identifier for for a {@link org.teiid.adminapi.ProcessObject ProcessObject}
     * in the system or "{@link org.teiid.adminapi.AdminObject#WILDCARD WILDCARD}"
     * if all Processes are desired.
     * @return Collection of {@link org.teiid.adminapi.ProcessObject ProcessObject}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    @RolesAllowed(value=AdminRoles.RoleName.ANONYMOUS)
    Collection<ProcessObject> getProcesses(String processIdentifier) throws AdminException;

}
