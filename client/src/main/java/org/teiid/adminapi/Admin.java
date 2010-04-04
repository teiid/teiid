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

import java.io.InputStream;
import java.io.Reader;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

public interface Admin {
	
	public enum Cache {CODE_TABLE_CACHE,PREPARED_PLAN_CACHE, QUERY_SERVICE_RESULT_SET_CACHE};
    
    /**
     * Assign a {@link ConnectorBinding} to a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model to map Connector Binding
     * @param sourceName sourceName for the model
     * @param jndiName JNDI names to which the source name needs to map to
     * @throws AdminException if there's a system error or if there's a user input error.
     */
    void assignBindingToModel(String vdbName,
                              int vdbVersion,
                              String modelName,
                              String sourceName,
                              String jndiName) throws AdminException;
    
    /**
     * Set/update the property for the Connector Binding identified by the given deployed name.
     * @param deployedName
     * @param propertyName
     * @param propertyValue
     * @throws AdminException
     */
    void setConnectorBindingProperty(String deployedName, String propertyName, String propertyValue) throws AdminException;
    
    /**
     * Add Connector Type, will import Connector Type from a file
     *
     * @param name  of the Connector Type to add
     * @param rar RAR file
     * @throws AdminException  if there's a system error.
     */
    void addConnectorType(String name, InputStream rar) throws AdminException;

    /**
     * Delete Connector Type from Next Configuration
     *
     * @param name String name of the Connector Type to delete
     * @throws AdminException
     *             if there's a system error.
     */
    void deleteConnectorType(String name) throws AdminException;
    
    /**
     * Export Connector Type rar file
     *
     * @param @param name  of the Connector Type
     * @return InputStream of contents of the rar file
     * @throws AdminException if there's a system error.
     */
    InputStream exportConnectorType(String name) throws AdminException;    

    /**
     * Deploy a {@link ConnectorBinding} to Configuration
     *
     * @param deployedName  Connector Binding name that will be added to Configuration
     * @param typeName Connector type name. 
     * @param properties Name & Value pair need to deploy the Connector Binding

     * @throws AdminException if there's a system error.
     */
    ConnectorBinding addConnectorBinding(String deployedName, String typeName, Properties properties) throws AdminException;

    /**
     * Delete the {@link ConnectorBinding} from the Configuration
     *
     * @param deployedName - deployed name of the connector binding
     * @throws AdminException  if there's a system error.
     */
    void deleteConnectorBinding(String deployedName) throws AdminException;
    
    /**
     * Export a {@link ConnectorBinding} to character Array in XML format
     *
     * @param deployedName the unique identifier for a {@link ConnectorBinding}.
     * @return Reader in XML format
     * @throws AdminException
     *             if there's a system error.
     */
    Reader exportConnectorBinding(String deployedName) throws AdminException;    

    /**
     * Deploy a {@link VDB} file.
     * @param name  Name of the VDB file to save under
     * @param VDB 	VDB.
     * @throws AdminException
     *             if there's a system error.
     * @return the {@link VDB} representing the current property values and runtime state.
     */
    public void deployVDB(String fileName, InputStream vdb) throws AdminException;
    
    
    /**
     * Delete the VDB with the given name and version
     * @param vdbName
     * @param version
     * @throws AdminException
     */
    void deleteVDB(String vdbName, int vdbVersion) throws AdminException;
    
    /**
     * Export VDB to byte array
     *
     * @param vdbName identifier of the {@link VDB}
     * @param vdbVersion {@link VDB} version
     * @return InputStream of the VDB
     * @throws AdminException if there's a system error.
     */
    InputStream exportVDB(String vdbName, int vdbVersion) throws AdminException;    
    
    /**
     * Set a process level property. 
     * @param propertyName - name of the property
     * @param propertyValue - value of the property
     */
    void setRuntimeProperty(String propertyName, String propertyValue) throws AdminException;
    
    /**
     * Get the Connector Types available in the configuration.
     *
     * @return Set of connector types.
     * @throws AdminException if there's a system error.
     */
    Set<String> getConnectorTypes() throws AdminException;

    /**
     * Get the VDBs that currently deployed in the system
     *
     * @return Collection of {@link VDB}s.  There could be multiple VDBs with the
     * same name in the Collection but they will differ by VDB version.
     * @throws AdminException if there's a system error.
     */
    Set<VDB> getVDBs() throws AdminException;
    
    /**
     * Get the VDB
     * @param vdbName
     * @param vbdVersion
     * @throws AdminException if there's a system error.
     * @return
     */
    VDB getVDB(String vdbName, int vbdVersion) throws AdminException;

    /**
     * Get the Connector Bindings that are available in the configuration
     *
     * @return Collection of {@link ConnectorBinding}
     * @throws AdminException if there's a system error.
     */
    Collection<ConnectorBinding> getConnectorBindings() throws AdminException;
    
    /**
     * Get the connector binding by the given the deployed name.
     * @param deployedName - name of the deployed connector binding
     * @return null if not found a connector binding by the given name
     * @throws AdminException if there's a system error.
     */
    ConnectorBinding getConnectorBinding(String deployedName) throws AdminException;

    /**
     * Get all the Connector Bindings for the given VDB identifier pattern
	 * @param vdbName - Name of the VDB
	 * @param vdbVersion - version of the VDB
     * @return Collection of {@link ConnectorBinding}
     * @throws AdminException if there's a system error.
     */
    Collection<ConnectorBinding> getConnectorBindingsInVDB(String vdbName, int vdbVersion) throws AdminException;

    /**
     * Get the Work Manager stats that correspond to the specified identifier pattern.
     *
     * @param identifier - an identifier for the queues {@link QueueWorkerPool}. "runtime" will return the stats for Query 
     * runtime Worker Pool. Also any Connector Binding name will return the stats for that connector binding.
     * @return Collection of {@link QueueWorkerPool}
     * @throws AdminException if there's a system error.
     */
    WorkerPoolStatistics getWorkManagerStats(String identifier) throws AdminException;
    
    
    /**
     * Get the Connection Pool Stats that correspond to the specified identifier pattern.
     * If the {@link ConnectionPoolStatistics ConnectionPool} represents an XA connection, there
     * will be 2 {@link ConnectionPoolStatistics ConnectionPool}s.  
     *
     * @param deployedName - an identifier that corresponds to the ConnectorBinding Name
     * @return {@link ConnectionPoolStatistics}
     * @throws AdminException if there's a system error.
     */
    ConnectionPoolStatistics getConnectorConnectionPoolStats(String deployedName) throws AdminException;
        

    /**
     * Get the Caches that correspond to the specified identifier pattern
     * @return Collection of {@link String}
     * @throws AdminException if there's a system error.
     */
    Collection<String> getCacheTypes() throws AdminException;

    /**
     * Get all the current Sessions.
     * @return Collection of {@link Session}
     * @throws AdminException if there's a system error.
     */
    Collection<Session> getSessions() throws AdminException;

    /**
     * Get the all Requests that are currently in process
     * @return Collection of {@link Request}
     * @throws AdminException if there's a system error.
     */
    Collection<Request> getRequests() throws AdminException;
    
    /**
     * Get the Requests for the given session
     * @return Collection of {@link Request}
     * @throws AdminException if there's a system error.
     */
    Collection<Request> getRequestsForSession(long sessionId) throws AdminException;
    

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
     */
    Collection<ProcessObject> getProcesses(String processIdentifier) throws AdminException;
    
    
    /**
     * Start Connector Binding
     *
     * @param deployedName 
     * @throws AdminException  if there's a system error.
     */
    void startConnectorBinding(ConnectorBinding binding) throws AdminException;

    /**
     * Stop Connector Binding
     *
     * @param deployedName  identifier for {@link org.teiid.adminapi.ConnectorBinding}
     */
    void stopConnectorBinding(ConnectorBinding binding) throws AdminException;

    /**
     * Clear the cache or caches specified by the cacheIdentifier.
     * @param cacheType Cache Type
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     */
    void clearCache(String cacheType) throws AdminException;

    /**
     * Terminate the Session
     *
     * @param identifier  Session Identifier {@link org.teiid.adminapi.Session}.
     * No wild cards currently supported, must be explicit
     * @throws AdminException  if there's a system error.
     */
    void terminateSession(long sessionId) throws AdminException;

    /**
     * Cancel Request
     *
     * @param sessionId session Identifier for the request.
     * @param requestId request Identifier
     * 
     * @throws AdminException  if there's a system error.
     */
    void cancelRequest(long sessionId, long requestId) throws AdminException;
  
    /**
     * Mark the given global transaction as rollback only.
     * @param transactionId
     * @throws AdminException
     */
    void terminateTransaction(String transactionId) throws AdminException;
    
    /**
     * Adds JDBC XA Data Source in the container.
     * @param dsName - name of the source
     * @param properties - properties
     * @throws AdminException
     */
    void addDataSource(String deploymentName, Properties properties) throws AdminException;
    
    /**
     * Delete data source. 
     * @param dsName
     * @throws AdminException
     */
    void deleteDataSource(String deploymentName) throws AdminException;
    
    /**
     * Get the property definitions for creating the JDBC data source.
     * @return
     * @throws AdminException
     */
    Collection<PropertyDefinition> getDataSourcePropertyDefinitions() throws AdminException;
    
    /**
     * Closes the admin connection
     */
    void close();
    
    /**
     * Assign a Role name to the Data Policy in a given VDB
     *  
     * @param vdbName
     * @param vdbVersion
     * @param policyName
     * @param role
     */
    void addRoleToDataPolicy(String vdbName, int vdbVersion, String policyName, String role) throws AdminException;
    
    /**
     * Assign a Role name to the Data Policy in a given VDB
     *  
     * @param vdbName
     * @param vdbVersion
     * @param policyName
     * @param role
     */
    void removeRoleFromDataPolicy(String vdbName, int vdbVersion, String policyName, String role) throws AdminException;  

}
