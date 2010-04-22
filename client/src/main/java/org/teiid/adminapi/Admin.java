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

	public enum DataSourceType {XA, LOCAL};
	public enum Cache {CODE_TABLE_CACHE,PREPARED_PLAN_CACHE, QUERY_SERVICE_RESULT_SET_CACHE};
    
    /**
     * Assign a {@link ConnectionFactory} to a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model to map Connection Factory
     * @param sourceName sourceName for the model
     * @param jndiName JNDI names to which the source name needs to map to
     * @throws AdminException
     */
    void assignConnectionFactoryToModel(String vdbName, int vdbVersion, String modelName, String sourceName, String jndiName) throws AdminException;
    
    /**
     * Set/update the property for the Connection Factory identified by the given deployed name.
     * @param deployedName
     * @param propertyName
     * @param propertyValue
     * @throws AdminException
     */
    void setConnectionFactoryProperty(String deployedName, String propertyName, String propertyValue) throws AdminException;
    
    /**
     * Add Connector, will import RAR from a file
     *
     * @param name  of the Connector to add
     * @param rar RAR file
     * @throws AdminException  
     */
    void addConnector(String name, InputStream rar) throws AdminException;

    /**
     * Delete Connector 
     *
     * @param name String name of the Connector to delete
     * @throws AdminException 
     */
    void deleteConnector(String name) throws AdminException;
    
    /**
     * Export Connector RAR file
     *
     * @param name  of the Connector
     * @return InputStream of contents of the rar file
     * @throws AdminException 
     */
    InputStream exportConnector(String name) throws AdminException;    

    /**
     * Deploy a {@link ConnectionFactory} to Configuration
     *
     * @param deployedName  Connection Factory name that will be added to Configuration
     * @param typeName Connector type name. 
     * @param properties Name & Value pair need to deploy the Connection Factory

     * @throws AdminException 
     */
    ConnectionFactory addConnectionFactory(String deployedName, String typeName, Properties properties) throws AdminException;

    /**
     * Delete the {@link ConnectionFactory} from the Configuration
     *
     * @param deployedName - deployed name of the connection factory
     * @throws AdminException  
     */
    void deleteConnectionFactory(String deployedName) throws AdminException;
    
    /**
     * Export a {@link ConnectionFactory} to character Array in XML format
     *
     * @param deployedName the unique identifier for a {@link ConnectionFactory}.
     * @return Reader in XML format
     * @throws AdminException
     *             
     */
    Reader exportConnectionFactory(String deployedName) throws AdminException;    

    /**
     * Deploy a {@link VDB} file.
     * @param name  Name of the VDB file to save under
     * @param VDB 	VDB.
     * @throws AdminException
     *             
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
     * @throws AdminException 
     */
    InputStream exportVDB(String vdbName, int vdbVersion) throws AdminException;    
    
    /**
     * Set a process level property. 
     * @param propertyName - name of the property
     * @param propertyValue - value of the property
     */
    void setRuntimeProperty(String propertyName, String propertyValue) throws AdminException;
    
    /**
     * Get the Connectors  available in the configuration.
     *
     * @return Set of connector names.
     * @throws AdminException 
     */
    Set<String> getConnectorNames() throws AdminException;

    /**
     * Get the VDBs that currently deployed in the system
     *
     * @return Collection of {@link VDB}s.  There could be multiple VDBs with the
     * same name in the Collection but they will differ by VDB version.
     * @throws AdminException 
     */
    Set<VDB> getVDBs() throws AdminException;
    
    /**
     * Get the VDB
     * @param vdbName
     * @param vbdVersion
     * @throws AdminException 
     * @return
     */
    VDB getVDB(String vdbName, int vbdVersion) throws AdminException;

    /**
     * Get the Connection Factories that are available in the configuration
     *
     * @return Collection of {@link ConnectionFactory}
     * @throws AdminException 
     */
    Collection<ConnectionFactory> getConnectionFactories() throws AdminException;
    
    /**
     * Get the Connection Factory by the given the deployed name.
     * @param deployedName - name of the deployed Connection Factory
     * @return null if not found a Connection Factory by the given name
     * @throws AdminException 
     */
    ConnectionFactory getConnectionFactory(String deployedName) throws AdminException;

    /**
     * Get all the Connection Factories for the given VDB identifier pattern
	 * @param vdbName - Name of the VDB
	 * @param vdbVersion - version of the VDB
     * @return Collection of {@link ConnectionFactory}
     * @throws AdminException 
     */
    Collection<ConnectionFactory> getConnectionFactoriesInVDB(String vdbName, int vdbVersion) throws AdminException;

    /**
     * Get the Work Manager stats that correspond to the specified identifier pattern.
     *
     * @param identifier - an identifier for the queues {@link QueueWorkerPool}. 
     * @return Collection of {@link QueueWorkerPool}
     * @throws AdminException 
     */
    WorkerPoolStatistics getWorkManagerStats(String identifier) throws AdminException;
    
    
    /**
     * Get the Connection Pool Stats that correspond to the specified identifier pattern.
     * If the {@link ConnectionPoolStatistics ConnectionPool} represents an XA connection, there
     * will be 2 {@link ConnectionPoolStatistics ConnectionPool}s.  
     *
     * @param deployedName - an identifier that corresponds to the connection factory Name
     * @return {@link ConnectionPoolStatistics}
     * @throws AdminException 
     */
    ConnectionPoolStatistics getConnectionFactoryStats(String deployedName) throws AdminException;
        

    /**
     * Get the Caches that correspond to the specified identifier pattern
     * @return Collection of {@link String}
     * @throws AdminException 
     */
    Collection<String> getCacheTypes() throws AdminException;

    /**
     * Get all the current Sessions.
     * @return Collection of {@link Session}
     * @throws AdminException 
     */
    Collection<Session> getSessions() throws AdminException;

    /**
     * Get the all Requests that are currently in process
     * @return Collection of {@link Request}
     * @throws AdminException 
     */
    Collection<Request> getRequests() throws AdminException;
    
    /**
     * Get the Requests for the given session
     * @return Collection of {@link Request}
     * @throws AdminException 
     */
    Collection<Request> getRequestsForSession(String sessionId) throws AdminException;
    

    /**
     * Get all of the available configuration Properties for the specified connector
     * @param connectorName - Name of the connector
     * @return
     * @throws AdminException
     */
    Collection<PropertyDefinition> getConnectorPropertyDefinitions(String connectorName) throws AdminException;
    
    
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
     * Start Connection Factory
     *
     @param deployedName - name of the deployed Connection Factory 
     * @throws AdminException  
     */
    void startConnectionFactory(String deployedName) throws AdminException;

    /**
     * Stop Connection Factory
     *
     * @param deployedName - name of the deployed Connection Factory 
     * @throws AdminException  
     */
    void stopConnectionFactory(String deployedName) throws AdminException;

    /**
     * Clear the cache or caches specified by the cacheIdentifier.
     * @param cacheType Cache Type
     * No wild cards currently supported, must be explicit
     * @throws AdminException  
     */
    void clearCache(String cacheType) throws AdminException;

    /**
     * Terminate the Session
     *
     * @param identifier  Session Identifier {@link org.teiid.adminapi.Session}.
     * No wild cards currently supported, must be explicit
     * @throws AdminException  
     */
    void terminateSession(String sessionId) throws AdminException;

    /**
     * Cancel Request
     *
     * @param sessionId session Identifier for the request.
     * @param requestId request Identifier
     * 
     * @throws AdminException  
     */
    void cancelRequest(String sessionId, long requestId) throws AdminException;
  
    /**
     * Mark the given global transaction as rollback only.
     * @param transactionId
     * @throws AdminException
     */
    void terminateTransaction(String transactionId) throws AdminException;
    
    /**
     * Adds JDBC XA Data Source in the container.
     * @param deploymentName - name of the source
     * @param type - type of data source
     * @param properties - properties
     * @throws AdminException
     */
    void addDataSource(String deploymentName, DataSourceType type, Properties properties) throws AdminException;
    
    /**
     * Delete data source. 
     * @param deployedName
     * @throws AdminException
     */
    void deleteDataSource(String deployedName) throws AdminException;
    
    /**
     * Export the data source in "-ds.xml" file format. 
     * @param deployedName
     * @return
     * @throws AdminException
     */
    Reader exportDataSource(String deployedName) throws AdminException;
    
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
    
    
    /**
     * Merge the Source VDB into Target VDB. Both Source and Target VDBs must be present for this method to
     * succeed. The changes will not be persistent between server restarts.
     * @param sourceVDBName
     * @param sourceVDBVersion
     * @param targetVDBName
     * @param targetVDBVersion
     */
    void mergeVDBs(String sourceVDBName, int sourceVDBVersion, String targetVDBName, int targetVDBVersion) throws AdminException;

}
