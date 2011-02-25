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
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.VDB.ConnectionType;

public interface Admin {

	public enum Cache {PREPARED_PLAN_CACHE, QUERY_SERVICE_RESULT_SET_CACHE};
    
    /**
     * Assign a {@link Translator} and Data source to a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model to map Connection Factory
     * @param sourceName sourceName for the model
     * @param translatorName 
     * @param dsName data source name that can found in the JNDI map.
     * @throws AdminException
     */
    void assignToModel(String vdbName, int vdbVersion, String modelName, String sourceName, String translatorName, String dsName) throws AdminException;
    
    /**
     * Change the {@link ConnectionType} of the {@link VDB}.
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param type
     * @throws AdminException 
     */
    void changeVDBConnectionType(String vdbName, int vdbVersion, ConnectionType type) throws AdminException;
    
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
     * Get the translators that are available in the configuration
     *
     * @return Collection of {@link Translator}
     * @throws AdminException 
     */
    Collection<Translator> getTranslators() throws AdminException;
    
    /**
     * Get the translator by the given the deployed name.
     * @param deployedName - name of the deployed translator
     * @return null if not found
     * @throws AdminException 
     */
    Translator getTranslator(String deployedName) throws AdminException;

    /**
     * Get the Worker Pool statistics in runtime engine.
     *
     * @return {@link WorkerPoolStatistics}
     * @throws AdminException 
     */
    WorkerPoolStatistics getWorkerPoolStats() throws AdminException;
    
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
     * @param templateName - Name of the connector
     * @return
     * @throws AdminException
     */
    Collection<PropertyDefinition> getTemplatePropertyDefinitions(String templateName) throws AdminException;
    
    
    /**
     * Get all transaction matching the identifier.
     * @return
     * @throws AdminException
     */
    Collection<Transaction> getTransactions() throws AdminException;
    
    /**
     * Clear the cache or caches specified by the cacheIdentifier.
     * @param cacheType Cache Type
     * No wild cards currently supported, must be explicit
     * @throws AdminException  
     */
    void clearCache(String cacheType) throws AdminException;
    
    /**
     * Clear the cache of the given VDB for provided cache type
     * @param cacheType Cache Type
     * No wild cards currently supported, must be explicit
     * @param vdbName - Name of the VDB
     * @param vdbVersion - VDB version
     * @throws AdminException  
     */
    void clearCache(String cacheType, String vdbName, int vdbVersion) throws AdminException;    

    
    /**
     * Get the Cache Statistics for the given type
     * @param cacheType Cache Type
     * @return {@link CacheStatistics} 
     * @throws AdminException  
     */
    CacheStatistics getCacheStats(String cacheType) throws AdminException;
    
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
     * @param executionId request Identifier
     * 
     * @throws AdminException  
     */
    void cancelRequest(String sessionId, long executionId) throws AdminException;
  
    /**
     * Mark the given global transaction as rollback only.
     * @param transactionId
     * @throws AdminException
     */
    void terminateTransaction(String transactionId) throws AdminException;
    
    /**
     * Closes the admin connection
     */
    void close();
    
    /**
     * Assign a Role name to the Data Role in a given VDB
     *  
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param mappedRoleName
     */
    void addDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException;
    
    /**
     * Remove a Role name to the Data Role in a given VDB
     *  
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param mappedRoleName
     */
    void removeDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException;
    
    /**
     * Set the any authenticated flag on the Data Role in a given VDB
     *  
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param anyAuthenticated
     */
    void setAnyAuthenticatedForDataRole(String vdbName, int vdbVersion, String dataRole, boolean anyAuthenticated) throws AdminException;
    
    /**
     * Merge the Source VDB into Target VDB. Both Source and Target VDBs must be present for this method to
     * succeed. The changes will not be persistent between server restarts.
     * @param sourceVDBName
     * @param sourceVDBVersion
     * @param targetVDBName
     * @param targetVDBVersion
     */
    void mergeVDBs(String sourceVDBName, int sourceVDBVersion, String targetVDBName, int targetVDBVersion) throws AdminException;

    
    /**
     * Creates a JCA data source
     * @param deploymentName - name of the source
     * @param templateName - template of data source
     * @param properties - properties
     * @throws AdminException
     */
    void createDataSource(String deploymentName, String templateName, Properties properties) throws AdminException;
    
    /**
     * Delete data source. 
     * @param deployedName
     * @throws AdminException
     */
    void deleteDataSource(String deployedName) throws AdminException;
    
    /**
     * Returns the all names of all the data sources available in the configuration.
     */
    Collection<String> getDataSourceNames() throws AdminException;
    
    /**
     * Get the Datasource templates  available in the configuration.
     *
     * @return Set of template names.
     * @throws AdminException 
     */
    Set<String> getDataSourceTemplateNames() throws AdminException;
    
    /**
     * Tell the engine that the given source is available. Pending dynamic vdb metadata loads will be resumed.
     * @param jndiName
     * @throws AdminException
     */
    void markDataSourceAvailable(String jndiName) throws AdminException;
}
