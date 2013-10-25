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
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.VDB.ConnectionType;

public interface Admin {

	public enum Cache {PREPARED_PLAN_CACHE, QUERY_SERVICE_RESULT_SET_CACHE};

	public enum SchemaObjectType {TABLES, PROCEDURES, FUNCTIONS};

    /**
     * Assign a {@link Translator} and Data source to a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model
     * @param sourceName source name
     * @param translatorName
     * @param dsName data source name that can found in the JNDI map.
     * @throws AdminException
     * @deprecated
     */
    @Deprecated
	void assignToModel(String vdbName, int vdbVersion, String modelName, String sourceName, String translatorName, String dsName) throws AdminException;
    
    /**
     * Removes a {@link Translator} and Data source from a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model
     * @param sourceName source name
     * @throws AdminException
     */
	void removeSource(String vdbName, int vdbVersion, String modelName,
			String sourceName)
			throws AdminException;

    /**
     * Adds a {@link Translator} and Data source to a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model
     * @param sourceName source name
     * @param translatorName
     * @param dsName data source name that can found in the JNDI map.
     * @throws AdminException
     */
	void addSource(String vdbName, int vdbVersion, String modelName,
			String sourceName, String translatorName, String dsName)
			throws AdminException;

    /**
     * Update a source's {@link Translator} and Data source
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param sourceName source name
     * @param translatorName
     * @param dsName data source name that can found in the JNDI map.
     * @throws AdminException
     */
	void updateSource(String vdbName, int vdbVersion, String sourceName,
			String translatorName, String dsName) throws AdminException;

    /**
     * Change the {@link ConnectionType} of the {@link VDB}.
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param type
     * @throws AdminException
     */
    void changeVDBConnectionType(String vdbName, int vdbVersion, ConnectionType type) throws AdminException;

    /**
     * Deploy a artifact (VDB, JAR, RAR files)
     * @param deployName  Name of the VDB file to save under
     * @param content
     * @throws AdminException
     */
    public void deploy(String deployName, InputStream content) throws AdminException;


    /**
     * Undeploy artifact (VDB, JAR, RAR files)
     * @param deployedName
     * @throws AdminException
     */
    void undeploy(String deployedName) throws AdminException;

    /**
     * Get the VDBs that currently deployed in the system
     *
     * @return Collection of {@link VDB}s.  There could be multiple VDBs with the
     * same name in the Collection but they will differ by VDB version.
     * @throws AdminException
     */
    Collection<? extends VDB> getVDBs() throws AdminException;

    /**
     * Get the VDB
     * @param vdbName
     * @param vbdVersion
     * @throws AdminException
     * @return
     */
    VDB getVDB(String vdbName, int vdbVersion) throws AdminException;

    /**
     * Restart the VDB. This issues reload of the metadata.
     * @param vdbName
     * @param vbdVersion
     * @param models names for which metadata needs to be reloaded, if null or not supplied all models reloaded
     * @throws AdminException
     * @return
     */
    void restartVDB(String vdbName, int vdbVersion, String... models) throws AdminException;

    /**
     * Get the translators that are available in the configuration
     *
     * @return Collection of {@link Translator}
     * @throws AdminException
     */
    Collection<? extends Translator> getTranslators() throws AdminException;

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
     * @return Collection of {@link WorkerPoolStatistics}
     * @throws AdminException
     */
    Collection<? extends WorkerPoolStatistics> getWorkerPoolStats() throws AdminException;

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
    Collection<? extends Session> getSessions() throws AdminException;

    /**
     * Get the all Requests that are currently in process
     * @return Collection of {@link Request}
     * @throws AdminException
     */
    Collection<? extends Request> getRequests() throws AdminException;

    /**
     * Get the Requests for the given session
     * @return Collection of {@link Request}
     * @throws AdminException
     */
    Collection<? extends Request> getRequestsForSession(String sessionId) throws AdminException;


    /**
     * Get all of the available configuration Properties for the specified connector
     * @param templateName - Name of the connector
     * @return
     * @throws AdminException
     */
    Collection<? extends PropertyDefinition> getTemplatePropertyDefinitions(String templateName) throws AdminException;

    /**
     * Get all of the available configuration Properties for the specified translator
     * @param translatorName - Name of the translator
     * @return
     * @throws AdminException
     */
    Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(String translatorName) throws AdminException;
    

    /**
     * Get all transaction matching the identifier.
     * @return
     * @throws AdminException
     */
    Collection<? extends Transaction> getTransactions() throws AdminException;

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
     * @return Collection of {@link CacheStatistics}
     * @throws AdminException
     */
    Collection<? extends CacheStatistics> getCacheStats(String cacheType) throws AdminException;

    /**
     * Get the Engine Statistics for the given type
     * @return Collection of {@link EngineStatistics}
     * @throws AdminException
     */
    Collection<? extends EngineStatistics> getEngineStats() throws AdminException;

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
     * Creates a JCA data source
     * @param deploymentName - name of the source
     * @param templateName - template of data source
     * @param properties - properties
     * @throws AdminException
     */
    void createDataSource(String deploymentName, String templateName, Properties properties) throws AdminException;

    /**
     * Given the deployed name of the data source, this will return all the configuration properties
     * used to create the datasource. If sensitive information like passwords are masked, they will NOT
     * be decrypted. "driver-name" property on the returned properties defines the template name used
     * to create this data source.
     */
    Properties getDataSource(String deployedName) throws AdminException;

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

    /**
     * Retrieve the schema of the given model
     *
     * @param vdbName
     * @param vdbVersion
     * @param modelName
     * @param EnumSet<SchemaObjectType> Type of schema objects to retrieve, null means ALL the schema object types
     * @param typeNamePattern RegEx pattern to filter to names of tables, procedures that are being read. Null means no filter.
     */
    String getSchema(String vdbName, int vdbVersion, String modelName, EnumSet<SchemaObjectType> allowedTypes, String typeNamePattern) throws AdminException;

    /**
     * Get the Query Plan for the given session with provided execution id.
     * @param sessionId
     * @param executionId
     * @return
     */
    String getQueryPlan(String sessionId, int executionId) throws AdminException;

    /**
     * Restart the Server
     * @throws AdminException
     */
    void restart();
}
