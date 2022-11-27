/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.adminapi;

import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.VDB.ConnectionType;

public interface Admin {

    public enum Cache {PREPARED_PLAN_CACHE, QUERY_SERVICE_RESULT_SET_CACHE};

    public enum SchemaObjectType {TABLES, PROCEDURES, FUNCTIONS};

    public enum TranlatorPropertyType{IMPORT, OVERRIDE, EXTENSION_METADATA, ALL};

    /**
     * Removes a {@link Translator} and Data source from a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model
     * @param sourceName source name
     * @throws AdminException
     */
    @Deprecated
    void removeSource(String vdbName, int vdbVersion, String modelName,
            String sourceName)
            throws AdminException;

    /**
     * Removes a {@link Translator} and Data source from a {@link VDB}'s Model
     *
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param modelName  Name of the Model
     * @param sourceName source name
     * @throws AdminException
     */
    void removeSource(String vdbName, String vdbVersion, String modelName,
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
    @Deprecated
    void addSource(String vdbName, int vdbVersion, String modelName,
            String sourceName, String translatorName, String dsName)
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
    void addSource(String vdbName, String vdbVersion, String modelName,
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
    @Deprecated
    void updateSource(String vdbName, int vdbVersion, String sourceName,
            String translatorName, String dsName) throws AdminException;

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
    void updateSource(String vdbName, String vdbVersion, String sourceName,
            String translatorName, String dsName) throws AdminException;

    /**
     * Change the {@link ConnectionType} of the {@link VDB}.
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param type
     * @throws AdminException
     */
    @Deprecated
    void changeVDBConnectionType(String vdbName, int vdbVersion, ConnectionType type) throws AdminException;

    /**
     * Change the {@link ConnectionType} of the {@link VDB}.
     * @param vdbName Name of the VDB
     * @param vdbVersion Version of the VDB
     * @param type
     * @throws AdminException
     */
    void changeVDBConnectionType(String vdbName, String vdbVersion, ConnectionType type) throws AdminException;

    /**
     * Deploy a artifact (VDB, JAR, RAR files)
     * @param deployName  Name of the VDB file to save under
     * @param content
     * @throws AdminException
     */
    public void deploy(String deployName, InputStream content) throws AdminException;

    /**
     * Deploy a artifact (VDB, JAR, RAR files)
     * @param deployName  Name of the VDB file to save under
     * @param content
     * @param persistent the deployed artifact is persisted or not
     * @throws AdminException
     */
    public void deploy(String deployName, InputStream content, boolean persistent) throws AdminException;

    /**
     * Get existing deployments on in the sysem
     * @throws AdminException
     */
    public List<String> getDeployments() throws AdminException;

    /**
     * Undeploy artifact (VDB, JAR, RAR files)
     * @param deployedName
     * @throws AdminException
     */
    void undeploy(String deployedName) throws AdminException;

    /**
     * Get the VDBs that are currently deployed in the system
     *
     * @return Collection of {@link VDB}s.  There could be multiple VDBs with the
     * same name in the Collection but they will differ by VDB version.
     * @throws AdminException
     */
    Collection<? extends VDB> getVDBs() throws AdminException;

    /**
     * Get the VDB
     * @param vdbName
     * @param vdbVersion
     * @throws AdminException
     * @return
     */
    @Deprecated
    VDB getVDB(String vdbName, int vdbVersion) throws AdminException;

    /**
     * Get the VDB
     * @param vdbName
     * @param vdbVersion
     * @throws AdminException
     * @return
     */
    VDB getVDB(String vdbName, String vdbVersion) throws AdminException;

    /**
     * Restart the VDB. This issues reload of the metadata.
     * @param vdbName
     * @param vdbVersion
     * @param models names for which metadata needs to be reloaded, if null or not supplied all models reloaded
     * @throws AdminException
     */
    @Deprecated
    void restartVDB(String vdbName, int vdbVersion, String... models) throws AdminException;

    /**
     * Restart the VDB. This issues reload of the metadata.
     * @param vdbName
     * @param vdbVersion
     * @param models names for which metadata needs to be reloaded, if null or not supplied all models reloaded
     * @throws AdminException
     */
    void restartVDB(String vdbName, String vdbVersion, String... models) throws AdminException;

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
     * @see #getTranslatorPropertyDefinitions
     * @deprecated
     */
    @Deprecated
    Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(String translatorName) throws AdminException;

    /**
     * Get all of the available configuration Properties for the specified translator
     * @param translatorName - Name of the translator
     * @param type - Type of property definition (import, override, extension-metadata)
     * @return
     * @throws AdminException
     */
    Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(String translatorName, TranlatorPropertyType type) throws AdminException;


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
    @Deprecated
    void clearCache(String cacheType, String vdbName, int vdbVersion) throws AdminException;

    /**
     * Clear the cache of the given VDB for provided cache type
     * @param cacheType Cache Type
     * No wild cards currently supported, must be explicit
     * @param vdbName - Name of the VDB
     * @param vdbVersion - VDB version
     * @throws AdminException
     */
    void clearCache(String cacheType, String vdbName, String vdbVersion) throws AdminException;

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
     * @param sessionId  Session Identifier {@link org.teiid.adminapi.Session}.
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
    @Deprecated
    void addDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException;

    /**
     * Assign a Role name to the Data Role in a given VDB
     *
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param mappedRoleName
     */
    void addDataRoleMapping(String vdbName, String vdbVersion, String dataRole, String mappedRoleName) throws AdminException;

    /**
     * Remove a Role name to the Data Role in a given VDB
     *
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param mappedRoleName
     */
    @Deprecated
    void removeDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException;

    /**
     * Remove a Role name to the Data Role in a given VDB
     *
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param mappedRoleName
     */
    void removeDataRoleMapping(String vdbName, String vdbVersion, String dataRole, String mappedRoleName) throws AdminException;

    /**
     * Set the any authenticated flag on the Data Role in a given VDB
     *
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param anyAuthenticated
     */
    @Deprecated
    void setAnyAuthenticatedForDataRole(String vdbName, int vdbVersion, String dataRole, boolean anyAuthenticated) throws AdminException;

    /**
     * Set the any authenticated flag on the Data Role in a given VDB
     *
     * @param vdbName
     * @param vdbVersion
     * @param dataRole
     * @param anyAuthenticated
     */
    void setAnyAuthenticatedForDataRole(String vdbName, String vdbVersion, String dataRole, boolean anyAuthenticated) throws AdminException;

    /**
     * Creates a JCA data source
     * <br>
     * Use this method to create JDBC driver based connection, XA-datasource or Resource Adapter.
     * Template Name defines the type of connection, if the template name is ends with "-xa" it is
     * considered to be a XA based data source.
     * <br>
     * @param deploymentName - This becomes the pool name, as well as the jndi name of the source
     * @param templateName - type of source. See {@link #getDataSourceNames} for all available types.
     * @param properties - All properties needed to create a data source, like connection-url, user, password
     *        to see all the properties use {@link #getTemplatePropertyDefinitions(String)} to retrieve the full list.
     *        The transaction-support property can be set to LocalTransaction for sources that support local transactions.
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
     * @param allowedTypes {@link EnumSet} Type of schema objects to retrieve, null means ALL the schema object types
     * @param typeNamePattern RegEx pattern to filter to names of tables, procedures that are being read. Null means no filter.
     */
    @Deprecated
    String getSchema(String vdbName, int vdbVersion, String modelName, EnumSet<SchemaObjectType> allowedTypes, String typeNamePattern) throws AdminException;

    /**
     * Retrieve the schema of the given model
     *
     * @param vdbName
     * @param vdbVersion
     * @param modelName
     * @param allowedTypes {@link EnumSet} Type of schema objects to retrieve, null means ALL the schema object types
     * @param typeNamePattern RegEx pattern to filter to names of tables, procedures that are being read. Null means no filter.
     */
    String getSchema(String vdbName, String vdbVersion, String modelName, EnumSet<SchemaObjectType> allowedTypes, String typeNamePattern) throws AdminException;

    /**
     * Get the Query Plan for the given session with provided execution id.
     * @param sessionId
     * @param executionId
     * @return
     */
    String getQueryPlan(String sessionId, long executionId) throws AdminException;

    /**
     * Restart the Server
     */
    void restart();

    /**
     * Set the profile name.  A null value will set the default profile name.
     * @param name
     */
    void setProfileName(String name);

    /**
     * Get the VDBs that are currently deployed in the system
     *
     * @param singleInstance
     * <br>
     * - <b>true</b> to return the VDB list from only a single server instance,
     * which is the same as {@link #getVDBs()}.
     * <br>
     * - <b>false</b> to get all VDBs from all servers in the domain
     * @return Collection of {@link VDB}s.
     * @throws AdminException
     */
    Collection<? extends VDB> getVDBs(boolean singleInstance) throws AdminException;

    /**
     * Deploy a .vdb or .zip vdb.  The deployment name will be taken from the url file name.
     *
     * @throws AdminException
     */
    void deployVDBZip(URL url) throws AdminException;
}