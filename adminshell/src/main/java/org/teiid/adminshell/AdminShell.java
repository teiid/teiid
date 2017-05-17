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

package org.teiid.adminshell;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.adminapi.*;
import org.teiid.adminapi.Admin.TranlatorPropertyType;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.adminshell.Help.Doc;


/**
 * Contextual shell wrapper around the AdminAPI, see {@link Admin}
 */
public class AdminShell {

	protected static Logger log = Logger.getLogger(AdminShell.class.getName());

	static Properties p;
	private static int connectionCount = 1;
	static Admin internalAdmin;
	private static String currentName;
	private static HashMap<String, Admin> connections = new HashMap<String, Admin>();
	private static Help help = new Help(AdminShell.class);

	@Doc(text="Get a named Admin connection to the specified server")
	public static void connectAsAdmin(
			@Doc(text = "host - hostname") String host,
			@Doc(text = "port - port") int port,
			@Doc(text = "username") String username,
			@Doc(text = "password") String password,
			@Doc(text = "connection name") String connectionName) throws AdminException {
		Admin admin = AdminFactory.getInstance().createAdmin(host, port, username, password.toCharArray());
		if (admin == null) {
		    throw new AdminProcessingException("Connection was not successful"); //$NON-NLS-1$
		}
		internalAdmin = admin;
		currentName = connectionName;
		Admin old = connections.put(connectionName, internalAdmin);
		if (old != null) {
			System.out.println("Closing previous admin associated with " + connectionName); //$NON-NLS-1$
			old.close();
		}
	}

	@Doc(text = "Connect as Admin using the defaults from connection.properties")
	@SuppressWarnings("nls")
	public static void connectAsAdmin() throws AdminException {
		loadConnectionProperties();
		connectAsAdmin(p.getProperty("admin.host", "localhost"), Integer.parseInt(p.getProperty("admin.port", "9990")), p.getProperty("admin.user", "admin"), p.getProperty("admin.password", "admin"), "conn-" + connectionCount++);
	}

	static void loadConnectionProperties() {
		if (p != null) {
			return;
		}
	    Properties props = new Properties();
	    FileInputStream fis = null;
	    try {
		    fis = new FileInputStream("connection.properties"); //$NON-NLS-1$
	    	props.load(fis);
	    } catch (IOException e) {
	    	log.log(Level.WARNING, "Could not load default connection properties.", e); //$NON-NLS-1$
	    } finally {
	    	if (fis != null) {
	    		try {
					fis.close();
				} catch (IOException e) {
				}
	    	}
	    }
	    p = props;
	}

	@Doc(text = "Adds a mapped role to the specified data role")
	public static void addDataRoleMapping(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "dataRole name") String policyName,
			@Doc(text = "mapped role name") String role) throws AdminException {
		getAdmin().addDataRoleMapping(vdbName, vdbVersion!=null?vdbVersion.toString():null, policyName, role);
	}

	@Doc(text = "Update a translator and data source for a given source")
	public static void updateSource(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "source name") String sourceName,
			@Doc(text = "translator name") String translatorName,
			@Doc(text = "jndi name") String jndiName)
			throws AdminException {
		getAdmin().updateSource(vdbName, vdbVersion!=null?vdbVersion.toString():null, sourceName, translatorName, jndiName);
	}
	
	@Doc(text = "Add a source to a model")
	public static void addSource(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "model name") String modelName,
			@Doc(text = "source name") String sourceName,
			@Doc(text = "translator name") String translatorName,
			@Doc(text = "jndi name") String jndiName)
			throws AdminException {
		getAdmin().addSource(vdbName, vdbVersion!=null?vdbVersion.toString():null, modelName, sourceName, translatorName, jndiName);
	}
	
	@Doc(text = "Remove a source from a model")
	public static void removeSource(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "model name") String modelName,
			@Doc(text = "source name") String sourceName)
			throws AdminException {
		getAdmin().removeSource(vdbName, vdbVersion!=null?vdbVersion.toString():null, modelName, sourceName);
	}

	@Doc(text = "Cancel a request")
	public static void cancelRequest(
			@Doc(text = "session id") String sessionId,
			@Doc(text = "execution id") long executionId)
			throws AdminException {
		getAdmin().cancelRequest(sessionId, executionId);
	}

	@Doc(text = "Clear the given cache")
	public static void clearCache(
			@Doc(text = "cache type") String cacheType) throws AdminException {
		getAdmin().clearCache(cacheType);
	}

	@Doc(text = "Clear the given cache for a VDB")
	public static void clearCache(
			@Doc(text = "cache type") String cacheType, @Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion
			) throws AdminException {
		getAdmin().clearCache(cacheType, vdbName, vdbVersion!=null?vdbVersion.toString():null);
	}

	@Doc(text = "Undeploy a artifact (JAR, RAR, VDB)")
	public static void undeploy(@Doc(text = "deployed name") String deployedName) throws AdminException {
		getAdmin().undeploy(deployedName);
	}

	@Doc(text = "Get all cache type Strings")
	public static Collection<String> getCacheTypes() throws AdminException {
		return getAdmin().getCacheTypes();
	}

	@Doc(text = "Change a VDB Connection Type")
	public static void changeVDBConnectionType(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "Connection Type (NONE, BY_VERSION, or ANY") String type)
			throws AdminException {
		getAdmin().changeVDBConnectionType(vdbName, vdbVersion!=null?vdbVersion.toString():null, ConnectionType.valueOf(type));
	}

	@Doc(text = "Get all translator instances")
	public static Collection<? extends Translator> getTranslators()
			throws AdminException {
		return getAdmin().getTranslators();
	}

	@Doc(text = "Get the specified ConnectionFactory")
	public static Translator getTranslator(
			@Doc(text = "deployed name") String deployedName)
			throws AdminException {
		return getAdmin().getTranslator(deployedName);
	}

	@Doc(text = "Get all PropertyDefinitions for the given template")
	public static Collection<? extends PropertyDefinition> getTemplatePropertyDefinitions(
			@Doc(text = "template name") String templateName) throws AdminException {
		return getAdmin().getTemplatePropertyDefinitions(templateName);
	}
	
	@Doc(text = "Get all PropertyDefinitions for the given translator")
	public static Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(
			@Doc(text = "translator name") String translatorName) throws AdminException {
		return getAdmin().getTranslatorPropertyDefinitions(translatorName);
    }
	
    @Doc(text = "Get all PropertyDefinitions for the given translator")
    public static Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(
            @Doc(text = "translator name") String translatorName,  @Doc(text = "type of property IMPPORT, OVERRIDE, EXTENSION_METADATA")String type) throws AdminException {
        return getAdmin().getTranslatorPropertyDefinitions(translatorName, TranlatorPropertyType.valueOf(type.toUpperCase()));
    }

	@Doc(text = "Get all Request instances")
	public static Collection<? extends Request> getRequests() throws AdminException {
		return getAdmin().getRequests();
	}

	@Doc(text = "Get all Request instances for the given session")
	public static Collection<? extends Request> getRequestsForSession(
			@Doc(text = "session id") String sessionId)
			throws AdminException {
		return getAdmin().getRequestsForSession(sessionId);
	}

	@Doc(text = "Get all Session instances")
	public static Collection<? extends Session> getSessions() throws AdminException {
		return getAdmin().getSessions();
	}

	@Doc(text = "Get all Transaction instances")
	public static Collection<? extends Transaction> getTransactions() throws AdminException {
		return getAdmin().getTransactions();
	}

	@Doc(text = "Get a specific VDB")
	public static VDB getVDB(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion) throws AdminException {
		return getAdmin().getVDB(vdbName, vdbVersion!=null?vdbVersion.toString():null);
	}

	@Doc(text = "Get all VDB instances")
	public static Collection<? extends VDB> getVDBs() throws AdminException {
		return getAdmin().getVDBs();
	}

	@Doc(text = "Get thread pool statistics for Teiid")
	public static Collection<? extends WorkerPoolStatistics> getWorkerPoolStats()
			throws AdminException {
		return getAdmin().getWorkerPoolStats();
	}

	@Doc(text = "Get cache statistics for given cache type")
	public static Collection<? extends CacheStatistics> getCacheStats(@Doc(text = "cacheType") String identifier)
			throws AdminException {
		return getAdmin().getCacheStats(identifier);
	}

	@Doc(text = "Get engine statistics for Teiid")
	public static Collection<? extends EngineStatistics> getEngineStats()
			throws AdminException {
		return getAdmin().getEngineStats();
	}

	@Doc(text = "Remove a mapped role for the data role")
	public static void removeDataRoleMapping(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "dataRole name") String policyName,
			@Doc(text = "mapped role name") String role) throws AdminException {
		getAdmin()
				.removeDataRoleMapping(vdbName, vdbVersion!=null?vdbVersion.toString():null, policyName, role);
	}

	@Doc(text = "Set the any authenticated flag for the data role")
    public static void setAnyAuthenticatedForDataRole(
    		@Doc(text = "vdb name")String vdbName,
    		@Doc(text = "vdb version")Object vdbVersion,
    		@Doc(text = "dataRole name")String dataRole,
    		@Doc(text = "any authenticated") boolean anyAuthenticated) throws AdminException {
    	getAdmin().setAnyAuthenticatedForDataRole(vdbName, vdbVersion!=null?vdbVersion.toString():null, dataRole, anyAuthenticated);
    }

	@Doc(text = "Terminate a session and associated requests")
	public static void terminateSession(
			@Doc(text = "session id") String sessionId) throws AdminException {
		getAdmin().terminateSession(sessionId);
	}

	@Doc(text = "Terminate a transaction")
	public static void terminateTransaction(
			@Doc(text = "transaction id") String transactionId)
			throws AdminException {
		getAdmin().terminateTransaction(transactionId);
	}

	@Doc(text = "Checks if a translator exists")
	public static boolean hasTranslator(@Doc(text = "deployed name") String factoryName) throws AdminException {
	    Collection<? extends Translator> bindings = getAdmin().getTranslators();

	    for (Translator binding:bindings) {
	        if (binding.getName().equals(factoryName)) {
	            return true;
	        }
	    }
	    return false;
	}

	@Doc(text = "Checks if a VDB exists")
	public static boolean hasVDB(
			@Doc(text = "vdb name") String vdbName) throws AdminException {
	    Collection<? extends VDB> vdbs = getAdmin().getVDBs();
	    for (VDB vdb:vdbs) {
	        if (vdb.getName().equals(vdbName)) {
	            return true;
	        }
	    }
	    return false;
	}

	@Doc(text = "Checks if a specific VDB version exists")
	public static boolean hasVDB(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion) throws AdminException {
	    Collection<? extends VDB> vdbs = getAdmin().getVDBs();
	    for (VDB vdb:vdbs) {
	        if (vdb.getName().equals(vdbName) && vdb.getVersion().equals(vdbVersion)) {
	            return true;
	        }
	    }
	    return false;
	}

	/*private static void writeFile(String deployedName, String fileName,
			InputStream contents) throws IOException, AdminProcessingException {
		if (contents == null) {
	    	throw new AdminProcessingException(deployedName + " not found for exporting");//$NON-NLS-1$
	    }
		ObjectConverterUtil.write(contents, fileName);
	}*/

	@Doc(text = "Deploy a Artifact (JAR, RAR, VDB) from file")
	public static void deploy(@Doc(text = "file name") String vdbFile) throws AdminException, FileNotFoundException {
		File file = new File(vdbFile);
		FileInputStream fis = new FileInputStream(file);
		try {
			getAdmin().deploy(file.getName(), fis);
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
			}
		}
	}
	@Doc(text = "Create a data source from supplied properties")
	public static void createDataSource(@Doc(text = "deployed name")String deploymentName, @Doc(text = "template name")String templateName, @Doc(text = "properties")Properties properties) throws AdminException {
		getAdmin().createDataSource(deploymentName, templateName, properties);
	}

	@Doc(text = "Delete data source")
	public static void deleteDataSource(@Doc(text = "deployed name")String deployedName) throws AdminException{
		getAdmin().deleteDataSource(deployedName);
	}

	@Doc(text = "Available data sources")
	public static Collection<String> getDataSourceNames() throws AdminException{
		return getAdmin().getDataSourceNames();
	}

	public static Properties getDataSource(@Doc(text = "Data Source Name")String deployedName) throws AdminException {
		return getAdmin().getDataSource(deployedName);
	}

	@Doc(text = "Available data source template names")
	public static Set<String> getDataSourceTemplateNames() throws AdminException{
		return getAdmin().getDataSourceTemplateNames();
	}

	@Doc(text = "Restart the VDB")
	public static void restartVDB(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "models") String... models)
			throws AdminException {
		getAdmin().restartVDB(vdbName, vdbVersion!=null?vdbVersion.toString():null, models);
	}

	@Doc(text = "Get query execution plan for the given execution id")
	public static String getQueryPlan(
			@Doc(text = "Session Id") String sessionId,
			@Doc(text = "Execution Id") int executionId)
			throws AdminException {
		return getAdmin().getQueryPlan(sessionId, executionId);
	}

	@Doc(text = "Get schema for the model")
	public static String getSchema(@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") Object vdbVersion,
			@Doc(text = "models") String modelName) throws AdminException {
		return getAdmin().getSchema(vdbName, vdbVersion!=null?vdbVersion.toString():null, modelName, null, null);
	}
	
	@Doc(text = "Restart the server")
    public static void restart(){
    	getAdmin().restart();
    }

	@Doc(text = "Get the current org.teiid.adminapi.Admin instance for direct use. Note: Used for advanced usecases to bypass AdminShell methods")
	public static Admin getAdmin() {
		if (internalAdmin == null) {
	        throw new NullPointerException("Not connected.  You must call a \"connectAsAdmin\" method or choose an active connection via \"useConnection\"."); //$NON-NLS-1$
	    }
		return internalAdmin;
	}

	@Doc(text = "Disconnect the current connection for the server")
	public static void disconnect() {
	    if (internalAdmin != null) {
	    	internalAdmin.close();
	    	internalAdmin = null;
	    	connections.remove(currentName);
	    	currentName = null;
	    }
	}

	@Doc(text = "Disconnect all connections from the server")
	public static void disconnectAll() {
		for (Admin admin : connections.values()) {
			admin.close();
		}
		connections.clear();
		internalAdmin = null;
		currentName = null;
	}

	@Doc(text = "Use another connection")
	public static void useConnection(
			@Doc(text = "connection name") String name) {
		Admin admin = connections.get(name);
		if (admin == null) {
			System.out.println("Warning: connection is not active for " + name); //$NON-NLS-1$
			return;
		}
		internalAdmin = admin;
		currentName = name;
	}

	@Doc(text = "Returns the current connection name")
	public static String getConnectionName() {
	    return currentName;
	}

	@Doc(text = "Return all connection names")
	public static Collection<String> getAllConnections() {
	    return connections.keySet();
	}

	@Doc(text = "Show help for all admin methods")
	public static void adminHelp() {
		help.help();
	}

	@Doc(text = "Show help for the given admin method")
	public static void adminHelp(
			@Doc(text = "method name") String method) {
		help.help(method);
	}
	
	@Doc(text = "Set the profile name for domain mode")
    public static void setProfileName(
            @Doc(text = "profile name") String profileName) {
        getAdmin().setProfileName(profileName);
    }
}
