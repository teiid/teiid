/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General public static
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General public static License for more details.
 * 
 * You should have received a copy of the GNU Lesser General public static
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.CacheStatistics;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.WorkerPoolStatistics;
import org.teiid.adminapi.VDB.ConnectionType;
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
			@Doc(text = "url - URL in the format \"mm[s]://host:port\"") String url,
			@Doc(text = "username") String username,
			@Doc(text = "password") String password, 
			@Doc(text = "connection name") String connectionName) throws AdminException {
		internalAdmin = AdminFactory.getInstance().createAdmin(username, password.toCharArray(), url);
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
		connectAsAdmin(p.getProperty("admin.url", "mm://localhost:31443"), p.getProperty("admin.user", "admin"), 
				p.getProperty("admin.password", "admin"), "conn-" + connectionCount++);
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
			@Doc(text = "vdb version") int vdbVersion,
			@Doc(text = "dataRole name") String policyName, 
			@Doc(text = "mapped role name") String role) throws AdminException {
		getAdmin().addDataRoleMapping(vdbName, vdbVersion, policyName, role);
	}

	@Doc(text = "Assign a translator and data source to a source Model")
	public static void assignToModel(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") int vdbVersion,
			@Doc(text = "model name") String modelName, 
			@Doc(text = "source name") String sourceName,
			@Doc(text = "translator name") String translatorName,
			@Doc(text = "jndi name") String jndiName)
			throws AdminException {
		getAdmin().assignToModel(vdbName, vdbVersion, modelName, sourceName, translatorName, jndiName);
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
			@Doc(text = "vdb version") int vdbVersion			
			) throws AdminException {
		getAdmin().clearCache(cacheType, vdbName, vdbVersion);
	}	

	@Doc(text = "Delete a VDB")
	public static void deleteVDB(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vdbVersion) throws AdminException {
		getAdmin().deleteVDB(vdbName, vdbVersion);
	}

	@Doc(text = "Get all cache type Strings")
	public static Collection<String> getCacheTypes() throws AdminException {
		return getAdmin().getCacheTypes();
	}
	
	@Doc(text = "Change a VDB Connection Type")
	public static void changeVDBConnectionType(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vdbVersion,
			@Doc(text = "Connection Type (NONE, BY_VERSION, or ANY") String type)
			throws AdminException {
		getAdmin().changeVDBConnectionType(vdbName, vdbVersion, ConnectionType.valueOf(type));
	}

	@Doc(text = "Get all translator instances")
	public static Collection<Translator> getTranslators()
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
	public static Collection<PropertyDefinition> getTemplatePropertyDefinitions(
			@Doc(text = "template name") String templateName) throws AdminException {
		return getAdmin().getTemplatePropertyDefinitions(templateName);
	}

	@Doc(text = "Get all Request instances")
	public static Collection<Request> getRequests() throws AdminException {
		return getAdmin().getRequests();
	}

	@Doc(text = "Get all Request instances for the given session")
	public static Collection<Request> getRequestsForSession(
			@Doc(text = "session id") String sessionId)
			throws AdminException {
		return getAdmin().getRequestsForSession(sessionId);
	}

	@Doc(text = "Get all Session instances")
	public static Collection<Session> getSessions() throws AdminException {
		return getAdmin().getSessions();
	}

	@Doc(text = "Get all Transaction instances")
	public static Collection<Transaction> getTransactions() throws AdminException {
		return getAdmin().getTransactions();
	}

	@Doc(text = "Get a specific VDB")
	public static VDB getVDB(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vbdVersion) throws AdminException {
		return getAdmin().getVDB(vdbName, vbdVersion);
	}

	@Doc(text = "Get all VDB instances")
	public static Set<VDB> getVDBs() throws AdminException {
		return getAdmin().getVDBs();
	}

	@Doc(text = "Get thread pool statistics for Teiid")
	public static WorkerPoolStatistics getWorkerPoolStats()
			throws AdminException {
		return getAdmin().getWorkerPoolStats();
	}
	
	@Doc(text = "Get cache statistics for given cache type")
	public static CacheStatistics getCacheStats(@Doc(text = "cacheType") String identifier)
			throws AdminException {
		return getAdmin().getCacheStats(identifier);
	}
	
	@Doc(text = "Remove a mapped role for the data role")
	public static void removeDataRoleMapping(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vdbVersion,
			@Doc(text = "dataRole name") String policyName, 
			@Doc(text = "mapped role name") String role) throws AdminException {
		getAdmin()
				.removeDataRoleMapping(vdbName, vdbVersion, policyName, role);
	}

	@Doc(text = "Set the any authenticated flag for the data role")
    public static void setAnyAuthenticatedForDataRole(
    		@Doc(text = "vdb name")String vdbName, 
    		@Doc(text = "vdb version")int vdbVersion, 
    		@Doc(text = "dataRole name")String dataRole, 
    		@Doc(text = "any authenticated") boolean anyAuthenticated) throws AdminException {
    	getAdmin().setAnyAuthenticatedForDataRole(vdbName, vdbVersion, dataRole, anyAuthenticated);
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
	
	@Doc(text = "Merge two vdbs")
	public static void mergeVDBs(
			@Doc(text = "source vdb name") String sourceVDBName, 
			@Doc(text = "source vdb version") int sourceVDBVersion,
			@Doc(text = "target vdb name") String targetVDBName, 
			@Doc(text = "target vdb version") int targetVDBVersion) throws AdminException {
		getAdmin().mergeVDBs(sourceVDBName, sourceVDBVersion, targetVDBName, targetVDBVersion);
	}
	
	@Doc(text = "Checks if a translator exists")
	public static boolean hasTranslator(@Doc(text = "deployed name") String factoryName) throws AdminException {
	    Collection<Translator> bindings = getAdmin().getTranslators();
	    
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
	    Collection<VDB> vdbs = getAdmin().getVDBs();
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
			@Doc(text = "vdb version") int version) throws AdminException {
	    Collection<VDB> vdbs = getAdmin().getVDBs();
	    for (VDB vdb:vdbs) {
	        if (vdb.getName().equals(vdbName) && vdb.getVersion() == version) {
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
	
	@Doc(text = "Deploy a VDB from file")
	public static void deployVDB(
			@Doc(text = "file name") String vdbFile) throws AdminException, FileNotFoundException {
		File file = new File(vdbFile);
		FileInputStream fis = new FileInputStream(file);
		try {
			getAdmin().deployVDB(file.getName(), fis);
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

	@Doc(text = "Available data source template names")
	public static Set<String> getDataSourceTemplateNames() throws AdminException{
		return getAdmin().getDataSourceTemplateNames();
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
	
	@Doc(text = "Tell the engine that the given source is available. Pending dynamic vdb metadata loads will be resumed.")
	public static void markDataSourceAvailable(
			@Doc(text = "jndi name") String name) throws AdminException {
		getAdmin().markDataSourceAvailable(name);
	}
	
}
