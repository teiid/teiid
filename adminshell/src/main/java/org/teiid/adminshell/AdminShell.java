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
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.ConnectionFactory;
import org.teiid.adminapi.ConnectionPoolStatistics;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.WorkerPoolStatistics;
import org.teiid.adminapi.Admin.DataSourceType;
import org.teiid.adminshell.Help.Doc;

import com.metamatrix.common.util.ReaderInputStream;
import com.metamatrix.core.util.ObjectConverterUtil;

/**
 * Contextual shell wrapper around the AdminAPI, see {@link Admin}
 */
public class AdminShell {
	
	protected static Logger log = Logger.getLogger(AdminShell.class.getName());
	
	static Properties p;
	private static int connectionCount = 1;
	private static Admin internalAdmin;
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
			System.out.println("Closing previous admin associated with " + connectionName);
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
	    	log.log(Level.WARNING, "Could not load default connection properties.", e);
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
	
	@Doc(text = "Add a ConnectionFactory")
	public static ConnectionFactory addConnectionFactory(
			@Doc(text = "deployed name") String deployedName,
			@Doc(text = "type name") String typeName, 
			Properties properties) throws AdminException {
		return getAdmin()
				.addConnectionFactory(deployedName, typeName, properties);
	}

	@Doc(text = "Adds a role to the specified policy")
	public static void addRoleToDataPolicy(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vdbVersion,
			@Doc(text = "policy name") String policyName, 
			@Doc(text = "role") String role) throws AdminException {
		getAdmin().addRoleToDataPolicy(vdbName, vdbVersion, policyName, role);
	}

	@Doc(text = "Assign a ConnectionFactory to a source Model")
	public static void assignConnectionFactoryToModel(
			@Doc(text = "vdb name") String vdbName,
			@Doc(text = "vdb version") int vdbVersion,
			@Doc(text = "model name") String modelName, 
			@Doc(text = "source name") String sourceName, 
			@Doc(text = "jndi name") String jndiName)
			throws AdminException {
		getAdmin().assignConnectionFactoryToModel(vdbName, vdbVersion, modelName,
				sourceName, jndiName);
	}

	@Doc(text = "Cancel a request")
	public static void cancelRequest(
			@Doc(text = "session id") String sessionId, 
			@Doc(text = "request id") long requestId)
			throws AdminException {
		getAdmin().cancelRequest(sessionId, requestId);
	}

	@Doc(text = "Clear the given cache")
	public static void clearCache(
			@Doc(text = "cache type") String cacheType) throws AdminException {
		getAdmin().clearCache(cacheType);
	}

	@Doc(text = "Delete a ConnectionFactory")
	public static void deleteConnectionFactory(
			@Doc(text = "deployed name") String deployedName)
			throws AdminException {
		getAdmin().deleteConnectionFactory(deployedName);
	}

	@Doc(text = "Delete a Connector")
	public static void deleteConnector(
			@Doc(text = "name") String name) throws AdminException {
		getAdmin().deleteConnector(name);
	}

	@Doc(text = "Delete a DataSource")
	public static void deleteDataSource(
			@Doc(text = "deployed name") String deploymentName) throws AdminException {
		getAdmin().deleteDataSource(deploymentName);
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

	@Doc(text = "Get all ConnectionFactory instances")
	public static Collection<ConnectionFactory> getConnectionFactories()
			throws AdminException {
		return getAdmin().getConnectionFactories();
	}

	@Doc(text = "Get all ConnectionFactory instances in the VDB")
	public static Collection<ConnectionFactory> getConnectionFactoriesInVDB(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vdbVersion) throws AdminException {
		return getAdmin().getConnectionFactoriesInVDB(vdbName, vdbVersion);
	}

	@Doc(text = "Get the specified ConnectionFactory")
	public static ConnectionFactory getConnectionFactory(
			@Doc(text = "deployed name") String deployedName)
			throws AdminException {
		return getAdmin().getConnectionFactory(deployedName);
	}

	@Doc(text = "Get the ConnectionPoolStatistics for the given ConnectionFactory")
	public static ConnectionPoolStatistics getConnectionFactoryStats(
			@Doc(text = "deployed name") String deployedName) throws AdminException {
		return getAdmin().getConnectionFactoryStats(deployedName);
	}

	@Doc(text = "Get all connector name Strings")
	public static Set<String> getConnectorNames() throws AdminException {
		return getAdmin().getConnectorNames();
	}

	@Doc(text = "Get all PropertyDefinition instances for the given connector")
	public static Collection<PropertyDefinition> getConnectorPropertyDefinitions(
			@Doc(text = "connector name") String connectorName) throws AdminException {
		return getAdmin().getConnectorPropertyDefinitions(connectorName);
	}

	@Doc(text = "Get all ProperyDefinition instances for a DataSource")
	public static Collection<PropertyDefinition> getDataSourcePropertyDefinitions()
			throws AdminException {
		return getAdmin().getDataSourcePropertyDefinitions();
	}

	@Doc(text = "Get the ProcessObject instances for the given identifier")
	public static Collection<ProcessObject> getProcesses(
			@Doc(text = "identifier") String processIdentifier)
			throws AdminException {
		return getAdmin().getProcesses(processIdentifier);
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

	@Doc(text = "Get WorkerPoolStatistics for the given WorkManager")
	public static WorkerPoolStatistics getWorkManagerStats(
			@Doc(text = "identifier") String identifier)
			throws AdminException {
		return getAdmin().getWorkManagerStats(identifier);
	}

	@Doc(text = "Remove a role for the data policy")
	public static void removeRoleFromDataPolicy(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vdbVersion,
			@Doc(text = "policy name") String policyName, 
			@Doc(text = "role name") String role) throws AdminException {
		getAdmin()
				.removeRoleFromDataPolicy(vdbName, vdbVersion, policyName, role);
	}

	@Doc(text = "Set a ConnectionFactory property")
	public static void setConnectionFactoryProperty(
			@Doc(text = "deployed name") String deployedName,
			@Doc(text = "propery name") String propertyName, 
			@Doc(text = "value") String propertyValue) throws AdminException {
		getAdmin().setConnectionFactoryProperty(deployedName, propertyName,
				propertyValue);
	}

	@Doc(text = "Set a runtime property")
	public static void setRuntimeProperty(
			@Doc(text = "name") String propertyName, 
			@Doc(text = "value") String propertyValue)
			throws AdminException {
		getAdmin().setRuntimeProperty(propertyName, propertyValue);
	}

	@Doc(text = "Start a ConnectionFactory")
	public static void startConnectionFactory(
			@Doc(text = "deployed name") String deployedName)
			throws AdminException {
		getAdmin().startConnectionFactory(deployedName);
	}

	@Doc(text = "Stop a ConnectionFactory")
	public static void stopConnectionFactory(
			@Doc(text = "deployed name") String deployedName)
			throws AdminException {
		getAdmin().stopConnectionFactory(deployedName);
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
	
	@Doc(text = "Checks if a ConnectionFactory exists")
	public static boolean hasConnectionFactory(
			@Doc(text = "deployed name") String factoryName) throws AdminException {
	    Collection<ConnectionFactory> bindings = getAdmin().getConnectionFactories();
	    
	    for (ConnectionFactory binding:bindings) {
	        if (binding.getName().equals(factoryName)) {
	            return true;
	        }        
	    }            
	    return false;
	}

	@Doc(text = "Checks if a Connector exists")
	public static boolean hasConnector(
			@Doc(text = "type name") String typeName) throws AdminException {
	    Collection<String> types = getAdmin().getConnectorNames();

	    for (String type:types) {
	        if (type.equals(typeName)) {
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

	@Doc(text = "Export a Connector RAR to file")
	public static void exportConnector(
			@Doc(text = "connector name") String name, 
			@Doc(text = "file name") String fileName) throws AdminException, IOException {
	    InputStream contents = getAdmin().exportConnector(name);
	    writeFile(name, fileName, contents);
	}

	@Doc(text = "Export a ConnectionFactory to an XML file")
	public static void exportConnectionFactory(
			@Doc(text = "deployed name") String deployedName, 
			@Doc(text = "file name") String fileName) throws AdminException, IOException{
	    Reader contents = getAdmin().exportConnectionFactory(deployedName);
	    writeFile(deployedName, fileName, contents);
	}

	private static void writeFile(String deployedName, String fileName,
			Reader contents) throws IOException, AdminProcessingException {
		if (contents == null) {
	    	throw new AdminProcessingException(deployedName + " not found for exporting");
	    }
    	ObjectConverterUtil.write(new ReaderInputStream(contents, Charset.forName("UTF-8")), fileName);	//$NON-NLS-1$
	}
	
	private static void writeFile(String deployedName, String fileName,
			InputStream contents) throws IOException, AdminProcessingException {
		if (contents == null) {
	    	throw new AdminProcessingException(deployedName + " not found for exporting");
	    }
		ObjectConverterUtil.write(contents, fileName);	
	}
	
	@Doc(text = "Add a connector from a RAR file")
	public static void addConnector(
			@Doc(text = "name of the Connector") String name, 
			@Doc(text = "RAR file name") String rarFile) throws FileNotFoundException, AdminException {
		FileInputStream fis = new FileInputStream(new File(rarFile));
		try {
			getAdmin().addConnector(name, fis);
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
			}
		}
	}
	
	@Doc(text = "Export a VDB to file")
	public static void exportVDB(
			@Doc(text = "vdb name") String vdbName, 
			@Doc(text = "vdb version") int vdbVersion, 
			@Doc(text = "file name") String fileName) throws AdminException, IOException{
	    InputStream contents = getAdmin().exportVDB(vdbName, vdbVersion);
	    writeFile(vdbName, fileName, contents);
	}
	
	@Doc(text = "Deploy a VDB from file")
	public static void deployVDB(
			@Doc(text = "file name") String vdbFile) throws AdminException, FileNotFoundException {
		FileInputStream fis = new FileInputStream(new File(vdbFile));
		try {
			getAdmin().deployVDB(vdbFile, fis);
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
			}
		}
	}

	@Doc(text = "Add a DataSource")
	public static void addDataSource(
			@Doc(text = "deployed name") String deploymentName, 
			DataSourceType type,
			Properties properties) throws AdminException {
		getAdmin().addDataSource(deploymentName, type, properties);
	}

	@Doc(text = "Export the DataSource XML to file")
	public static void exportDataSource(
			@Doc(text = "deployed name") String deployedName,
			@Doc(text = "file name") String fileName) throws AdminException, IOException {
		Reader contents = getAdmin().exportDataSource(deployedName);
		writeFile(deployedName, fileName, contents);
	}
	
	@Doc(text = "Get the current Admin connection")
	public static Admin getAdmin() {
		if (internalAdmin == null) {
	        throw new NullPointerException("Not connected.  You must call a \"connectAsAdmin\" method or choose an active connection via \"useConnection\".");
	    }
		return internalAdmin;
	}
	
	@Doc(text = "Disconnect the current connection for the server")
	public static void disconnect() {
	    if (internalAdmin != null) {
	    	internalAdmin.close();
	    	internalAdmin = null;
	    	currentName = null;
	    	connections.remove(currentName);
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
			System.out.println("Warning: connection is not active for " + name);
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
	
}
