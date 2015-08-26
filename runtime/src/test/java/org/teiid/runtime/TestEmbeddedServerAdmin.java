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
package org.teiid.runtime;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.adminapi.*;
import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.adminapi.Admin.TranlatorPropertyType;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.translator.file.FileExecutionFactory;

@SuppressWarnings("nls")
public class TestEmbeddedServerAdmin {
	
	private static Admin admin;
	private static EmbeddedServer server;
	
	@BeforeClass
	public static void init() throws Exception {
		server = new EmbeddedServer();
		EmbeddedConfiguration config = new EmbeddedConfiguration();
		server.start(config);
		
		FileExecutionFactory executionFactory = new FileExecutionFactory();
		executionFactory.start();
		server.addTranslator("file", executionFactory);
		
		FileManagedConnectionFactory managedconnectionFactory = new FileManagedConnectionFactory();
		managedconnectionFactory.setParentDirectory("src/test/resources");
		server.addConnectionFactory("java:/test-file", managedconnectionFactory.createConnectionFactory());
		
		server.deployVDB(new FileInputStream(new File("src/test/resources/adminapi-test-vdb.xml")));
//		admin = server.getAdmin();
		admin = EmbeddedAdminFactory.getInstance().createAdmin(server);
	}
	
	private Connection newSession() throws SQLException {
		return server.getDriver().connect("jdbc:teiid:AdminAPITestVDB", new Properties());
	}
	
	@Test
	public void testGetVdbs() throws AdminException {
		for(VDB vdb : admin.getVDBs()) {
			assertEquals(vdb.getName(), "AdminAPITestVDB");
			assertEquals(vdb.getVersion(), 1);
			assertEquals(vdb.getDescription(), "The adminapi test VDB");
			assertEquals(vdb.getModels().size(), 1);
		}
	}
	
	@Test
	public void testGetVDB() throws AdminException{
		VDB vdb = admin.getVDB("AdminAPITestVDB", 1);
		assertEquals(vdb.getDescription(), "The adminapi test VDB");
		assertEquals(vdb.getModels().size(), 1);
	}
	
	@Test
	public void testSource() throws AdminException {
		admin.addSource("AdminAPITestVDB", 1, "TestModel", "text-connector-test", "file", "java:/test-file");	
		
		for(VDB vdb : admin.getVDBs()){
			VDBMetaData vdbMetaData = (VDBMetaData) vdb;
			for (ModelMetaData m : vdbMetaData.getModelMetaDatas().values()) {
				SourceMappingMetadata mapping = m.getSourceMapping("text-connector-test");
				if (mapping != null){
					assertEquals(mapping.getConnectionJndiName(), "java:/test-file");
					assertEquals(mapping.getTranslatorName(), "file");
				}
			}
		}
		
		admin.updateSource("AdminAPITestVDB", 1, "text-connector-test", "mysql", "java:/test-jdbc");
		
		for(VDB vdb : admin.getVDBs()){
			VDBMetaData vdbMetaData = (VDBMetaData) vdb;
			for (ModelMetaData m : vdbMetaData.getModelMetaDatas().values()) {
				SourceMappingMetadata mapping = m.getSourceMapping("text-connector-test");
				if (mapping != null){
					assertEquals(mapping.getConnectionJndiName(), "java:/test-jdbc");
					assertEquals(mapping.getTranslatorName(), "mysql");
				}
			}
		}
		
		admin.removeSource("AdminAPITestVDB", 1, "TestModel", "text-connector-test");
	}
	
	@Test
	public void testChangeVDBConnectionType() throws AdminException {
		ConnectionType previous = admin.getVDB("AdminAPITestVDB", 1).getConnectionType();
		admin.changeVDBConnectionType("AdminAPITestVDB", 1, ConnectionType.ANY);
		assertEquals(ConnectionType.ANY, admin.getVDB("AdminAPITestVDB", 1).getConnectionType());
		admin.changeVDBConnectionType("AdminAPITestVDB", 1, previous);
	}
	
	@Test
	public void testDeployUndeploy() throws AdminException, FileNotFoundException {
		admin.undeploy("AdminAPITestVDB");
		assertNull(admin.getVDB("AdminAPITestVDB", 1));
		admin.deploy("AdminAPITestVDB-vdb.xml", new FileInputStream(new File("src/test/resources/adminapi-test-vdb.xml")));
		assertNotNull(admin.getVDB("AdminAPITestVDB", 1));
	}
	
	@Test
	public void testRestartVDB() throws AdminException{
		admin.restartVDB("AdminAPITestVDB", 1, "TestModel");
		assertNotNull(admin.getVDB("AdminAPITestVDB", 1));
	}
	
	@Test
	public void testGetTranslator() throws AdminException {
		
		for(Translator translator : admin.getTranslators()){
			assertEquals("file", translator.getName());
			assertEquals("File Translator, reads contents of files or writes to them", translator.getDescription());
			assertEquals("false", translator.getProperties().getProperty("supportsOuterJoins"));
		}
		
		Translator translator = admin.getTranslator("file");
		assertEquals("File Translator, reads contents of files or writes to them", translator.getDescription());
		assertEquals("false", translator.getProperties().getProperty("supportsOuterJoins"));
	}
	
	@Test
	public void testGetWorkerPoolStats() throws AdminException{
		for(WorkerPoolStatistics pool : admin.getWorkerPoolStats()){
			assertEquals("QueryProcessorQueue", pool.getQueueName());
			assertEquals(64, pool.getMaxThreads());
		}
	}
	
	@Test
	public void testGetCacheTypes() throws AdminException{
		Set<String> cacheTypes = (Set<String>) admin.getCacheTypes();
		assertTrue(cacheTypes.contains("PREPARED_PLAN_CACHE"));
		assertTrue(cacheTypes.contains("QUERY_SERVICE_RESULT_SET_CACHE"));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetSessions() throws AdminException, SQLException {
		Connection conn = newSession();
		List<Session> sessions = (List<Session>) admin.getSessions();
		assertEquals(1, sessions.size());
		assertEquals("AdminAPITestVDB", sessions.get(0).getVDBName());
		assertEquals(1, sessions.get(0).getVDBVersion());
		assertEquals("JDBC", sessions.get(0).getApplicationName());
		assertNotNull(sessions.get(0).getSessionId());
		conn.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetRequests() throws AdminException, SQLException {
		Connection conn = newSession();
		Statement stmt = conn.createStatement();
		String command = "SELECT * FROM helloworld" ;
		ResultSet rs = stmt.executeQuery(command);
		List<RequestMetadata> requests = (List<RequestMetadata>) admin.getRequests();
		assertEquals(1, requests.size());
		assertEquals(command, requests.get(0).getCommand());
		assertNotNull(requests.get(0).getSessionId());		
		rs.close();
		stmt.close();
		conn.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetRequest() throws AdminException, SQLException {
		Connection conn = newSession();
		Statement stmt = conn.createStatement();
		String command = "SELECT * FROM helloworld" ;
		ResultSet rs = stmt.executeQuery(command);
		List<Session> sessions = (List<Session>) admin.getSessions();
		String id = sessions.get(0).getSessionId();
		List<Request> requests = (List<Request>) admin.getRequestsForSession(id);
		assertEquals(1, requests.size());
		assertEquals(command, requests.get(0).getCommand());
		assertEquals(id, requests.get(0).getSessionId());
		rs.close();
		stmt.close();
		conn.close();
	}
	
	@Test(expected = AdminProcessingException.class)
	public void testGetTemplatePropertyDefinitions() throws AdminException {
		admin.getTemplatePropertyDefinitions("file");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetTranslatorPropertyDefinitions() throws AdminException {
		List<PropertyDefinition> list = (List<PropertyDefinition>) admin.getTranslatorPropertyDefinitions("file", TranlatorPropertyType.OVERRIDE);
		assertEquals(19, list.size());
		
		list = (List<PropertyDefinition>) admin.getTranslatorPropertyDefinitions("file", TranlatorPropertyType.ALL);
        assertEquals(19, list.size());		
	}
	
	@Test
	public void testGetTransactions() throws AdminException, SQLException {

		Connection conn = newSession();
		conn.setAutoCommit(false);
		assertEquals(0, admin.getTransactions().size());
		conn.commit();		
		conn.close();
	}
	
	public void testClearCache() throws AdminException{
		admin.clearCache("PREPARED_PLAN_CACHE");
		admin.clearCache("QUERY_SERVICE_RESULT_SET_CACHE");
		admin.clearCache("PREPARED_PLAN_CACHE", "AdminAPITestVDB", 1);
		admin.clearCache("QUERY_SERVICE_RESULT_SET_CACHE", "AdminAPITestVDB", 1);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetCacheStats() throws AdminException {
		List<CacheStatistics> list = (List<CacheStatistics>) admin.getCacheStats("PREPARED_PLAN_CACHE");
		assertEquals(list.get(0).getName(), Admin.Cache.PREPARED_PLAN_CACHE.name());
		list = (List<CacheStatistics>) admin.getCacheStats("QUERY_SERVICE_RESULT_SET_CACHE");
		assertEquals(list.get(0).getName(), Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetEngineStats() throws AdminException, SQLException {
		Connection conn1 = newSession();
		Connection conn2 = newSession();
		List<EngineStatistics> list = (List<EngineStatistics>) admin.getEngineStats();
		assertEquals(2, list.get(0).getSessionCount());
		conn1.close();
		conn2.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testTerminateSession() throws AdminException, SQLException {
		Connection conn = newSession();
		List<Session> sessions = (List<Session>) admin.getSessions();
		String sessionId = sessions.get(0).getSessionId();
		admin.terminateSession(sessionId);
		assertEquals(0, admin.getSessions().size());
		conn.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testCancelRequest() throws AdminException, SQLException {
		Connection conn = newSession();
		Statement stmt = conn.createStatement();
		String command = "SELECT * FROM helloworld" ;
		ResultSet rs = stmt.executeQuery(command);
		List<Session> sessions = (List<Session>) admin.getSessions();
		String id = sessions.get(0).getSessionId();
		List<Request> requests = (List<Request>) admin.getRequestsForSession(id);
		long executionId = requests.get(0).getExecutionId();
		assertEquals(1, admin.getRequests().size());
		admin.cancelRequest(id, executionId);
		assertEquals(0, admin.getRequests().size());
		rs.close();
		stmt.close();
		conn.close();
	}
	
	@Ignore("This test need enable DataRole Configuration in 'adminapi-test-vdb.xml'")
	@Test
	public void testDataRoleMapping() throws AdminException {
		
		String vdbName = "AdminAPITestVDB";
		int vdbVersion = 1;
		String policyName = "TestDataRole";
		DataPolicyMetadata policy = getPolicy(admin.getVDB(vdbName, vdbVersion), policyName);
		assertEquals(1, policy.getMappedRoleNames().size());
		
		admin.addDataRoleMapping(vdbName, vdbVersion, policyName, "test-role-name");
		policy = getPolicy(admin.getVDB(vdbName, vdbVersion), policyName);
		assertEquals(2, policy.getMappedRoleNames().size());
		
		admin.removeDataRoleMapping(vdbName, vdbVersion, policyName, "test-role-name");
		policy = getPolicy(admin.getVDB(vdbName, vdbVersion), policyName);
		assertEquals(1, policy.getMappedRoleNames().size());
		
		
		boolean previous = policy.isAnyAuthenticated();
		admin.setAnyAuthenticatedForDataRole(vdbName, vdbVersion, policyName, !previous);
		policy = getPolicy(admin.getVDB(vdbName, vdbVersion), policyName);
		assertEquals(!previous, policy.isAnyAuthenticated());
		admin.setAnyAuthenticatedForDataRole(vdbName, vdbVersion, policyName, previous);
	}
	
	private DataPolicyMetadata getPolicy(VDB vdb, String policyName) {
		VDBMetaData vdbMetaData = (VDBMetaData) vdb;
		return vdbMetaData.getDataPolicyMap().get(policyName);
	}	
	
	@Test
	public void testTerminateTransaction() throws AdminException {
		// need enhance
		admin.terminateTransaction("xid");
	}
	
	@Test(expected = AdminProcessingException.class)
	public void testDataSources() throws AdminException{
		admin.createDataSource("", "", new Properties());
		admin.getDataSource("");
		admin.deleteDataSource("");
		admin.getDataSourceNames();
		admin.getDataSourceTemplateNames();
		admin.markDataSourceAvailable("");
	}
	
	@Test
	public void testGetSchema() throws AdminException {
		String expected = "CREATE VIEW helloworld (\n" + 
						  "	expr1 string\n" +
						  ")\n"  +
						  "AS\n" +
						  "SELECT 'HELLO WORLD';";
		EnumSet<SchemaObjectType> allowedTypes = EnumSet.of(Admin.SchemaObjectType.TABLES);
		String schema = admin.getSchema("AdminAPITestVDB", 1, "TestModel",  allowedTypes, "helloworld");
		assertEquals(expected, schema);
		schema = admin.getSchema("AdminAPITestVDB", 1, "TestModel",  null, null);
		assertEquals(expected, schema);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetQueryPlan() throws SQLException, AdminException {
				
		Connection conn = newSession();
		Statement stmt = conn.createStatement();
		stmt.execute("set showplan on");
		String command = "SELECT * FROM helloworld" ;
		ResultSet rs = stmt.executeQuery(command);
		List<Session> sessions = (List<Session>) admin.getSessions();
		String sessionId = sessions.get(0).getSessionId();
		List<Request> requests = (List<Request>) admin.getRequestsForSession(sessionId);
		int executionId = (int) requests.get(0).getExecutionId();
		String plan = admin.getQueryPlan(sessionId, executionId);
		assertNotNull(plan);
		rs.close();
		stmt.close();
		conn.close();
	}
	
	@AfterClass
	public static void destory() throws SQLException {
		admin.close();
		server.stop();
	}

}
