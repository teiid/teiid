package org.teiid.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.resource.ResourceException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.TranlatorPropertyType;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.WorkerPoolStatistics;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.file.FileExecutionFactory;

@SuppressWarnings("nls")
public class TestEmbeddedServerAdmin {
	
	private static Admin admin;
	private static EmbeddedServer server;
	private static Connection conn;
	
	@BeforeClass
	public static void init() throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, FileNotFoundException, IOException, ResourceException, SQLException {
		server = new EmbeddedServer();
		server.start(new EmbeddedConfiguration());
		
		FileExecutionFactory executionFactory = new FileExecutionFactory();
		executionFactory.start();
		server.addTranslator("file", executionFactory);
		
		FileManagedConnectionFactory managedconnectionFactory = new FileManagedConnectionFactory();
		managedconnectionFactory.setParentDirectory("src/test/resources");
		server.addConnectionFactory("java:/test-file", managedconnectionFactory.createConnectionFactory());
		
		server.deployVDB(new FileInputStream(new File("src/test/resources/adminapi-test-vdb.xml")));
		admin = server.getAdmin();
		conn = server.getDriver().connect("jdbc:teiid:AdminAPITestVDB", new Properties());
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
		admin.deploy("AdminAPITestVDB", new FileInputStream(new File("src/test/resources/adminapi-test-vdb.xml")));
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
	
	@Test
	public void testGetSessions() throws AdminException {
		@SuppressWarnings("unchecked")
		List<Session> sessions = (List<Session>) admin.getSessions();
		assertEquals(1, sessions.size());
		assertEquals("AdminAPITestVDB", sessions.get(0).getVDBName());
		assertEquals(1, sessions.get(0).getVDBVersion());
		assertEquals("JDBC", sessions.get(0).getApplicationName());
		assertNotNull(sessions.get(0).getSessionId());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetRequests() throws AdminException, SQLException {
		Connection conn = server.getDriver().connect("jdbc:teiid:AdminAPITestVDB", new Properties());
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
		Connection conn = server.getDriver().connect("jdbc:teiid:AdminAPITestVDB", new Properties());
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
	}
	
	@AfterClass
	public static void destory() throws SQLException {
		conn.close();
		admin.close();
		server.stop();
	}
	
	public static void main(String[] args) throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, FileNotFoundException, IOException, AdminException, ResourceException, SQLException {
		
		init();
		
		System.out.println();
		
		System.out.println(admin.getSessions());
		
		destory();
	}

}
