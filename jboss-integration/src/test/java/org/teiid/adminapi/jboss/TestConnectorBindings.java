package org.teiid.adminapi.jboss;


import static junit.framework.Assert.*;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.ConnectionPoolStatistics;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.WorkerPoolStatistics;

import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.util.UnitTestUtil;

@Ignore
public class TestConnectorBindings extends BaseConnection {
	ServerDatasourceConnection ds;
	Admin admin;
	
	@Before
	public void setUp() throws Exception {
		//if (!Bootstrap.getInstance().isStarted()) Bootstrap.getInstance().bootstrap();
		ds = new ServerDatasourceConnection();
		//admin = AdminProvider.getRemote( "jnp://localhost:1099", "javaduke", "anotherduke");	
		admin = AdminFactory.getInstance().createAdmin("admin", "admin".toCharArray(), "mm://localhost:31443");
	}
	
	@After
	public void tearDown() throws Exception {
	}

	@Test public void testConnectorBinding() throws Exception {
		ConnectorBinding binding = admin.getConnectorBinding("test-mysql-cb");
		
		assertNull(binding);
		
		Properties p = new Properties();
		p.setProperty("jndi-name", "test-mysql-cb");
		p.setProperty("rar-name", "connector-jdbc-7.0.0-SNAPSHOT.rar");
		p.setProperty("CapabilitiesClass", "org.teiid.connector.jdbc.derby.DerbyCapabilities");
		p.setProperty("XaCapable", "true");
		p.setProperty("SourceJNDIName", "java:DerbyDS");
		admin.addConnectorBinding("test-mysql-cb","connector-jdbc-7.0.0-SNAPSHOT", p);	
		
		binding = admin.getConnectorBinding("test-mysql-cb");
		
		assertNotNull(binding);	
		
		assertEquals("java:DerbyDS", binding.getPropertyValue("SourceJNDIName"));
		
		admin.stopConnectorBinding(binding);
		
		admin.startConnectorBinding(binding);
		
		admin.setConnectorBindingProperty("test-mysql-cb", "SourceJNDIName", "DummyDS");
		
		binding = admin.getConnectorBinding("test-mysql-cb");
		
		assertEquals("DummyDS", binding.getPropertyValue("SourceJNDIName"));
		
		admin.deleteConnectorBinding("test-mysql-cb");
		
		binding = admin.getConnectorBinding("test-mysql-cb");
		
		assertNull(binding);		
	}
	
	@Test public void testVDBDeploy() throws Exception {
		
		VDB vdb = admin.getVDB("TransactionsRevisited", 1);
		if (vdb != null) {
			admin.deleteVDB("TransactionsRevisited", 1);
		}
		
		assertNull(admin.getVDB("TransactionsRevisited", 1));
		
		File f = UnitTestUtil.getTestDataFile("TransactionsRevisited.vdb");

		admin.deployVDB(f.getName(), f.toURI().toURL());
		
		assertNotNull(admin.getVDB("TransactionsRevisited", 1));
		
		Set<VDB> vdbs = admin.getVDBs();
		assertTrue(vdbs.size() >= 1);
		
		Collection<ConnectorBinding> bindings = admin.getConnectorBindingsInVDB("TransactionsRevisited",1);
		assertEquals(2, bindings.size());
		
		admin.deleteVDB("TransactionsRevisited", 1);
		
		assertNull(admin.getVDB("TransactionsRevisited", 1));
	}
	
	
	@Test public void testSessions() throws Exception{
		Collection<Session> sessions = admin.getSessions();
		System.out.println(sessions);
		assertTrue(sessions.size() >= 1);
		for (Session s: sessions) {
			assertEquals("ramesh@teiid-security", s.getUserName());
		}
		
		for (Session s:sessions) {
			admin.terminateSession(s.getSessionId());
		}
		
		sessions = admin.getSessions();
		assertTrue(sessions.size() == 0);
	}
	
	
	@Test public void testRequests() throws Exception {
		
		VDB vdb = admin.getVDB("TransactionsRevisited", 1);
		if (vdb == null) {
			File f = UnitTestUtil.getTestDataFile("TransactionsRevisited.vdb");
			admin.deployVDB(f.getName(), f.toURI().toURL());
		}
		
		Runnable work = new Runnable() {
			public void run() {
				for (int i = 0; i < 5; i++) {
					try {
						execute(ds, "TransactionsRevisited", "select * from pm1.g1");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		};
		Thread t = new Thread(work);
		t.start();
		
		try {
			Collection<Request> requests = admin.getRequests();
			assertTrue(requests.size() > 0);
		}finally {
			t.join();
		}
		
		long sessionId = 0;
		Collection<Session> sessions = admin.getSessions();
		for (Session s:sessions) {
			sessionId = s.getSessionId();
		}		
		
		t = new Thread(work);
		t.start();
		
		try {
			Collection<Request> requests = admin.getRequestsForSession(sessionId);
			assertTrue(requests.size() >= 0);	
		} finally {
			t.join();
			admin.deleteVDB("TransactionsRevisited", 1);
		}
	}
	
	@Test
	public void testCache() throws Exception {
		Collection<String> caches = admin.getCacheTypes();
		assertEquals(4, caches.size());
		
		admin.clearCache("CODE_TABLE_CACHE");
	}
	
	@Test
	public void testTransactions() throws Exception {
		
		VDB vdb = admin.getVDB("TransactionsRevisited", 1);
		if (vdb == null) {
			File f = UnitTestUtil.getTestDataFile("TransactionsRevisited.vdb");
			admin.deployVDB(f.getName(), f.toURI().toURL());
		}
		
		Runnable work = new Runnable() {
			public void run() {
				try {
					for (int i = 0; i < 10; i++) {
						int v = i+200;
						execute(ds, "TransactionsRevisited", "insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values("+v+",'"+v+"',"+v+",'"+v+"')");
					}
					execute(ds, "TransactionsRevisited", "delete from vm.g1 where pm1e1 >= 200");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		
		Thread t = new Thread(work);
		t.start();
		Thread.sleep(2000);
		try {
			Collection<Transaction> txns = admin.getTransactions();
			assertTrue(txns.size() >= 0);
		} finally {
			t.join();
			admin.deleteVDB("TransactionsRevisited", 1);
		}
	}
	
	@Test
	public void testWorkmanagerStats() throws Exception {
		WorkerPoolStatistics stats = admin.getWorkManagerStats("runtime");
		System.out.println(stats);
	}
	
	@Test
	public void testConnectionPool() throws Exception {
		ConnectionPoolStatistics stats = admin.getConnectorConnectionPoolStats("mysql-connector-binding");
		System.out.println(stats);
	}
	
	@Test
	public void testConnectorTypeProperties() throws Exception {
		Collection<PropertyDefinition> defs = admin.getConnectorTypePropertyDefinitions("connector-jdbc-7.0.0-SNAPSHOT");
		for (PropertyDefinition pd:defs) {
			System.out.println(pd.getName());
			if (pd.getName().equals("ExtensionTranslationClassName")) {
				assertEquals("Extension SQL Translation Class", pd.getDisplayName());
				assertEquals(true, pd.isAdvanced());
				assertEquals(true, pd.isRequired());
				assertEquals(false, pd.isMasked());
				assertEquals(true, pd.isModifiable());
				
				HashSet<String> values = new HashSet<String>();
				values.add("org.teiid.connector.jdbc.h2.H2Translator");
				values.add("org.teiid.connector.jdbc.sqlserver.SqlServerSQLTranslator");
				values.add("org.teiid.connector.jdbc.mysql.MySQL5Translator");
				values.add("org.teiid.connector.jdbc.derby.DerbySQLTranslator");
				values.add("org.teiid.connector.jdbc.postgresql.PostgreSQLTranslator");
				values.add("org.teiid.connector.jdbc.db2.DB2SQLTranslator");
				values.add("org.teiid.connector.jdbc.access.AccessSQLTranslator");
				values.add("org.teiid.connector.jdbc.mysql.MySQLTranslator");
				values.add("org.teiid.connector.jdbc.translator.Translator");
				values.add("org.teiid.connector.jdbc.oracle.OracleSQLTranslator");
				assertEquals(values, pd.getAllowedValues());
			}
		}
	}
	
	@Test
	public void testConnectorTypes() throws Exception {
		Set<String> defs = admin.getConnectorTypes();
		assertTrue(defs.contains("connector-salesforce-7.0.0-SNAPSHOT"));
		assertTrue(defs.contains("connector-jdbc-7.0.0-SNAPSHOT"));
		assertTrue(defs.contains("connector-text-7.0.0-SNAPSHOT"));
		assertTrue(defs.contains("connector-loopback-7.0.0-SNAPSHOT"));
		assertTrue(defs.contains("connector-ldap-7.0.0-SNAPSHOT"));
		System.out.println(defs);
	}
	
	@Test
	public void testPropertyDefsForDS() throws Exception {
		Collection<PropertyDefinition> defs = admin.getDataSourcePropertyDefinitions();		
		System.out.println(defs);
	}
	
	@Test
	public void testTemplate() throws Exception{
		File f = new File(UnitTestUtil.getTestDataPath()+"/connector-loopback.rar");
		admin.addConnectorType("connector-loopy", f.toURI().toURL());
		
		Set<String> names = admin.getConnectorTypes();
		assertTrue(names.contains("connector-loopy"));
		
		admin.deleteConnectorType("connector-loopy");
		
		names = admin.getConnectorTypes();
		assertTrue(!names.contains("connector-loopy"));
	}
	
	@Test
	public void testExportVDB() throws Exception{
		File f = new File(UnitTestUtil.getTestScratchPath()+"/Admin.vdb");
		
		assertTrue(!f.exists());
		
		InputStream in = admin.exportVDB("Admin", 1);
		if (in != null) {
			ObjectConverterUtil.write(in, f);
		}
		
		assertTrue(f.exists());
		f.delete();
	}	
	
	@Test public void testAssignConnectorBinding() throws Exception {
		
		VDB vdb = admin.getVDB("TransactionsRevisited", 1);
		if (vdb == null) {
			admin.deleteVDB("TransactionsRevisited", 1);
			File f = UnitTestUtil.getTestDataFile("TransactionsRevisited.vdb");
			admin.deployVDB(f.getName(), f.toURI().toURL());
		}
		
		admin.assignBindingsToModel("TransactionsRevisited", 1, "pm1", new String[] {"java:foo", "java:bar"});
	}
}
