package org.teiid.adminapi.jboss;


import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.ConnectionFactory;
import org.teiid.adminapi.ConnectionPoolStatistics;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.WorkerPoolStatistics;

import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.util.UnitTestUtil;

public class TestConnectorBindings extends BaseConnection {
	static ServerDatasourceConnection ds;
	static Admin admin;
	
	@Before
	public void setUp() throws Exception {
		//if (!Bootstrap.getInstance().isStarted()) Bootstrap.getInstance().bootstrap();
		ds = new ServerDatasourceConnection();
		//admin = AdminProvider.getRemote( "jnp://localhost:1099", "javaduke", "anotherduke");	
		admin = AdminFactory.getInstance().createAdmin("admin", "admin".toCharArray(), "mm://localhost:31443"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		installVDB();
	}
	
	@After
	public void tearDown() {
		admin.close();
	}
	
	//@AfterClass
	public static void end() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("admin", "admin".toCharArray(), "mm://localhost:31443"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

		VDB vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		if (vdb != null) {
			admin.deleteVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		}
		admin.close();
	}

	@Test public void testConnectorBinding() throws Exception {
		ConnectionFactory binding = admin.getConnectionFactory("test-mysql-cb"); //$NON-NLS-1$
		
		if (binding != null) {
			admin.deleteConnectionFactory("test-mysql-cb"); //$NON-NLS-1$
		}
		
		Properties p = new Properties();
		p.setProperty("jndi-name", "test-mysql-cb"); //$NON-NLS-1$ //$NON-NLS-2$
		p.setProperty("rar-name", "connector-jdbc-7.0.0-SNAPSHOT.rar"); //$NON-NLS-1$ //$NON-NLS-2$
		p.setProperty("CapabilitiesClass", "org.teiid.connector.jdbc.derby.DerbyCapabilities"); //$NON-NLS-1$ //$NON-NLS-2$
		p.setProperty("XaCapable", "true"); //$NON-NLS-1$ //$NON-NLS-2$
		p.setProperty("SourceJNDIName", "java:DerbyDS"); //$NON-NLS-1$ //$NON-NLS-2$
		admin.addConnectionFactory("test-mysql-cb","connector-jdbc-7.0.0-SNAPSHOT", p);	 //$NON-NLS-1$ //$NON-NLS-2$
		
		binding = admin.getConnectionFactory("test-mysql-cb"); //$NON-NLS-1$
		
		assertNotNull(binding);	
		
		assertEquals("java:DerbyDS", binding.getPropertyValue("SourceJNDIName")); //$NON-NLS-1$ //$NON-NLS-2$
		
		admin.stopConnectionFactory("test-mysql-cb");
		
		admin.startConnectionFactory("test-mysql-cb");
		
		admin.setConnectionFactoryProperty("test-mysql-cb", "SourceJNDIName", "DummyDS"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		binding = admin.getConnectionFactory("test-mysql-cb"); //$NON-NLS-1$
		
		assertEquals("DummyDS", binding.getPropertyValue("SourceJNDIName")); //$NON-NLS-1$ //$NON-NLS-2$
		
		admin.deleteConnectionFactory("test-mysql-cb"); //$NON-NLS-1$
		
		binding = admin.getConnectionFactory("test-mysql-cb"); //$NON-NLS-1$
		
		assertNull(binding);		
	}
	
	@Test public void testVDBDeploy() throws Exception {
		
		VDB vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		if (vdb != null) {
			admin.deleteVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		}
		
		installVDB();
		
		assertNotNull(admin.getVDB("TransactionsRevisited", 1)); //$NON-NLS-1$
		
		Set<VDB> vdbs = admin.getVDBs();
		assertTrue(vdbs.size() >= 1);
		
		Collection<ConnectionFactory> bindings = admin.getConnectionFactoriesInVDB("TransactionsRevisited",1); //$NON-NLS-1$
		assertEquals(2, bindings.size());
		
		admin.deleteVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		
		assertNull(admin.getVDB("TransactionsRevisited", 1)); //$NON-NLS-1$
	}
	
	
	@Test public void testSessions() throws Exception{
		Connection c = ds.getConnection("TransactionsRevisited");
		Collection<Session> sessions = admin.getSessions();
		
		int size = sessions.size();
		assertTrue( size >= 1);
		
		Session found = null;
		for (Session s: sessions) {
			if (s.getUserName().equals("admin@teiid-security")) {
				found = s;
				break;
			}
		}
		
		assertNotNull(found);
		
		admin.terminateSession(found.getSessionId());
		
		sessions = admin.getSessions();
		assertTrue(sessions.size() == (size-1));
	}
	
	
	@Test public void testRequests() throws Exception {
		Runnable work = new Runnable() {
			public void run() {
				for (int i = 0; i < 5; i++) {
					try {
						execute(ds, "TransactionsRevisited", "select * from pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		};
		Thread t = new Thread(work);
		t.start();
		
		// can not really stop and take reading here..
		try {
			Collection<Request> requests = admin.getRequests();
			assertTrue(requests.size() >= 0);
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
		}
	}
	
	@Test
	public void testCache() throws Exception {
		Collection<String> caches = admin.getCacheTypes();
		assertEquals(4, caches.size());
		
		admin.clearCache("CODE_TABLE_CACHE"); //$NON-NLS-1$
	}
	
	@Ignore
	@Test
	public void testTransactions() throws Exception {
		Runnable work = new Runnable() {
			public void run() {
				try {
					for (int i = 0; i < 10; i++) {
						int v = i+200;
						execute(ds, "TransactionsRevisited", "insert into vm.g1 (pm1e1, pm1e2, pm2e1, pm2e2) values("+v+",'"+v+"',"+v+",'"+v+"')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
					}
					execute(ds, "TransactionsRevisited", "delete from vm.g1 where pm1e1 >= 200"); //$NON-NLS-1$ //$NON-NLS-2$
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
		}
	}

	private static void installVDB() throws AdminException, FileNotFoundException {
		VDB vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		if (vdb == null) {
			File f = UnitTestUtil.getTestDataFile("TransactionsRevisited.vdb"); //$NON-NLS-1$
			FileInputStream fis = new FileInputStream(f);
			admin.deployVDB(f.getName(), fis);
			try {
				fis.close();
			} catch (IOException e) {
			}
		}
	}
	
	@Test
	public void testWorkmanagerStats() throws Exception {
		WorkerPoolStatistics stats = admin.getWorkManagerStats("runtime"); //$NON-NLS-1$
		System.out.println(stats);
		assertNotNull(stats);
	}
	
	@Test
	public void testConnectionPool() throws Exception {
		ConnectionPoolStatistics stats = admin.getConnectionFactoryStats("mysql-connector-binding"); //$NON-NLS-1$
		System.out.println(stats);
		assertNotNull(stats);
	}
	
	@Test
	public void testConnectorTypeProperties() throws Exception {
		Collection<PropertyDefinition> defs = admin.getConnectorPropertyDefinitions("connector-jdbc-7.0.0-SNAPSHOT"); //$NON-NLS-1$
		for (PropertyDefinition pd:defs) {
			System.out.println(pd.getName());
			if (pd.getName().equals("ExtensionTranslationClassName")) { //$NON-NLS-1$
				assertEquals("Extension SQL Translation Class", pd.getDisplayName()); //$NON-NLS-1$
				assertEquals(true, pd.isAdvanced());
				assertEquals(true, pd.isRequired());
				assertEquals(false, pd.isMasked());
				assertEquals(true, pd.isModifiable());
				
				HashSet<String> values = new HashSet<String>();
				values.add("org.teiid.connector.jdbc.h2.H2Translator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.sqlserver.SqlServerSQLTranslator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.mysql.MySQL5Translator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.derby.DerbySQLTranslator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.postgresql.PostgreSQLTranslator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.db2.DB2SQLTranslator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.access.AccessSQLTranslator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.mysql.MySQLTranslator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.translator.Translator"); //$NON-NLS-1$
				values.add("org.teiid.connector.jdbc.oracle.OracleSQLTranslator"); //$NON-NLS-1$
				assertEquals(values, pd.getAllowedValues());
			}
		}
	}
	
	@Test
	public void testConnectorTypes() throws Exception {
		Set<String> defs = admin.getConnectorNames();
		assertTrue(defs.contains("connector-salesforce-7.0.0-SNAPSHOT")); //$NON-NLS-1$
		assertTrue(defs.contains("connector-jdbc-7.0.0-SNAPSHOT")); //$NON-NLS-1$
		assertTrue(defs.contains("connector-text-7.0.0-SNAPSHOT")); //$NON-NLS-1$
		assertTrue(defs.contains("connector-loopback-7.0.0-SNAPSHOT")); //$NON-NLS-1$
		assertTrue(defs.contains("connector-ldap-7.0.0-SNAPSHOT")); //$NON-NLS-1$
		System.out.println(defs);
	}
	
	@Test
	public void testPropertyDefsForDS() throws Exception {
		Collection<PropertyDefinition> defs = admin.getDataSourcePropertyDefinitions();		
		System.out.println(defs);
		assertNotNull(defs);
		assertTrue(defs.size() > 1);
	}
	
	@Test
	public void testTemplate() throws Exception{
		File f = new File(UnitTestUtil.getTestDataPath()+"/connector-loopback.rar"); //$NON-NLS-1$
		FileInputStream fis = new FileInputStream(f);
		admin.addConnector("connector-loopy", fis); //$NON-NLS-1$
		fis.close();
		
		Set<String> names = admin.getConnectorNames();
		assertTrue(names.contains("connector-loopy")); //$NON-NLS-1$
		
		admin.deleteConnector("connector-loopy"); //$NON-NLS-1$
		
		names = admin.getConnectorNames();
		//assertTrue(!names.contains("connector-loopy")); //$NON-NLS-1$
	}
	
	@Test
	public void testExportVDB() throws Exception{
		File f = new File(UnitTestUtil.getTestScratchPath()+"/TransactionsRevisited.vdb"); //$NON-NLS-1$
		
		assertTrue(!f.exists());
		
		InputStream contents = admin.exportVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		if (contents != null) {
			ObjectConverterUtil.write(contents, f.getCanonicalPath());
		}
		
		assertTrue(f.exists());
		f.delete();
	}	
	
	@Test public void testAssignConnectorBinding() throws Exception {
		admin.assignConnectionFactoryToModel("TransactionsRevisited", 1, "pm1", "mysql", "jndi:FOO"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		
		boolean checked = false;
		VDB vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		List<Model> models = vdb.getModels();
		for (Model model:models) {
			if (model.getName().equals("pm1")) { //$NON-NLS-1$
				List<String> sources = model.getSourceNames();
				for (String source:sources) {
					if (source.equals("mysql")) { //$NON-NLS-1$
						assertEquals("jndi:FOO", model.getSourceJndiName(source)); //$NON-NLS-1$
						checked = true;
					}
				}
			}
		}
		
		assertTrue("Test not veryfied", checked); //$NON-NLS-1$
	}
	
	
	@Test public void testAddRoleNames() throws Exception {
		installVDB();
		admin.addRoleToDataPolicy("TransactionsRevisited", 1, "policy1", "managers"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		
		VDB vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		List<DataPolicy> policies = vdb.getDataPolicies();
		assertTrue (!policies.isEmpty());
		for (DataPolicy policy:policies) {
			if (policy.getName().equals("policy1")) { //$NON-NLS-1$
				List<String> sources = policy.getMappedRoleNames();
				assertTrue(sources.contains("managers"));
			}
		}
		
		// remove the role
		admin.removeRoleFromDataPolicy("TransactionsRevisited", 1, "policy1", "managers");
		
		vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		policies = vdb.getDataPolicies();
		assertTrue (!policies.isEmpty());
		
		for (DataPolicy policy:policies) {
			if (policy.getName().equals("policy1")) { //$NON-NLS-1$
				List<String> sources = policy.getMappedRoleNames();
				assertFalse(sources.contains("managers"));
			}
		}		
		
		// remove non-existent role name
		admin.removeRoleFromDataPolicy("TransactionsRevisited", 1, "policy1", "FOO");
	}	
	
	@Test public void testExportConnectionFactory() throws Exception {
		ObjectConverterUtil.write(admin.exportConnectionFactory("products-cf"), "cf.xml");
	}	
	
	@Test public void testExportDataSource() throws Exception {
		ObjectConverterUtil.write(admin.exportDataSource("CustomersDS"), "ds.xml");
	}
}
