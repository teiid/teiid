package org.teiid.adminapi.jboss;


import static junit.framework.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.util.Collection;
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
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.core.util.UnitTestUtil;

@Ignore
@SuppressWarnings("nls")
public class TestConnectorBindings extends BaseConnection {
	static ServerDatasourceConnection ds;
	static Admin admin;
	private static final String VERSION = "-7.0.0-SNAPSHOT"; 
	
	@Before
	public void setUp() throws Exception {
		//if (!Bootstrap.getInstance().isStarted()) Bootstrap.getInstance().bootstrap();
		ds = new ServerDatasourceConnection();
		//admin = AdminProvider.getRemote( "jnp://localhost:1099", "javaduke", "anotherduke");	
		admin = AdminFactory.getInstance().createAdmin("admin", "admin".toCharArray(), "mms://127.0.0.1:31443"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
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
	
	@Test public void testVDBDeploy() throws Exception {
		
		VDB vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		if (vdb != null) {
			admin.deleteVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		}
		
		installVDB();
		
		assertNotNull(admin.getVDB("TransactionsRevisited", 1)); //$NON-NLS-1$
		
		Set<VDB> vdbs = admin.getVDBs();
		assertTrue(vdbs.size() >= 1);
		
		admin.deleteVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		
		assertNull(admin.getVDB("TransactionsRevisited", 1)); //$NON-NLS-1$
	}
	
	@Test
	public void testChangeConnectionType() throws Exception {
		VDB vdb = admin.getVDB("TransactionsRevisited", 1);
		assertNotNull(vdb); //$NON-NLS-1$
		
		ConnectionType ct = vdb.getConnectionType();
		assertEquals(ConnectionType.BY_VERSION, ct);
		
		admin.changeVDBConnectionType("TransactionsRevisited", 1, ConnectionType.ANY);
		
		vdb = admin.getVDB("TransactionsRevisited", 1);
		
		ct = vdb.getConnectionType();
		assertEquals(ConnectionType.ANY, ct);		
	}

	@Test public void testGetVDB() throws Exception {
		VDB vdb = admin.getVDB("TransactionsRevisited", 1);
		assertNotNull(vdb); //$NON-NLS-1$
		
		assertEquals("TransactionsRevisited", vdb.getName());
		assertEquals(1, vdb.getVersion());
		assertEquals("A VDB to test transactions", vdb.getDescription());
		//assertEquals("sample-value", vdb.getPropertyValue("sample"));
		assertEquals(VDB.Status.INACTIVE, vdb.getStatus());

		// test model
		List<Model> models = vdb.getModels();
		assertEquals(4, models.size());
		Model model = null;
		for (Model m:models) {
			if (m.getName().equals("pm1")) {
				model = m;
				break;
			}
		}
		assertNotNull(model);
		assertEquals(Model.Type.PHYSICAL, model.getModelType());
		assertEquals("sample-value", model.getPropertyValue("sample"));
		List<String> sourceNames = model.getSourceNames();
		assertEquals(1, sourceNames.size());
		assertEquals("mysql", model.getSourceTranslatorName(sourceNames.get(0)));
		assertEquals("java:mysql-connector-binding", model.getSourceConnectionJndiName(sourceNames.get(0)));
		assertTrue(model.isSource());
		//assertTrue(model.isSupportsMultiSourceBindings());
		assertTrue(model.isVisible());
		
		// test data policies
		List<DataPolicy> policies = vdb.getDataPolicies();
		assertEquals(1, policies.size());
		assertEquals("policy1", policies.get(0).getName());
		assertEquals("roleOne described", policies.get(0).getDescription());
		
		List<DataPolicy.DataPermission> permissions = policies.get(0).getPermissions();
		assertEquals(2, permissions.size());
		
		for(DataPolicy.DataPermission permission: permissions) {
			if (permission.getResourceName().equals("myTable.T1")) {
				assertTrue(permission.getAllowRead());
				assertFalse(permission.getAllowCreate());
				assertFalse(permission.getAllowDelete());
				assertFalse(permission.getAllowUpdate());				
			}
			else if (permission.getResourceName().equals("myTable.T2")) {
				assertFalse(permission.getAllowRead());
				assertFalse(permission.getAllowCreate());
				assertTrue(permission.getAllowDelete());
				assertFalse(permission.getAllowUpdate());
			}
			else {
				fail("there are only two types of permissions");
			}
		}
		
	}
	
	
	@Test public void testSessions() throws Exception{
		Connection c = ds.getConnection("TransactionsRevisited"); // to create the session
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
		c.close();
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
		
		String sessionId = null;
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
		assertEquals(3, caches.size());
		
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
	public void testTranslatorTemplateProperties() throws Exception {
		Collection<PropertyDefinition> defs = admin.getTemplatePropertyDefinitions("translator-jdbc"+VERSION); //$NON-NLS-1$
		for (PropertyDefinition pd:defs) {
			System.out.println(pd.getName()+":"+pd.getPropertyTypeClassName()+":"+pd.getDefaultValue());
			if (pd.getName().equals("ExtensionTranslationClassName")) { //$NON-NLS-1$
				assertEquals("Extension SQL Translation Class", pd.getDisplayName()); //$NON-NLS-1$
				assertEquals(false, pd.isAdvanced());
				assertEquals(true, pd.isRequired());
				assertEquals(false, pd.isMasked());
				assertEquals(true, pd.isModifiable());
				
				assertEquals(12, pd.getAllowedValues().size());
				System.out.println(pd.getAllowedValues());
			}
		}
	}
	
	@Test
	public void testGetTemplate() throws Exception {
		Translator translator = admin.getTranslator("oracle");
		for (String key:translator.getProperties().stringPropertyNames()) {
			System.out.println(key+"="+translator.getPropertyValue(key));
		}
		assertEquals("org.teiid.translator.jdbc.oracle.OracleSQLTranslator", translator.getPropertyValue("ExtensionTranslationClassName")); //$NON-NLS-1$
		assertEquals(false, translator.getPropertyValue("XaCapable"));
	}
	
	@Test public void testAssignConnectorBinding() throws Exception {
		admin.assignToModel("TransactionsRevisited", 1, "pm1", "mysql", "mysql", "jndi:FOO"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
		
		boolean checked = false;
		VDB vdb = admin.getVDB("TransactionsRevisited", 1); //$NON-NLS-1$
		List<Model> models = vdb.getModels();
		for (Model model:models) {
			if (model.getName().equals("pm1")) { //$NON-NLS-1$
				List<String> sources = model.getSourceNames();
				for (String source:sources) {
					if (source.equals("mysql")) { //$NON-NLS-1$
						assertEquals("jndi:FOO", model.getSourceConnectionJndiName(source)); //$NON-NLS-1$
						checked = true;
					}
				}
			}
		}
		
		assertTrue("Test not veryfied", checked); //$NON-NLS-1$
	}
	
	
	@Test public void testAddRoleNames() throws Exception {
		installVDB();
		admin.addDataRoleMapping("TransactionsRevisited", 1, "policy1", "managers"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		
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
		admin.removeDataRoleMapping("TransactionsRevisited", 1, "policy1", "managers");
		
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
		admin.removeDataRoleMapping("TransactionsRevisited", 1, "policy1", "FOO");
	}	
	
	@Test
	public void testCreateDataSource() throws Exception {
		
		Properties p = new Properties();
		p.setProperty("DatabaseName", "txntest");
		p.setProperty("PortNumber", "3306");
		p.setProperty("ServerName", "localhost");
		p.setProperty("additional-ds-properties", "foo=bar, t= x");
		p.setProperty("user-name", "rareddy");
		p.setProperty("password", "mm");
		p.setProperty("xa-datasource-class", "com.mysql.jdbc.jdbc2.optional.MysqlXADataSource");
		
		admin.createDataSource("test-mysql","connector-jdbc-xa", p);	 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test
	public void testCreateDriverSource() throws Exception {
		
		Properties p = new Properties();
		p.setProperty("connection-url", "jdbc:mysql://localhost:3306/txntest");
		p.setProperty("additional-connection-properties", "foo=bar, t= x");
		p.setProperty("user-name", "rareddy");
		p.setProperty("password", "mm");
		p.setProperty("driver-class", "com.mysql.jdbc.Driver");
		
		admin.createDataSource("test-mysql-driver","connector-jdbc", p);	 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
}
