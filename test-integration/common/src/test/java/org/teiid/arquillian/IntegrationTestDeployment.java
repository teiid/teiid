package org.teiid.arquillian;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ArchivePaths;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.ByteArrayAsset;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestDeployment {

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("localhost", 9999,	"admin", "admin".toCharArray());
	}
	
	@After
	public void teardown() {
		admin.close();
	}
	
	@Test
	public void testVDBDeployment() throws Exception {
		try {
			Collection<?> vdbs = admin.getVDBs();
			assertTrue(vdbs.isEmpty());
			
			Collection<String> dsNames = admin.getDataSourceNames();
			if (dsNames.contains("Oracle11_PushDS")) {
				admin.deleteDataSource("Oracle11_PushDS");
			}
			
			admin.deploy("bqt.vdb",new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));

			vdbs = admin.getVDBs();
			assertFalse(vdbs.isEmpty());

			VDB vdb = admin.getVDB("bqt", 1);
			assertFalse(vdb.isValid());
			assertTrue(vdb.getStatus().equals(Status.INACTIVE));

			Properties props = new Properties();
			props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
			props.setProperty("user-name", "sa");
			props.setProperty("password", "sa");
			
			admin.createDataSource("Oracle11_PushDS", "h2", props);
			Thread.sleep(2000);
			vdb = admin.getVDB("bqt", 1);
			assertTrue(vdb.isValid());
			assertTrue(vdb.getStatus().equals(Status.ACTIVE));
			
			dsNames = admin.getDataSourceNames();
			assertTrue(dsNames.contains("Oracle11_PushDS"));

			admin.deleteDataSource("Oracle11_PushDS");
			vdb = admin.getVDB("bqt", 1);
			assertFalse(vdb.isValid());
			assertTrue(vdb.getStatus().equals(Status.INACTIVE));
		} finally {
			undeploy();
		}
	}

	@Test
	public void testTraslators() throws Exception {
		Collection<? extends Translator> translators = admin.getTranslators();
		System.out.println(translators);
		assertEquals(29, translators.size());

		JavaArchive jar = getLoopyArchive();
		
		try {
			admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());
			
			VDBTranslatorMetaData t = (VDBTranslatorMetaData)admin.getTranslator("loopy");
			assertNotNull(t);
			assertEquals("ANY", t.getPropertyValue("SupportedJoinCriteria"));
			assertEquals("true", t.getPropertyValue("supportsSelectDistinct"));
		} finally {
			admin.undeploy("loopy.jar");	
		}

		VDBTranslatorMetaData t = (VDBTranslatorMetaData)admin.getTranslator("orcl");
		assertNull(t);
	}

	private JavaArchive getLoopyArchive() {
		JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "loopy.jar")
			      .addClasses(SampleExecutionFactory.class)
			      .addAsManifestResource(new ByteArrayAsset(SampleExecutionFactory.class.getName().getBytes()),
			            ArchivePaths.create("services/org.teiid.translator.ExecutionFactory"));
		jar.addAsManifestResource(new ByteArrayAsset("Dependencies: org.jboss.teiid.translator.loopback\n".getBytes()),
	            ArchivePaths.create("MANIFEST.MF"));
		return jar;
	}

	@Test
	public void testVDBConnectionType() throws Exception {
		try {
			
			admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));			
			
			VDB vdb = admin.getVDB("bqt", 1);
			Model model = vdb.getModels().get(0);
			admin.assignToModel("bqt", 1, model.getName(), "Source", "h2", "java:jboss/datasources/ExampleDS");
			assertEquals(ConnectionType.BY_VERSION, vdb.getConnectionType());
			
			try {
				Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
				conn.close();
			} catch (Exception e) {
				fail("must have succeeded in connection");
			}
			
			admin.changeVDBConnectionType("bqt", 1, ConnectionType.NONE);

			try {
				TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
				fail("should have failed to connect as no new connections allowed");
			} catch (Exception e) {
				//pass
			}

			admin.deploy("bqt2.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt2.vdb")));
			admin.assignToModel("bqt", 2, model.getName(), "Source", "h2", "java:jboss/datasources/ExampleDS");
			
			try {
				Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
				conn.close();
			} catch (Exception e) {
				fail("should not have failed to connect");
			}
			
			admin.changeVDBConnectionType("bqt", 2, ConnectionType.ANY);
			try {
				Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
				conn.close();
			} catch (Exception e) {
				fail("should have connected to the second vdb");
			}
			
			vdb = admin.getVDB("bqt", 2);
			model = vdb.getModels().get(0);
			assertEquals(model.getSourceConnectionJndiName("Source"), "java:jboss/datasources/ExampleDS");
			assertEquals(model.getSourceTranslatorName("Source"), "h2");
			assertEquals(ConnectionType.ANY, vdb.getConnectionType());
			
		} finally {
			undeploy();
			if(admin.getVDB("bqt", 2) != null){
				admin.undeploy("bqt2.vdb");
			}
		}
	}
	
	@Test
	public void testCacheTypes() throws Exception {
		String[] array = {Admin.Cache.PREPARED_PLAN_CACHE.toString(), Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.toString()};
		Collection<String> types = admin.getCacheTypes();
		assertArrayEquals(array, types.toArray());
	}
	
	@Test
	public void testSessions() throws Exception {
		try {
			deployVdb();

			Collection<? extends Session> sessions = admin.getSessions();
			assertEquals (0, sessions.size());
			
			try {
				Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user;ApplicationName=test", null);
				sessions = admin.getSessions();
				assertEquals (1, sessions.size());
				Session s = sessions.iterator().next();
				
				assertEquals("user@teiid-security", s.getUserName());
				assertEquals("test", s.getApplicationName());
				assertEquals("bqt", s.getVDBName());
				assertEquals(1, s.getVDBVersion());
				assertNotNull(s.getSessionId());
				
				conn.close();
			} catch (Exception e) {
				fail("should have connected to the vdb");
			}
			
			try {
				Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user;ApplicationName=test", null);
				sessions = admin.getSessions();
				assertEquals (1, sessions.size());
				Session s = sessions.iterator().next();

				admin.terminateSession(s.getSessionId());
				sessions = admin.getSessions();
				assertEquals (0, sessions.size());			
				conn.close();
			} catch (Exception e) {
				fail("should have connected to the vdb");
			}			
			
		} finally {
			undeploy();
		}
	}

	private boolean deployVdb() throws AdminException, FileNotFoundException {
		boolean vdbOneDeployed;
		admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));			
		vdbOneDeployed = true;
		
		VDB vdb = admin.getVDB("bqt", 1);
		Model model = vdb.getModels().get(0);
		admin.assignToModel("bqt", 1, model.getName(), "Source", "h2", "java:jboss/datasources/ExampleDS");
		return vdbOneDeployed;
	}
	
	private void undeploy() throws Exception {
		VDB vdb = admin.getVDB("bqt", 1);
		if (vdb != null) {
			admin.undeploy("bqt.vdb");
		}
	}
	
	@Test
	public void testGetRequests() throws Exception {
		JavaArchive jar = getLoopyArchive();

		try {
			admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());
			deployVdb();
			VDB vdb = admin.getVDB("bqt", 1);
			Model model = vdb.getModels().get(0);
			Translator t = admin.getTranslator("loopy");
			assertNotNull(t);
			
			admin.assignToModel("bqt", 1, model.getName(), "Source", "loopy", "java:jboss/datasources/ExampleDS");
			Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
			Collection<? extends Session> sessions = admin.getSessions();
			assertEquals (1, sessions.size());
			Session s = sessions.iterator().next();
			
			Statement stmt = conn.createStatement();
			
			Collection<? extends Request> requests = admin.getRequests();
			
			assertEquals(0, requests.size());
			
			stmt.execute("select * from source.smalla");
			
			requests = admin.getRequests();
			assertEquals(1, requests.size());
			
			Request r = requests.iterator().next();
			assertEquals("select * from source.smalla", r.getCommand());
			assertNotNull(r.getExecutionId());
			assertNotNull(r.getSessionId());

			stmt.execute("select * from source.smalla");
			Collection<? extends Request> requests2 = admin.getRequestsForSession(s.getSessionId());
			assertEquals(1, requests2.size());
			
			Request r2 = requests.iterator().next();
			assertEquals("select * from source.smalla", r2.getCommand());
			assertEquals(s.getSessionId(), r2.getSessionId());
			
			stmt.close();
			conn.close();
			
			requests = admin.getRequests();
			assertEquals(0, requests.size());

		} finally {
			admin.undeploy("loopy.jar");
			undeploy();
		}
	}
	
	@Test
	public void getDatasourceTemplateNames() throws Exception {
		String[] array  = {"teiid-connector-file.rar", "teiid-local", "teiid-connector-salesforce.rar", "teiid-connector-ldap.rar", "teiid-connector-ws.rar", "h2"};
		try {
			deployVdb();
			Set<String> templates = admin.getDataSourceTemplateNames();
			assertArrayEquals(array, templates.toArray(new String[templates.size()]));
		} finally {
			undeploy();
		}
	}
	
	@Test
	public void getTemplatePropertyDefinitions() throws Exception{
		try {
			HashSet<String> props = new HashSet<String>();			
			
			deployVdb();
			
			Collection<? extends PropertyDefinition> pds = admin.getTemplatePropertyDefinitions("h2");
			for(PropertyDefinition pd:pds) {
				props.add(pd.getName());
			}
			assertTrue(props.contains("connection-url"));
			assertTrue(props.contains("user-name"));
			assertTrue(props.contains("password"));
			assertTrue(props.contains("check-valid-connection-sql"));
			
			
			HashSet<String> rar_props = new HashSet<String>();
			pds = admin.getTemplatePropertyDefinitions("teiid-connector-file.rar");
			for(PropertyDefinition pd:pds) {
				rar_props.add(pd.getName());
			}
			
			assertTrue(rar_props.contains("ParentDirectory"));
			assertTrue(rar_props.contains("FileMapping"));
			assertTrue(rar_props.contains("AllowParentPaths"));
			
		} finally {
			undeploy();
		}		
	}
	
	@Test
	public void getWorkerPoolStats() throws Exception{
		try {
			deployVdb();
			assertNotNull(admin.getWorkerPoolStats());
		} finally {
			undeploy();
		}		
	}
	
	@Test
	public void testDataRoleMapping() throws Exception{
		try {
			admin.deploy("bqt2.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt2.vdb")));			
			
			VDB vdb = admin.getVDB("bqt", 2);
			Model model = vdb.getModels().get(0);
			admin.assignToModel("bqt", 2, model.getName(), "Source", "h2", "java:jboss/datasources/ExampleDS");
			
			vdb = admin.getVDB("bqt", 2);
			assertTrue(vdb.isValid());
			List<DataPolicy> policies = vdb.getDataPolicies();
			assertEquals(1, policies.size());
			
			DataPolicy dp = policies.get(0);
			assertEquals("roleOne", dp.getName());
			assertEquals(2, dp.getPermissions().size());
			assertTrue(dp.isAllowCreateTemporaryTables());
			assertTrue(dp.isAnyAuthenticated());
			
			List<String> roleNames = dp.getMappedRoleNames();
			assertArrayEquals(new String[]{"ROLE1", "ROLE2"}, roleNames.toArray());
			
			admin.removeDataRoleMapping("bqt", 2, "roleOne", "ROLE1");
			
			vdb = admin.getVDB("bqt", 2);
			policies = vdb.getDataPolicies();
			dp = policies.get(0);
			
			roleNames = dp.getMappedRoleNames();
			assertArrayEquals(new String[]{"ROLE2"}, roleNames.toArray());
			
			admin.addDataRoleMapping("bqt", 2, "roleOne", "ROLE3");
			
			vdb = admin.getVDB("bqt", 2);
			policies = vdb.getDataPolicies();
			dp = policies.get(0);
			
			roleNames = dp.getMappedRoleNames();
			assertArrayEquals(new String[]{"ROLE2", "ROLE3"}, roleNames.toArray());
			
			admin.setAnyAuthenticatedForDataRole("bqt", 2, "roleOne", false);
			
			vdb = admin.getVDB("bqt", 2);
			policies = vdb.getDataPolicies();
			dp = policies.get(0);
			
			assertFalse(dp.isAnyAuthenticated());
		} finally {
			if (admin.getVDB("bqt", 2) != null) {
				admin.undeploy("bqt2.vdb");
			}
		}		
	}
	
	
	
}
