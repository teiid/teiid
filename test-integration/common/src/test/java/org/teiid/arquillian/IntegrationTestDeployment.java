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

package org.teiid.arquillian;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.postgresql.Driver;
import org.teiid.adminapi.*;
import org.teiid.adminapi.Admin.TranlatorPropertyType;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestDeployment {

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost",
                AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());
	}
	
	@After
	public void teardown() throws AdminException {
		AdminUtil.cleanUp(admin);
		admin.close();
	}
	
	@Test public void testChainedDelegates() throws Exception {
		Properties props = new Properties();
		props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
		props.setProperty("user-name", "sa");
		props.setProperty("password", "sa");
		
		AdminUtil.createDataSource(admin, "ChainedDS", "h2", props);
		admin.deploy("fake.jar",new FileInputStream(UnitTestUtil.getTestDataFile("fake.jar")));
		try {
			admin.deploy("chained-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("chained-vdb.xml")));
		} finally {
			admin.undeploy("fake.jar");
			admin.deleteDataSource("ChainedDS");
		}
	}
	
	@Test
	public void testVDBDeployment() throws Exception {
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
		assertTrue(AdminUtil.waitForVDBLoad(admin, "bqt", 1, 3));
		assertFalse(vdb.isValid());

		Properties props = new Properties();
		props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
		props.setProperty("user-name", "sa");
		props.setProperty("password", "sa");
		props.setProperty("connection-properties", "foo=bar,blah=blah");
		
		admin.createDataSource("Oracle11_PushDS", "h2", props);
		
		vdb = admin.getVDB("bqt", 1);
		assertTrue(vdb.isValid());
		assertTrue(vdb.getStatus().equals(Status.ACTIVE));
		
		dsNames = admin.getDataSourceNames();
		assertTrue(dsNames.contains("Oracle11_PushDS"));

		/*
		admin.deleteDataSource("Oracle11_PushDS");
		vdb = admin.getVDB("bqt", 1);
		assertFalse(vdb.isValid());
		
		admin.deploy("bqt.2.vdb",new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
		vdb = admin.getVDB("bqt", 2);
		assertEquals("2", vdb.getVersion());
		*/
	}
	
	@Test
	public void testGetDatasourceProperties() throws Exception {
	    // jdbc data source
	    String jdbcSource = "jdbc-source";
	    assertFalse(admin.getDataSourceNames().contains(jdbcSource));
	    
		Properties props = new Properties();
		props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
		props.setProperty("user-name", "sa");
		props.setProperty("password", "sa");
		props.setProperty("connection-properties", "foo=bar,blah=blah");
		props.setProperty("max-pool-size", "4");
		
		admin.createDataSource(jdbcSource, "h2", props);	
		
		assertTrue(admin.getDataSourceNames().contains(jdbcSource));
		
		Properties p = admin.getDataSource(jdbcSource);
		assertEquals("4", p.getProperty("max-pool-size"));
		assertEquals("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", p.getProperty("connection-url"));
		assertEquals("h2", p.getProperty("driver-name"));
		
		admin.deleteDataSource("jdbc-source");
		assertFalse(admin.getDataSourceNames().contains(jdbcSource));
		
		// resource -adapter
		assertTrue(admin.getDataSourceTemplateNames().contains("webservice"));
		
		String raSource = "ra-source";
		assertFalse(admin.getDataSourceNames().contains(raSource));
		
		p = new Properties();
		p.setProperty("class-name", "org.teiid.resource.adapter.ws.WSManagedConnectionFactory");
		p.setProperty("EndPoint", "{endpoint}");
		
		admin.createDataSource(raSource, "webservice", p);		
		
		assertTrue(admin.getDataSourceNames().contains(raSource));
		
		p = admin.getDataSource(raSource);
		
		assertEquals("org.teiid.resource.adapter.ws.WSManagedConnectionFactory", p.getProperty("class-name"));
		assertEquals("{endpoint}", p.getProperty("EndPoint"));
		assertEquals("webservice", p.getProperty("driver-name"));
		
		admin.deleteDataSource(raSource);		
		assertFalse(admin.getDataSourceNames().contains(raSource));
		assertTrue(admin.getDataSourceTemplateNames().contains("webservice"));
	}

	@Test
	public void testTraslators() throws Exception {
		Collection<? extends Translator> translators = admin.getTranslators();
		assertEquals(translators.toString(), 55, translators.size());

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
	
    @Test
    public void testTraslatorProperties() throws Exception {
        Collection<? extends PropertyDefinition> props = admin.getTranslatorPropertyDefinitions("accumulo", TranlatorPropertyType.OVERRIDE);
        assertEquals(21, props.size());
        
        props = admin.getTranslatorPropertyDefinitions("accumulo", TranlatorPropertyType.EXTENSION_METADATA);
        assertEquals(3, props.size());
        for (PropertyDefinition p: props) {
            if (p.getName().equals("{http://www.teiid.org/translator/accumulo/2013}CF")) {
                assertEquals("org.teiid.metadata.Column", p.getPropertyValue("owner"));
            }
        }

        props = admin.getTranslatorPropertyDefinitions("accumulo", TranlatorPropertyType.IMPORT);
        assertEquals(2, props.size());
        for (PropertyDefinition p: props) {
            if (p.getName().equals("importer.ColumnNamePattern")) {
                assertEquals("java.lang.String", p.getPropertyTypeClassName());
            }
        }        
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
		admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));	
		
		AdminUtil.waitForVDBLoad(admin, "bqt2", 1, 3);	
		
		VDB vdb = admin.getVDB("bqt", 1);
		Model model = vdb.getModels().get(0);
		admin.updateSource("bqt", 1, "Source", "h2", "java:jboss/datasources/ExampleDS");
		
		try {
			//should not be able to remove from non-multisource
			admin.removeSource("bqt", 1, model.getName(), "Source");
			fail();
		} catch (AdminException e) {
			
		}
		
		assertEquals(ConnectionType.BY_VERSION, vdb.getConnectionType());
		
		Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
		conn.close();
		
		admin.changeVDBConnectionType("bqt", 1, ConnectionType.NONE);

		try {
			TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
			fail("should have failed to connect as no new connections allowed");
		} catch (Exception e) {
			//pass
		}

		admin.deploy("bqt2.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt2.vdb")));
		AdminUtil.waitForVDBLoad(admin, "bqt2", 1, 3);	

		admin.updateSource("bqt", 2, "Source", "h2", "java:jboss/datasources/ExampleDS");
		
		conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
		conn.close();
		
		admin.changeVDBConnectionType("bqt", 2, ConnectionType.ANY);
		conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
		conn.close();
		
		vdb = admin.getVDB("bqt", 2);
		model = vdb.getModels().get(0);
		assertEquals(model.getSourceConnectionJndiName("Source"), "java:jboss/datasources/ExampleDS");
		assertEquals(model.getSourceTranslatorName("Source"), "h2");
		assertEquals(ConnectionType.ANY, vdb.getConnectionType());
	}
	
	@Test
	public void testCacheTypes() throws Exception {
		String[] array = {Admin.Cache.PREPARED_PLAN_CACHE.toString(), Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.toString()};
		Collection<String> types = admin.getCacheTypes();
		assertArrayEquals(array, types.toArray());
	}
	
	@Test
	public void testSessions() throws Exception {
		deployVdb();

		Collection<? extends Session> sessions = admin.getSessions();
		assertEquals (0, sessions.size());
		
		Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user;ApplicationName=test", null);
		sessions = admin.getSessions();
		assertEquals (1, sessions.size());
		Session s = sessions.iterator().next();
		
		assertEquals("user@teiid-security", s.getUserName());
		assertEquals("test", s.getApplicationName());
		assertEquals("bqt", s.getVDBName());
		assertEquals("1", s.getVDBVersion());
		assertNotNull(s.getSessionId());
		
		conn.close();
		
		conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user;ApplicationName=test", null);
		sessions = admin.getSessions();
		assertEquals (1, sessions.size());
		s = sessions.iterator().next();

		admin.terminateSession(s.getSessionId());
		Thread.sleep(2000);
		sessions = admin.getSessions();
		assertEquals (0, sessions.size());			
		conn.close();
	}

	private boolean deployVdb() throws AdminException, FileNotFoundException {
		boolean vdbOneDeployed;
		admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
		AdminUtil.waitForVDBLoad(admin, "bqt", 1, 3);
		vdbOneDeployed = true;
		
		VDB vdb = admin.getVDB("bqt", 1);
		Model model = vdb.getModels().get(0);
		admin.updateSource("bqt", 1, "Source", "h2", "java:jboss/datasources/ExampleDS");
		return vdbOneDeployed;
	}
	
	@Test
	public void testGetRequests() throws Exception {
		JavaArchive jar = getLoopyArchive();

		try {
			admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());
			deployVdb();
			Translator t = admin.getTranslator("loopy");
			assertNotNull(t);
			
			admin.updateSource("bqt", 1, "Source", "loopy", "java:jboss/datasources/ExampleDS");
			Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
			
			Collection<? extends Session> sessions = admin.getSessions();
			assertTrue (sessions.size() >= 1);
			
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select session_id()");
			rs.next();
			String session = rs.getString(1);
			rs.close();
			
			Collection<? extends Request> requests = admin.getRequestsForSession(session);
			
			assertEquals(0, requests.size());
			
			stmt.execute("select * from source.smalla");
			
			requests = admin.getRequests();
			assertEquals(1, requests.size());
			
			Request r = requests.iterator().next();
			assertEquals("select * from source.smalla", r.getCommand());
			assertNotNull(r.getExecutionId());
			assertNotNull(r.getSessionId());

			stmt.execute("select * from source.smalla");
			Collection<? extends Request> requests2 = admin.getRequestsForSession(session);
			assertEquals(1, requests2.size());
			
			Request r2 = requests.iterator().next();
			assertEquals("select * from source.smalla", r2.getCommand());
			assertEquals(session, r2.getSessionId());
			
			stmt.close();
			conn.close();
			
			requests = admin.getRequestsForSession(session);
			assertEquals(0, requests.size());

		} finally {
			admin.undeploy("loopy.jar");
		}
	}
	
	@Test
	public void getDatasourceTemplateNames() throws Exception {
		Set<String> vals  = new HashSet<String>(Arrays.asList(new String[]{"teiid-local", "google", "teiid", "ldap", 
				"accumulo", "file", "cassandra", "salesforce", "salesforce-34", "mongodb", "solr", "webservice", 
				"simpledb", "h2", "teiid-xa", "h2-xa", "teiid-local-xa"}));
		deployVdb();
		Set<String> templates = admin.getDataSourceTemplateNames();
		assertEquals(vals, templates);
	}
	
	@Test
	public void getTemplatePropertyDefinitions() throws Exception{
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
		assertTrue(props.contains("max-pool-size"));
		assertTrue(props.contains("connection-properties"));
		assertTrue(props.contains("max-pool-size"));
		
		HashSet<String> rar_props = new HashSet<String>();
		pds = admin.getTemplatePropertyDefinitions("file");
		for(PropertyDefinition pd:pds) {
			rar_props.add(pd.getName());
		}
		
		assertTrue(rar_props.contains("ParentDirectory"));
		assertTrue(rar_props.contains("FileMapping"));
		assertTrue(rar_props.contains("AllowParentPaths"));
		assertTrue(rar_props.contains("resourceadapter-class"));
		assertTrue(rar_props.contains("managedconnectionfactory-class"));
		assertTrue(rar_props.contains("max-pool-size"));
	}
	
	@Test
	public void getTranslatorPropertyDefinitions() throws Exception{
		HashSet<String> props = new HashSet<String>();			
		
		Collection<? extends PropertyDefinition> pds = admin.getTranslatorPropertyDefinitions("ws");
		for(PropertyDefinition pd:pds) {
			props.add(pd.getName());
		}
		assertTrue(props.contains("DefaultBinding"));
		assertTrue(props.contains("DefaultServiceMode"));
		assertTrue(props.contains("MaxDependentInPredicates"));
		
		for(PropertyDefinition pd:pds) {
			if (pd.getName().equals("DefaultBinding")) {
				assertEquals("java.lang.String", pd.getPropertyTypeClassName());
				assertFalse(pd.isRequired());
				assertEquals("Contols what SOAP or HTTP type of invocation will be used if none is specified.", pd.getDescription());
				assertEquals("Default Binding", pd.getDisplayName());
				assertTrue(pd.isModifiable());
				assertFalse(pd.isAdvanced());
				assertFalse(pd.isMasked());
				assertEquals("SOAP12", pd.getDefaultValue());
				assertNotNull(pd.getAllowedValues());
			}
		}
	}	
	
	@Test
	public void getWorkerPoolStats() throws Exception{
		deployVdb();
		assertNotNull(admin.getWorkerPoolStats());
	}
	
	@Test
	public void testDataRoleMapping() throws Exception{
		admin.deploy("bqt2.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt2.vdb")));			
		AdminUtil.waitForVDBLoad(admin, "bqt2", 1, 3);

		VDB vdb = admin.getVDB("bqt", 2);
		Model model = vdb.getModels().get(0);
		admin.updateSource("bqt", 2, "Source", "h2", "java:jboss/datasources/ExampleDS");
		
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
	}
	
	@Test
	@Ignore
	public void testCreateConnectionFactory() throws Exception{
		String deployedName = "wsOne";
		
		assertFalse(admin.getDataSourceNames().contains(deployedName));
		
		Properties p = new Properties();
		p.setProperty("class-name", "org.teiid.resource.adapter.ws.WSManagedConnectionFactory");
		p.setProperty("EndPoint", "{endpoint}");
		admin.createDataSource(deployedName, "webservice", p);
		
		assertTrue(admin.getDataSourceNames().contains(deployedName));
		
		admin.deleteDataSource(deployedName);
		
		assertFalse(admin.getDataSourceNames().contains(deployedName));
		
		admin.createDataSource(deployedName, "webservice", p);
		
		assertTrue(admin.getDataSourceNames().contains(deployedName));
		
		admin.deleteDataSource(deployedName);
	}
	
	@Test
    public void testCreateXADatasource() throws Exception {
	    String vdbName = "test";
        String deployedName = "fooXA";
        String testVDB = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + 
                "<vdb name=\"test\" version=\"1\">\n" + 
                "    <property name=\"UseConnectorMetadata\" value=\"cached\" />\n" + 
                "    <model name=\"loopy\">\n" + 
                "        <source name=\"loop\" translator-name=\"loopback\" />\n" + 
                "    </model>\n" + 
                "</vdb>";
        admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
        AdminUtil.waitForVDBLoad(admin, vdbName, 1, 3);
        
        assertTrue(admin.getDataSourceTemplateNames().contains("teiid-xa"));
        
        Properties p = new Properties();
        p.setProperty("DatabaseName", "test");
        try {
         admin.createDataSource(deployedName, "teiid-xa", p);
         fail("should have fail not find portNumber");
        } catch(AdminException e) {           
        }
        
        assertFalse(admin.getDataSourceNames().contains(deployedName));
        
        p.setProperty("ServerName", "127.0.0.1");
        p.setProperty("PortNumber", "31000");
        
        admin.createDataSource(deployedName, "teiid-xa", p);
        
        assertTrue(admin.getDataSourceNames().contains(deployedName));
        
        
        //admin.deleteDataSource(deployedName);
    }	
	
	@Test
	public void testVDBRestart() throws Exception{
		String vdbName = "test";
		String testVDB = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + 
				"<vdb name=\"test\" version=\"1\">\n" + 
				"    <property name=\"UseConnectorMetadata\" value=\"cached\" />\n" + 
				"    <model name=\"loopy\">\n" + 
				"        <source name=\"loop\" translator-name=\"loopy\" />\n" + 
				"    </model>\n" + 
				"</vdb>";
		
		Collection<?> vdbs = admin.getVDBs();
		assertTrue(vdbs.isEmpty());
		
		JavaArchive jar = getLoopyArchive();
		admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());
		
		// normal load
		admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
		AdminUtil.waitForVDBLoad(admin, vdbName, 1, 3);
		int count = assertMetadataLoadCount(false, 1);

		// 1st restart
		admin.restartVDB(vdbName, 1);
		AdminUtil.waitForVDBLoad(admin, vdbName, 1, 3);
		count = assertMetadataLoadCount(true, count+1);

		// 2nd restart
		admin.restartVDB(vdbName, 1);
		AdminUtil.waitForVDBLoad(admin, vdbName, 1, 3);
		count = assertMetadataLoadCount(true, count+1);
		
		admin.undeploy("loopy.jar");
	}

	private int assertMetadataLoadCount(boolean check, int expected) throws SQLException {				
		Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:test.1@mm://localhost:31000;user=user;password=user", null);		
		Statement stmt = conn.createStatement();
		stmt.execute("SELECT execCount FROM Matadata");
		ResultSet rs = stmt.getResultSet();
		rs.next();
		int execCount = rs.getInt(1);
		if (check) {
			assertEquals(expected, execCount);
		}
		rs.close();
		stmt.close();
		conn.close();
		return execCount;
	}	
	
	@Test
	public void testDDLExport() throws Exception{
		String vdbName = "test";
		String testVDB = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + 
				"<vdb name=\"test\" version=\"1\">\n" + 
				"    <property name=\"UseConnectorMetadata\" value=\"cached\" />\n" + 
				"    <model name=\"loopy\">\n" + 
				"        <source name=\"loop\" translator-name=\"loopy\" />\n" + 
				"    </model>\n" + 
				"</vdb>";
		
		Collection<?> vdbs = admin.getVDBs();
		assertTrue(vdbs.isEmpty());
		
		JavaArchive jar = getLoopyArchive();
		admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());
		
		// normal load
		admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
		AdminUtil.waitForVDBLoad(admin, vdbName, 1, 3);
		
		String ddl = admin.getSchema(vdbName, 1, "loopy", null, null);

		String expected = "CREATE FOREIGN TABLE Matadata (\n" + 
				"	execCount integer\n" + 
				");";
		assertEquals(expected, ddl);
		
		admin.undeploy("loopy.jar");
	}	
	
	@Test public void testErrorDeployment() throws Exception {
		Collection<?> vdbs = admin.getVDBs();
		assertTrue(vdbs.isEmpty());
		
		admin.deploy("error-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("error-vdb.xml")));
		
		AdminUtil.waitForVDBLoad(admin, "error", 1, 3);
		VDB vdb = admin.getVDB("error", 1);
		assertEquals(Status.FAILED, vdb.getStatus());
	}

	@Test
	public void testODBCConnectionSuccess() throws Exception {
		admin.deploy("bqt.vdb",new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "user");
		p.setProperty("password", "user");
		Connection c = d.connect("jdbc:postgresql://127.0.0.1:35432/bqt", p);
		c.close();
	}
	
	@Test
	public void testODBCConnectionFailure() throws Exception {
		admin.deploy("bqt.vdb",new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
		Driver d = new Driver();
		Properties p = new Properties();
		p.setProperty("user", "user");
		p.setProperty("password", "notpassword");
		try {
			d.connect("jdbc:postgresql://127.0.0.1:35432/bqt", p);
			fail("failed due to bad credentials");
		} catch (SQLException e) {
		}
	}

	@Test
	public void testSystemPropertiesInVDBXML() throws Exception{
		String vdbName = "test";
		String testVDB = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + 
				"<vdb name=\"test\" version=\"1\">\n" + 
				"    <property name=\"UseConnectorMetadata\" value=\"${teiid.vdb.UseConnectorMetadata:none}\" />\n" + 
				"    <model name=\"loopy\">\n" + 
				"        <source name=\"loop\" translator-name=\"loopy\" />\n" + 
				"    </model>\n" + 
				"</vdb>";
		
		Collection<?> vdbs = admin.getVDBs();
		assertTrue(vdbs.isEmpty());
		
		JavaArchive jar = getLoopyArchive();
		admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());
		
		// normal load
		admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
		AdminUtil.waitForVDBLoad(admin, vdbName, 1, 3);
		
		VDB vdb = admin.getVDB(vdbName, 1);
		String value = vdb.getPropertyValue("UseConnectorMetadata");

		// see the arquillian.zml file in resources in the JVM proeprties section for the expected value
		assertEquals("custom", value);
		
		admin.undeploy("loopy.jar");
		admin.undeploy("test-vdb.xml");
	}
	
	@Test
	public void testGeometry() throws Exception {
		admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));	
		
		AdminUtil.waitForVDBLoad(admin, "bqt", 1, 3);	
		
		Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user", null);
		
		Statement s = conn.createStatement();
		//test each functional area - jts, proj4j, and geojson
		s.executeQuery("select st_geomfromtext('POINT(0 0)')");
		s.executeQuery("select ST_AsText(ST_Transform(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))',2249),4326))");
		s.executeQuery("select ST_AsGeoJson(ST_GeomFromText('POINT (-48.23456 20.12345)'))");
		s.executeQuery("select ST_AsText(ST_GeomFromGeoJSON('{\"coordinates\":[-48.23456,20.12345],\"type\":\"Point\"}'))");
	}
	
	@Test(expected=AdminProcessingException.class) public void testAmbigiousDeployment() throws Exception {
		admin.deploy("bqt2.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt2.vdb")));			
		AdminUtil.waitForVDBLoad(admin, "bqt2", 1, 3);
		admin.deploy("bqt2-1.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt2.vdb")));			
	}
	
    @Test
    public void testInsensitiveDeployment() throws Exception { 
        admin.deploy("dynamicview-VDB.xml", new FileInputStream(UnitTestUtil.getTestDataFile("dynamicview-vdb.xml")));
        AdminUtil.waitForVDBLoad(admin, "dynamic", 1, 3);
        VDB vdb = admin.getVDB("dynamic", "1");
        assertTrue(vdb.getStatus() == Status.ACTIVE);
        admin.undeploy("dynamicview-VDB.xml");
        
        admin.deploy("bqt.VDB", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));  
        AdminUtil.waitForVDBLoad(admin, "bqt", 1, 3);   
        vdb = admin.getVDB("bqt", "1");
        assertTrue(vdb.getStatus() == Status.ACTIVE);
        admin.undeploy("bqt.VDB");        

    }	
}
