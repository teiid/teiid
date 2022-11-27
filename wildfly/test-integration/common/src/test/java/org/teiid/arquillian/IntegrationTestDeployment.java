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

package org.teiid.arquillian;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.TranlatorPropertyType;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
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
        assertTrue(vdbs.toString(), vdbs.isEmpty());

        Collection<String> dsNames = admin.getDataSourceNames();
        if (dsNames.contains("Oracle11_PushDS")) {
            admin.deleteDataSource("Oracle11_PushDS");
        }

        admin.deploy("bqt.vdb",new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));

        vdbs = admin.getVDBs();
        assertFalse(vdbs.isEmpty());

        VDB vdb = admin.getVDB("bqt", 1);
        assertFalse(vdb.isValid());
        assertTrue(AdminUtil.waitForVDBLoad(admin, "bqt", 1));
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
    public void testTranslators() throws Exception {
        Collection<? extends Translator> translators = admin.getTranslators();
        assertEquals(translators.toString(), 64, translators.size());

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
    public void testCustomMetadataRepository() throws Exception {
        JavaArchive jar = getMetyArchive();

        try {
            admin.deploy("mety.jar", jar.as(ZipExporter.class).exportAsInputStream());
            String testVDB = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                    "<vdb name=\"test\" version=\"1\">\n" +
                    "    <model type=\"virtual\" name=\"mety\">\n" +
                    "        <metadata type = \"deployment.mety.jar\"/>" +
                    "    </model>\n" +
                    "</vdb>";
            admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
            AdminUtil.waitForVDBLoad(admin, "test", 1);
        } finally {
            admin.undeploy("mety.jar");
        }
    }

    @Test
    public void testTranslatorProperties() throws Exception {
        Collection<? extends PropertyDefinition> props = admin.getTranslatorPropertyDefinitions("accumulo", TranlatorPropertyType.OVERRIDE);
        assertEquals(20, props.size());

        props = admin.getTranslatorPropertyDefinitions("accumulo", TranlatorPropertyType.EXTENSION_METADATA);
        assertEquals(4, props.size());
        for (PropertyDefinition p: props) {
            if (p.getName().equals("{http://www.teiid.org/translator/accumulo/2013}CF")) {
                assertEquals("org.teiid.metadata.Column", p.getPropertyValue("owner"));
            }
        }

        props = admin.getTranslatorPropertyDefinitions("accumulo", TranlatorPropertyType.IMPORT);
        assertEquals(8, props.size());
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

    private JavaArchive getMetyArchive() {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "mety.jar")
                  .addClasses(SampleMetadataRepository.class)
                  .addAsManifestResource(new ByteArrayAsset(SampleMetadataRepository.class.getName().getBytes()),
                        ArchivePaths.create("services/org.teiid.metadata.MetadataRepository"));
        jar.addAsManifestResource(new ByteArrayAsset("Dependencies: org.jboss.teiid.common-core,org.jboss.teiid.api\n".getBytes()),
                ArchivePaths.create("MANIFEST.MF"));
        return jar;
    }

    @Test
    public void testVDBConnectionType() throws Exception {
        admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));

        AdminUtil.waitForVDBLoad(admin, "bqt2", 1);

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
        AdminUtil.waitForVDBLoad(admin, "bqt2", 1);

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

        //wait for sessions created for proactive materialization to finish
        Collection<? extends Session> sessions = waitForNumberOfSessions(0);

        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user;ApplicationName=test", null);
        sessions = admin.getSessions();
        assertEquals (1, sessions.size());
        Session s = sessions.iterator().next();

        assertEquals("user", s.getUserName());
        assertEquals("test", s.getApplicationName());
        assertEquals("bqt", s.getVDBName());
        assertEquals("1", s.getVDBVersion());
        assertNotNull(s.getSessionId());

        conn.close();

        conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user;ApplicationName=test", null);
        sessions = waitForNumberOfSessions(1);
        s = sessions.iterator().next();

        admin.terminateSession(s.getSessionId());
        waitForNumberOfSessions(0);
        conn.close();
    }

    private Collection<? extends Session> waitForNumberOfSessions(int count)
            throws AdminException, InterruptedException {
        Collection<? extends Session> sessions = admin.getSessions();
        long start = System.currentTimeMillis();
        while (sessions.size() != count) {
            if (System.currentTimeMillis() - start > 5000) {
                fail("should be " + count + " session(s), but was " + sessions);
            }
            Thread.sleep(100);
            sessions = admin.getSessions();
        }
        return sessions;
    }

    private boolean deployVdb() throws AdminException, FileNotFoundException {
        boolean vdbOneDeployed;
        admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
        AdminUtil.waitForVDBLoad(admin, "bqt", 1);
        vdbOneDeployed = true;

        VDB vdb = admin.getVDB("bqt", 1);
        Model model = vdb.getModels().get(0);
        admin.updateSource("bqt", 1, "Source", "h2", "java:jboss/datasources/ExampleDS");
        return vdbOneDeployed;
    }

    @Test
    public void testOSDQ() throws Exception {
        deployVdb();

        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:bqt@mm://localhost:31000;user=user;password=user;ApplicationName=test", null);

        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("select osdq.validemail('not really'), validemail('user@teiid.org')");
        rs.next();
        assertFalse(rs.getBoolean(1));
        assertTrue(rs.getBoolean(2));
        conn.close();
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

            Thread.sleep(500);

            Collection<? extends Request> requests = admin.getRequestsForSession(session);

            assertEquals(0, requests.size());

            stmt.execute("select * from source.smalla");

            requests = admin.getRequests();
            assertEquals(1, requests.size());

            Request r = requests.iterator().next();
            assertEquals("select * from source.smalla", r.getCommand());
            assertNotNull(r.getExecutionId());
            assertNotNull(r.getSessionId());

            String plan = admin.getQueryPlan(r.getSessionId(), r.getExecutionId());
            assertNotNull(plan);

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
                "accumulo", "file", "ftp", "cassandra", "salesforce", "salesforce-34", "salesforce-41", "mongodb", "solr", "webservice",
                "simpledb", "h2", "teiid-xa", "h2-xa", "teiid-local-xa", "couchbase", "infinispan", "hdfs", "s3"}));
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

        Collection<? extends PropertyDefinition> xa_pds = admin.getTemplatePropertyDefinitions("h2-xa");
        HashSet<String> xa_props = new HashSet<String>();
        for(PropertyDefinition pd:xa_pds) {
            xa_props.add(pd.getName());
        }
        assertTrue(xa_props.contains("xa-resource-timeout"));
        assertTrue(xa_props.contains("max-pool-size"));
        assertTrue(xa_props.contains("transaction-isolation"));
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
        AdminUtil.waitForVDBLoad(admin, "bqt2", 1);

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
                "    <property name=\"cache-metadata\" value=\"true\" />\n" +
                "    <model name=\"loopy\">\n" +
                "        <source name=\"loop\" translator-name=\"loopback\" />\n" +
                "    </model>\n" +
                "</vdb>";
        admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
        AdminUtil.waitForVDBLoad(admin, vdbName, 1);

        assertTrue(admin.getDataSourceTemplateNames().contains("teiid-xa"));

        Properties p = new Properties();
        p.setProperty("DatabaseName", "test");
        try {
            admin.createDataSource(deployedName, "teiid-xa", p);
            fail("should have fail not find portNumber");
        } catch (AdminException e) {
        }

        assertFalse(admin.getDataSourceNames().contains(deployedName));

        p.setProperty("ServerName", "127.0.0.1");
        p.setProperty("PortNumber", "31000");
        p.setProperty("connection-properties", "x=something,y=foo");

        admin.createDataSource(deployedName, "teiid-xa", p);

        assertTrue(admin.getDataSourceNames().contains(deployedName));

        Properties fullProps = admin.getDataSource(deployedName);

        assertEquals("127.0.0.1", fullProps.getProperty("ServerName"));
        assertEquals("31000", fullProps.getProperty("PortNumber"));
        assertEquals("x=something,y=foo", fullProps.getProperty("connection-properties"));

        admin.deleteDataSource(deployedName);

        p.clear();

        p.put("URL", "...");

        admin.createDataSource(deployedName, "teiid-xa", p);

        fullProps = admin.getDataSource(deployedName);

        assertEquals("...", fullProps.getProperty("URL"));

        //will only be set if enabled
        assertEquals("false", fullProps.getProperty("interleaving"));
    }

    @Test
    public void testCreateGoogleSpreadSheetSource() throws AdminException {
        Properties p = new Properties();
        p.setProperty("ClientSecret", "a");
        p.setProperty("ClientId", "b");
        p.setProperty("RefreshToken", "c");
        p.setProperty("SpreadsheetName", "d");
        p.setProperty("class-name", "org.teiid.resource.adapter.google.SpreadsheetManagedConnectionFactory");

        admin.createDataSource("my-sheet", "google", p);
        admin.deleteDataSource("my-sheet");
    }

    @Test
    public void testCreateInfinispanSource() throws AdminException {
        Properties p = new Properties();
        p.setProperty("UserName", "b");
        p.setProperty("Password", "c");
        p.setProperty("CacheName", "foo");
        p.setProperty("RemoteServerList", "localhost:12345");
        p.setProperty("class-name", "org.teiid.resource.adapter.infinispan.hotrod.InfinispanManagedConnectionFactory");

        admin.createDataSource("my-jdg", "infinispan", p);
        admin.deleteDataSource("my-jdg");
    }

    @Test
    public void testCreateHdfsSource() throws AdminException {
        Properties p = new Properties();
        p.setProperty("FsUri", "localhost:12345");
        p.setProperty("class-name", "org.teiid.resource.adapter.hdfs.HdfsManagedConnectionFactory");

        admin.createDataSource("my-hdfs", "hdfs", p);
        admin.deleteDataSource("my-hdfs");
    }

    @Test
    public void testCreateS3Source() throws AdminException {
        Properties p = new Properties();
        p.setProperty("Endpoint", "localhost:12345");
        p.setProperty("Bucket", "bucket");
        p.setProperty("AccessKey", "123");
        p.setProperty("SecretKey", "abc");
        p.setProperty("class-name", "org.teiid.resource.adapter.s3.S3ManagedConnectionFactory");

        admin.createDataSource("my-s3", "s3", p);
        admin.deleteDataSource("my-s3");
    }

    @Test
    public void testCreateSimpleDBSource() throws AdminException {
        Properties p = new Properties();
        p.setProperty("AccessKey", "123");
        p.setProperty("SecretAccessKey", "abc");
        p.setProperty("class-name", "org.teiid.resource.adapter.simpledb.SimpleDBManagedConnectionFactory");

        admin.createDataSource("my-simpledb", "simpledb", p);
        admin.deleteDataSource("my-simpledb");
    }

    @Test
    public void testCreateCassandraSource() throws AdminException {
        Properties p = new Properties();
        p.setProperty("Address", "localhost");
        p.setProperty("Keyspace", "something");
        p.setProperty("Username", "user");
        p.setProperty("Password", "abc");
        p.setProperty("class-name", "org.teiid.resource.adapter.cassandra.CassandraManagedConnectionFactory");

        admin.createDataSource("my-cassandra", "cassandra", p);
        admin.deleteDataSource("my-cassandra");
    }

    @Test
    public void testVDBRestart() throws Exception{
        String vdbName = "test";
        String testVDB = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"test\" version=\"1\">\n" +
                "    <property name=\"cache-metadata\" value=\"true\" />\n" +
                "    <model name=\"loopy\">\n" +
                "        <source name=\"loop\" translator-name=\"loopy\" />\n" +
                "    </model>\n" +
                "</vdb>";

        Collection<?> vdbs = admin.getVDBs();
        assertTrue(vdbs.toString(), vdbs.isEmpty());

        JavaArchive jar = getLoopyArchive();
        admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());

        // normal load
        admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
        AdminUtil.waitForVDBLoad(admin, vdbName, 1);
        int count = assertMetadataLoadCount(false, 1);

        // 1st restart
        admin.restartVDB(vdbName, 1);
        AdminUtil.waitForVDBLoad(admin, vdbName, 1);
        count = assertMetadataLoadCount(true, count+1);

        // 2nd restart
        admin.restartVDB(vdbName, 1);
        AdminUtil.waitForVDBLoad(admin, vdbName, 1);
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
                "    <property name=\"cache-metadata\" value=\"true\" />\n" +
                "    <model name=\"loopy\">\n" +
                "        <source name=\"loop\" translator-name=\"loopy\" />\n" +
                "    </model>\n" +
                "</vdb>";

        Collection<?> vdbs = admin.getVDBs();
        assertTrue(vdbs.toString(), vdbs.isEmpty());

        JavaArchive jar = getLoopyArchive();
        admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());

        // normal load
        admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
        AdminUtil.waitForVDBLoad(admin, vdbName, 1);

        String ddl = admin.getSchema(vdbName, 1, "loopy", null, null);

        String expected = "CREATE FOREIGN TABLE Matadata (\n" +
                "	execCount integer\n" +
                ");";
        assertEquals(expected, ddl);

        admin.undeploy("loopy.jar");
    }

    @Test public void testErrorDeployment() throws Exception {
        Collection<?> vdbs = admin.getVDBs();
        assertTrue(vdbs.toString(), vdbs.isEmpty());

        admin.deploy("error-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("error-vdb.xml")));

        AdminUtil.waitForVDBLoad(admin, "error", 1);
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
                "    <property name=\"some-property\" value=\"${teiid.some-property:false}\" />\n" +
                "    <model name=\"loopy\">\n" +
                "        <source name=\"loop\" translator-name=\"loopy\" />\n" +
                "    </model>\n" +
                "</vdb>";

        Collection<?> vdbs = admin.getVDBs();
        assertTrue(vdbs.toString(), vdbs.isEmpty());

        JavaArchive jar = getLoopyArchive();
        admin.deploy("loopy.jar", jar.as(ZipExporter.class).exportAsInputStream());

        // normal load
        admin.deploy("test-vdb.xml", new ByteArrayInputStream(testVDB.getBytes()));
        AdminUtil.waitForVDBLoad(admin, vdbName, 1);

        VDB vdb = admin.getVDB(vdbName, 1);
        String value = vdb.getPropertyValue("some-property");

        // see the arquillian.xml file in resources in the JVM properties section for the expected value
        assertEquals("true", value);

        admin.undeploy("loopy.jar");
        admin.undeploy("test-vdb.xml");
    }

    @Test
    public void testGeometry() throws Exception {
        admin.deploy("bqt.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));

        AdminUtil.waitForVDBLoad(admin, "bqt", 1);

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
        AdminUtil.waitForVDBLoad(admin, "bqt2", 1);
        admin.deploy("bqt2-1.vdb", new FileInputStream(UnitTestUtil.getTestDataFile("bqt2.vdb")));
    }

    @Test
    public void testInsensitiveDeployment() throws Exception {
        admin.deploy("dynamicview-VDB.xml", new FileInputStream(UnitTestUtil.getTestDataFile("dynamicview-vdb.xml")));
        AdminUtil.waitForVDBLoad(admin, "dynamic", 1);
        VDB vdb = admin.getVDB("dynamic", "1");
        assertTrue(vdb.getStatus() == Status.ACTIVE);
        admin.undeploy("dynamicview-VDB.xml");

        admin.deploy("bqt.VDB", new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
        AdminUtil.waitForVDBLoad(admin, "bqt", 1);
        vdb = admin.getVDB("bqt", "1");
        assertTrue(vdb.getStatus() == Status.ACTIVE);
        admin.undeploy("bqt.VDB");

    }

    @Test
    public void testDeployZipDDL() throws Exception {
        File f = UnitTestUtil.getTestScratchFile("some.vdb");
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
        out.putNextEntry(new ZipEntry("v1.ddl"));
        out.write("CREATE VIEW helloworld as SELECT 'HELLO WORLD';".getBytes("UTF-8"));
        out.putNextEntry(new ZipEntry("v2.ddl"));
        out.write("CREATE VIEW helloworld2 as SELECT 'HELLO WORLD2';".getBytes("UTF-8"));
        out.putNextEntry(new ZipEntry("META-INF/vdb.ddl"));
        String externalDDL = "CREATE DATABASE test2 VERSION '1';"
                + "USE DATABASE test2 VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "IMPORT FROM REPOSITORY \"DDL-FILE\" INTO test2 OPTIONS(\"ddl-file\" '/v1.ddl');"
                + "IMPORT FOREIGN SCHEMA public FROM REPOSITORY \"DDL-FILE\" INTO test2 OPTIONS(\"ddl-file\" '/v2.ddl');";
        out.write(externalDDL.getBytes("UTF-8"));
        out.close();

        admin.deploy("some.vdb", new FileInputStream(f));

        AdminUtil.waitForVDBLoad(admin, "test2", "1");
        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:test2@mm://localhost:31000;user=user;password=user", null);
        ResultSet rs = conn.createStatement().executeQuery("select * from helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
        rs = conn.createStatement().executeQuery("select * from helloworld2");
        rs.next();
        assertEquals("HELLO WORLD2", rs.getString(1));
    }
}
