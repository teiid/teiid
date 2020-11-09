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
package org.teiid.metadatastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.security.Identity;
import java.security.Principal;
import java.security.acl.Group;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBRepository;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.metadata.MetadataRepository;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.runtime.EmbeddedAdminImpl;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.EmbeddedServer.ConnectionFactoryProvider;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.runtime.TestEmbeddedServer;
import org.teiid.runtime.util.ConvertVDB;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.file.FileExecutionFactory;
import org.teiid.vdb.runtime.VDBKey;

@SuppressWarnings("nls")
public class TestDDLMetadataStore {

    static ExtendedEmbeddedServer es;

    @Before
    public void setup() {
        FileUtils.removeDirectoryAndChildren(new File(UnitTestUtil.getTestScratchPath()));
        es = new ExtendedEmbeddedServer();
    }

    @After
    public void teardown() {
        if (es != null) {
            es.stop();
        }
    }

    static final class ExtendedEmbeddedServer extends EmbeddedServer {
        @Override
        public Admin getAdmin() {
            return new DatasourceAwareEmbeddedAdmin(this);
        }

        @Override
        public VDBRepository getVDBRepository() {
            return super.getVDBRepository();
        }
    }

    private static class DatasourceAwareEmbeddedAdmin extends EmbeddedAdminImpl {
        HashSet<String> datasourceNames = new HashSet<String>();

        public DatasourceAwareEmbeddedAdmin(EmbeddedServer embeddedServer) {
            super(embeddedServer);
        }

        @Override
        public void createDataSource(String deploymentName, String templateName, Properties properties)
                throws AdminException {
            if (deploymentName.equals("z") && templateName.equals("custom")) { // custom name comes from ddl
                final AtomicInteger counter = new AtomicInteger();
                ConnectionFactoryProvider<AtomicInteger> cfp =
                        new EmbeddedServer.SimpleConnectionFactoryProvider<AtomicInteger>(counter);
                es.addConnectionFactoryProvider(deploymentName, cfp);
                datasourceNames.add(deploymentName);
            }
        }

        @Override
        public Collection<? extends PropertyDefinition> getTemplatePropertyDefinitions(String templateName)
                throws AdminException {
            return Collections.emptyList();
        }

        @Override
        public Properties getDataSource(String deployedName) throws AdminException {
            throw new AdminProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40137, "getDataSource")); //$NON-NLS-1$
        }

        @Override
        public void deleteDataSource(String deployedName) throws AdminException {
            if (deployedName.equals("z")) {
                // TODO: no remove on method on server
                datasourceNames.remove(deployedName);
            }
        }

        @Override
        public Collection<String> getDataSourceNames() throws AdminException {
            return this.datasourceNames;
        }

        @Override
        public Set<String> getDataSourceTemplateNames() throws AdminException {
            HashSet<String> names = new HashSet<String>();
            names.add("custom");
            return names;
        }
    }

    private static class SimplePrincipal extends Identity {
        private SimplePrincipal(String name) {
            super(name);
        }
    }

    private static class SimpleGroup extends SimplePrincipal implements Group {
        private HashSet<Principal> members = new HashSet<>();

        private SimpleGroup(String name) {
            super(name);
        }

        @Override
        public boolean addMember(Principal user) {
            return members.add(user);
        }

        @Override
        public boolean isMember(Principal member) {
            return members.contains(member);
        }

        @Override
        public Enumeration<? extends Principal> members() {
            return Collections.enumeration(members);
        }

        @Override
        public boolean removeMember(Principal user) {
            return members.remove(user);
        }
    }



    public static class ThreadLocalSecurityHelper implements SecurityHelper {

        private static ThreadLocal<Subject> threadLocalContext = new ThreadLocal<Subject>();

        @Override
        public Object associateSecurityContext(Object context) {
            Object previous = threadLocalContext.get();
            threadLocalContext.set((Subject)context);
            return previous;
        }

        @Override
        public Object getSecurityContext(String securityDomain) {
            return threadLocalContext.get();
        }

        @Override
        public void clearSecurityContext() {
            threadLocalContext.remove();
        }

        @Override
        public Object authenticate(String securityDomain,
                String baseUserName, Credentials credentials,
                String applicationName) throws LoginException {
            Subject subject = new Subject();
            subject.getPrincipals().add(new SimplePrincipal("superuser"));

            SimpleGroup rolesGroup = new SimpleGroup("Roles");
            rolesGroup.addMember(new SimplePrincipal("superuser"));
            rolesGroup.addMember(new SimplePrincipal("admin"));
            subject.getPrincipals().add(rolesGroup);
            return subject;
        }

        @Override
        public Subject getSubjectInContext(Object context) {
            return (Subject)context;
        }

        @Override
        public GSSResult negotiateGssLogin(String securityDomain,
                byte[] serviceTicket) throws LoginException {
            return null;
        }

    }

    @Test
    public void testVDBExport() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        ec.setSecurityHelper(new ThreadLocalSecurityHelper());
        es.addTranslator("y", new TestEmbeddedServer.FakeTranslator(false));
        es.start(ec);

        final AtomicInteger counter = new AtomicInteger();
        ConnectionFactoryProvider<AtomicInteger> cfp = new EmbeddedServer.SimpleConnectionFactoryProvider<AtomicInteger>(counter);
        es.addConnectionFactoryProvider("z", cfp);
        es.addMetadataRepository("myrepo", Mockito.mock(MetadataRepository.class));

        es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataPath()+"/first-db.ddl"), true);

        Admin admin = es.getAdmin();
        VDBMetaData vdb = (VDBMetaData)admin.getVDB("empty", "2");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        VDBMetadataParser.marshall(vdb, out);

        String expected = ObjectConverterUtil
                .convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/" + "first-vdb.xml"));
        assertEquals(expected, new String(out.toByteArray()));

        String exportedDdl = admin.getSchema("empty", "2", null, null, null);
        Assert.assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("first-vdb.ddl")),
                exportedDdl);
    }

    @Test
    public void testRoles() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        ec.setSecurityHelper(new ThreadLocalSecurityHelper());
        es.addTranslator("y", new TestEmbeddedServer.FakeTranslator(false));
        es.addTranslator("y2", new TestEmbeddedServer.FakeTranslator(false));

        final AtomicInteger counter = new AtomicInteger();
        ConnectionFactoryProvider<AtomicInteger> cfp =
                new EmbeddedServer.SimpleConnectionFactoryProvider<AtomicInteger>(counter);
        es.addConnectionFactoryProvider("z", cfp);

        es.start(ec);
        es.addMetadataRepository("myrepo", Mockito.mock(MetadataRepository.class));
        es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataPath()+"/first-db.ddl"), true);

        TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:empty", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from mytable");
        assertFalse(rs.next());
        assertEquals("my-column", rs.getMetaData().getColumnLabel(1));

        s.execute("update mytable set \"my-column\" = 'a'");
        assertEquals(2, s.getUpdateCount());

        try {
            s.execute("delete from mytable where \"my-column\" = 'a'");
            fail("should have stopped by roles");
        } catch(Exception e) {
            //pass
        }
    }

    @Test
    public void testConvertVDBXML() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.addTranslator("file", new FileExecutionFactory());
        es.addTranslator("h2", new ExecutionFactory<>());
        es.start(ec);

        FileInputStream vdb = new FileInputStream(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.xml");
        es.deployVDB(vdb);

        String content = ConvertVDB.convert(new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.xml"));

        es.undeployVDB("Portfolio");

        /*
        FileWriter fw = new FileWriter(new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.ddl"));
        fw.write(content);
        fw.close();
        */

        String expected = ObjectConverterUtil
                .convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.ddl"));
        assertEquals(expected, content);

        //make sure the output is not valid
        try {
            es.getAdmin().deploy("portfolio-vdb.ddl", new ByteArrayInputStream(content.getBytes("UTF-8")));
            fail();
        } catch (AdminProcessingException e) {

        }
    }

    @Test
    public void testMigrateVDBXML() throws Exception {
        File vdb = new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.xml");
        String content = ConvertVDB.convert(vdb);
        /*
        FileWriter fw = new FileWriter(new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-converted-vdb.ddl"));
        fw.write(content);
        fw.close();
        */
        String expected = ObjectConverterUtil
                .convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-converted-vdb.ddl"));
        assertEquals(expected, content);
    }

    @Test
    public void testOverideTranslator() throws Exception {
        File vdb = new File(UnitTestUtil.getTestDataPath() + "/" + "override-vdb.xml");
        String content = ConvertVDB.convert(vdb);
        /*
        FileWriter fw = new FileWriter(new File(UnitTestUtil.getTestDataPath() + "/" + "override-vdb.ddl"));
        fw.write(content);
        fw.close();
        */
        String expected = ObjectConverterUtil
                .convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/" + "override-vdb.ddl"));
        assertEquals(expected, content);
    }

    @Test
    public void testMultiSource() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);
        es.addTranslator(FileExecutionFactory.class);

        es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataPath() + "/" + "multisource-vdb.ddl"), true);

        es.getAdmin().addSource("multisource", "1", "MarketData", "x", "file", "z");

        Connection c = es.getDriver().connect("jdbc:teiid:multisource", null);
        DatabaseMetaData dmd = c.getMetaData();
        ResultSet rs = dmd.getProcedureColumns(null, null, "deleteFile", null);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(2, count);
    }

    @Test
    public void testPolicies() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.addTranslator("file", new FileExecutionFactory());
        es.addTranslator("h2", new ExecutionFactory<>());
        es.start(ec);

        FileInputStream vdb = new FileInputStream(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.xml");
        es.deployVDB(vdb);

        CompositeVDB cvdb = es.getVDBRepository().getCompositeVDB(new VDBKey("portfolio", 1));
        TransformationMetadata metadata = cvdb.getVDB().getAttachment(TransformationMetadata.class);
        assertEquals(
                "[Accounts[R], Accounts.Account.SSN[ mask null], Accounts.Customer[ condition state <> 'New York'], Accounts.Customer.SSN[ mask null], MarketData[R], Stocks[R], Stocks.StockPrices.Price[ mask CASE WHEN hasRole('Prices') = true THEN Price END order 1]]",
                metadata.getPolicies().get("ReadOnly").getPermissions().toString());
    }
}
