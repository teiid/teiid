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
package org.teiid.metadatastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.jboss.security.SimpleGroup;
import org.jboss.security.SimplePrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.ExportFormat;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.metadata.MetadataRepository;
import org.teiid.runtime.*;
import org.teiid.runtime.EmbeddedServer.ConnectionFactoryProvider;
import org.teiid.runtime.util.ConvertVDB;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.file.FileExecutionFactory;

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
    
    public static class ThreadLocalSecurityHelper implements SecurityHelper {
        
        private static ThreadLocal<Subject> threadLocalContext = new ThreadLocal<Subject>();

        @Override
        public Object associateSecurityContext(Object context) {
            Object previous = threadLocalContext.get();
            threadLocalContext.set((Subject)context);
            return previous;
        }

        @Override
        public Object getSecurityContext() {
            return threadLocalContext.get();
        }

        @Override
        public void clearSecurityContext() {
            threadLocalContext.remove();
        }

        @Override
        public Subject getSubjectInContext(String securityDomain) {
            return threadLocalContext.get();
        }

        @Override
        public Object authenticate(String securityDomain,
                String baseUserName, Credentials credentials,
                String applicationName) throws LoginException {
            Subject subject = new Subject();
            subject.getPrincipals().add(new SimpleGroup("superuser"));
            
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
        VDBMetadataParser.marshell(vdb, out);
        
        String expected = ObjectConverterUtil
                .convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/" + "first-vdb.xml"));
        assertEquals(expected, new String(out.toByteArray()));
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
        
        Admin admin = es.getAdmin();
        String content = admin.getSchema("Portfolio", "1", null, null, null, ExportFormat.DDL);
        es.undeployVDB("Portfolio");
        
        /*
        FileWriter fw = new FileWriter(new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.ddl"));
        fw.write(content);
        fw.close();
        */
        
        String expected = ObjectConverterUtil
                .convertFileToString(new File(UnitTestUtil.getTestDataPath() + "/" + "portfolio-vdb.ddl"));
        assertEquals(expected, content);
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
}
