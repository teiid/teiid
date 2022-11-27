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

package org.teiid.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.security.Identity;
import java.security.Principal;
import java.security.acl.Group;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Vector;

import javax.security.auth.Subject;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Policy.Operation;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.runtime.DoNothingSecurityHelper;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestRowBasedSecurity {

    private EmbeddedServer es;

    @After public void tearDown() {
        es.stop();
    }

    @Test public void testSecurity() throws Exception {
        es = new EmbeddedServer();
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        final Vector<Principal> v = new Vector<Principal>();
        v.add(new Identity("myrole") {});
        final Subject subject = new Subject();
        Group g = Mockito.mock(Group.class);
        Mockito.stub(g.getName()).toReturn("Roles");
        Mockito.stub(g.members()).toReturn((Enumeration) v.elements());
        subject.getPrincipals().add(g);
        ec.setSecurityHelper(new DoNothingSecurityHelper() {

            @Override
            public Subject getSubjectInContext(Object context) {
                return subject;
            }
        });
        es.start(ec);
        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
            @Override
            public void getMetadata(MetadataFactory metadataFactory, Object conn) throws TranslatorException {
                Table t = metadataFactory.addTable("x");
                Column col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, t);
                metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, t);
                metadataFactory.addPermission("y", t, null, null, Boolean.TRUE, null, null, null, "col = 'a'", null);
                metadataFactory.addColumnPermission("y", col, null, null, null, null, "null", null);

                t = metadataFactory.addTable("y");
                col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, t);
                metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, t);
                metadataFactory.addPolicy("z", "col policy", t, "col = 'e'", null, Operation.ALL);
                metadataFactory.addPermission("z", t, null, null, null, null, null, null, "col = 'e'", null);

                Table v = metadataFactory.addTable("v");
                metadataFactory.addPermission("y", v, null, null, Boolean.TRUE, null, null, null, null, null);
                col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, v);
                metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, v);
                v.setTableType(Type.View);
                v.setVirtual(true);
                v.setSelectTransformation("/*+ cache(scope:session) */ select col, col2 from y");

                Table v_mat = metadataFactory.addTable("v_mat");
                metadataFactory.addPermission("y", v_mat, null, null, Boolean.TRUE, null, null, null, null, null);
                col = metadataFactory.addColumn("col", TypeFacility.RUNTIME_NAMES.STRING, v_mat);
                metadataFactory.addColumn("col2", TypeFacility.RUNTIME_NAMES.STRING, v_mat);
                v_mat.setTableType(Type.View);
                v_mat.setVirtual(true);
                v_mat.setMaterialized(true);
                v_mat.setSelectTransformation("select col, col2 from x union all select col, col2 from y");
            }
            @Override
            public boolean isSourceRequiredForMetadata() {
                return false;
            }
        };
        hcef.addData("SELECT x.col, x.col2 FROM x", Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c", "d")));
        hcef.addData("SELECT y.col, y.col2 FROM y", Arrays.asList(Arrays.asList("e", "f"), Arrays.asList("h", "g")));
        hcef.addData("SELECT x.col2, x.col FROM x", Arrays.asList(Arrays.asList("b", "a"), Arrays.asList("d", "c")));
        hcef.addData("SELECT y.col FROM y", Arrays.asList(Arrays.asList("e"), Arrays.asList("h")));
        hcef.addData("SELECT x.col FROM x", Arrays.asList(Arrays.asList("a"), Arrays.asList("c")));

        es.addTranslator("hc", hcef);
        es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("roles-vdb.xml")));

        Connection c = es.getDriver().connect("jdbc:teiid:z;PassthroughAuthentication=true", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from x");
        rs.next();
        assertEquals(null, rs.getString(1)); //masking
        assertEquals("b", rs.getString(2));
        assertFalse(rs.next()); //row filter
        rs.close();

/*
        rs = s.executeQuery("select lookup('myschema.x', 'col', 'col2', 'b')");
        rs.next();
        assertEquals(null, rs.getString(1)); //should still be null for this session
*/
        s = c.createStatement();
        rs = s.executeQuery("select count(col2) from v where col is not null");
        rs.next();
        assertEquals(1, rs.getInt(1));

        //with or without caching we should get the same answer

        s = c.createStatement();
        rs = s.executeQuery("select count(*) from v_mat");
        rs.next();
        assertEquals(2, rs.getInt(1));

        s = c.createStatement();
        rs = s.executeQuery("select count(*) from v_mat option nocache");
        rs.next();
        assertEquals(2, rs.getInt(1));

        //different session with different roles
        v.clear();
        c = es.getDriver().connect("jdbc:teiid:z;PassthroughAuthentication=true", null);
        s = c.createStatement();
        rs = s.executeQuery("select count(col2) from v where col is not null");
        rs.next();
        assertEquals(2, rs.getInt(1));

        /*s = c.createStatement();
        rs = s.executeQuery("select count(*) from v_mat");
        rs.next();
        assertEquals(3, rs.getInt(1));*/
    }

    @Test public void testDdlSecurity() throws Exception {
        es = new EmbeddedServer();
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        final Vector<Principal> v = new Vector<Principal>();
        v.add(new Identity("myrole") {});
        final Subject subject = new Subject();
        Group g = Mockito.mock(Group.class);
        Mockito.stub(g.getName()).toReturn("Roles");
        Mockito.stub(g.members()).toReturn((Enumeration) v.elements());
        subject.getPrincipals().add(g);
        ec.setSecurityHelper(new DoNothingSecurityHelper() {

            @Override
            public Subject getSubjectInContext(Object context) {
                return subject;
            }
        });
        es.start(ec);
        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory();
        hcef.addData("SELECT ACCOUNT.id FROM ACCOUNT", Arrays.asList(Arrays.asList(1), Arrays.asList(2)));

        es.addTranslator("hc", hcef);

        es.deployVDB(new ByteArrayInputStream(("CREATE DATABASE test VERSION '1.0.0';\n" +
                "USE DATABASE test VERSION '1.0.0';\n" +
                "\n" +
                "CREATE ROLE user_role  WITH FOREIGN ROLE myrole;\n" +
                "\n" +
                "CREATE SERVER accounts FOREIGN DATA WRAPPER hc;\n" +
                "CREATE SCHEMA public SERVER accounts;\n" +
                "SET SCHEMA public;\n" +
                "\n" +
                "CREATE FOREIGN TABLE ACCOUNT (id integer, SSN string);\n" +
                "\n" +
                "GRANT SELECT ON TABLE public.ACCOUNT TO user_role;\n" +
                "GRANT SELECT ON COLUMN public.ACCOUNT.SSN\n" +
                "    MASK '''xxxx'''\n" +
                "    TO user_role;").getBytes(Charset.forName("UTF-8"))), true);

        Connection c = es.getDriver().connect("jdbc:teiid:test;PassthroughAuthentication=true", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select id, ssn from account");
        rs.next();
        assertEquals("xxxx", rs.getString(2));
    }

}
