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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Types;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.plan.PlanNode.Property;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.analysis.AnalysisRecord;


@SuppressWarnings("nls")
public class TestQueryPlans {
    static FakeServer server;
    private static Connection conn;

    @BeforeClass public static void setUp() throws Exception {
        server = new FakeServer(true);
        server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
        conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @AfterClass public static void tearDown() throws Exception {
        conn.close();
        server.stop();
    }

    @Test public void testNoExec() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set noexec on");
        ResultSet rs = s.executeQuery("select * from all_tables");
        assertFalse(rs.next());
        s.execute("SET NOEXEC off");
        rs = s.executeQuery("select * from all_tables");
        assertTrue(rs.next());
    }

    @Test public void testShowPlan() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set showplan on");
        ResultSet rs = s.executeQuery("select * from all_tables");
        assertNull(s.unwrap(TeiidStatement.class).getDebugLog());

        rs = s.executeQuery("show plan");
        assertTrue(rs.next());
        assertEquals(rs.getMetaData().getColumnType(1), Types.CLOB);
        assertTrue(rs.getString(1).startsWith("ProjectNode"));
        SQLXML plan = rs.getSQLXML(2);
        assertTrue(plan.getString().startsWith("<?xml"));
        assertNull(rs.getObject("DEBUG_LOG"));
        assertNotNull(rs.getObject("PLAN_TEXT"));

        s.execute("SET showplan debug");
        rs = s.executeQuery("select * from all_tables");
        assertNotNull(s.unwrap(TeiidStatement.class).getDebugLog());
        PlanNode node = s.unwrap(TeiidStatement.class).getPlanDescription();
        Property p = node.getProperty(AnalysisRecord.PROP_DATA_BYTES_SENT);
        assertEquals("20", p.getValues().get(0));

        rs = s.executeQuery("show plan");
        assertTrue(rs.next());
        assertNotNull(rs.getObject("DEBUG_LOG"));

        s.execute("SET showplan off");
        rs = s.executeQuery("select * from all_tables");
        assertNull(s.unwrap(TeiidStatement.class).getPlanDescription());
        assertTrue(rs.next());
    }

    @Test public void testShowPlanMultibatch() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set showplan debug");
        ResultSet rs = s.executeQuery("with x as( select * from sys.columns limit 50) select * from x t1, x t2");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(2500, count);
        rs = s.executeQuery("show plan");
        assertTrue(rs.next());
        assertEquals(rs.getMetaData().getColumnType(2), Types.SQLXML);
        String string = rs.getSQLXML(2).getString();
        PlanNode node = PlanNode.fromXml(string);

        Property p = node.getProperty("Statistics");
        assertTrue(p.getValues().contains("Node Output Rows: 2500"));
    }

    @Test public void testShow() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("show all");
        assertTrue(rs.next());
        assertNotNull(rs.getString("NAME"));

        s.execute("set showplan on");

        rs = s.executeQuery("show showplan");
        rs.next();
        assertEquals("on", rs.getString(1));
    }

}
