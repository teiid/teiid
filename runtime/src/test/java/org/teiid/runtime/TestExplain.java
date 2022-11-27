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

package org.teiid.runtime;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("nls")
public class TestExplain {

    private static EmbeddedServer es = new EmbeddedServer();
    private Connection conn;

    @BeforeClass public static void beforeClass() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.deployVDB(new ByteArrayInputStream(("CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test; SET SCHEMA test;"
                + "CREATE VIEW test as select 1").getBytes()), true);
    }

    @Before public void before() throws Exception {
        conn = es.getDriver().connect("jdbc:teiid:test", null);
    }

    @AfterClass public static void afterClass() {
        es.stop();
    }

    @After public void after() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Test public void testExplainXml() throws SQLException {
        try (Statement statement = conn.createStatement();) {
            ResultSet rs = statement.executeQuery("explain (format xml) select * from test");
            rs.next();
            String plan = rs.getString(1);
            assertFalse(plan.contains("Statistics"));
        }
    }

    @Test public void testExplainAnalyzeYaml() throws SQLException {
        try (Statement statement = conn.createStatement();) {
            ResultSet rs = statement.executeQuery("explain (analyze true, format yaml) select * from test");
            rs.next();
            String plan = rs.getString(1);
            assertTrue(plan.contains("- Node Output Rows: 1"));
        }
    }

    @Test public void testExplainAnalyzeText() throws SQLException {
        try (Statement statement = conn.createStatement();) {
            ResultSet rs = statement.executeQuery("explain (analyze) select * from test");
            rs.next();
            String plan = rs.getString(1);
            assertTrue(plan.contains("0: Node Output Rows: 1"));
        }
    }

    @Test public void testExplainAnalyzeWithGeneratedKey() throws SQLException {
        try (Statement statement = conn.createStatement();) {
            statement.execute("create local temporary table t (id serial, val string, primary key (id))");
            int result = statement.executeUpdate("explain (analyze) insert into t (val) values ('a')", Statement.RETURN_GENERATED_KEYS);
            assertEquals(0, result);
            ResultSet keys = statement.getGeneratedKeys();
            assertFalse(keys.next()); //can't retrieve them

            //works without explain
            result = statement.executeUpdate("insert into t (val) values ('a')", Statement.RETURN_GENERATED_KEYS);
            keys = statement.getGeneratedKeys();
            assertTrue(keys.next());

            //TODO: this is a small nuance of the implementation,
            //it expects updates still not to be executed by executeQuery
            //ResultSet planRs = statement.executeQuery("explain (analyze) insert into t (val) values ('a')");
        }
    }

    @Test public void testPgSubquery() throws Exception {
        Statement s = conn.createStatement();
        s.execute("explain SELECT pg_get_constraintdef((select oid from pg_constraint where contype = 'f' and conrelid = (select oid from pg_class where relname = 'Functions')), true)");
        ResultSet rs = s.getResultSet();
        rs.next();
        assertTrue(rs.getString(1).contains("ProjectNode"));
    }

}
