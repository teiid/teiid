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

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("nls")
public class TestVirtualFunctionPushdown {

    private static EmbeddedServer es = new EmbeddedServer();
    private Connection conn;
    private static HardCodedExecutionFactory ef;

    @BeforeClass public static void beforeClass() throws Exception {
        es.start(new EmbeddedConfiguration());
        ef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsSelectExpression() {
                return true;
            }
        };
        es.addTranslator("hardcoded", ef);
        es.deployVDB(new ByteArrayInputStream(("CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE SERVER s FOREIGN DATA WRAPPER hardcoded;"
                + "CREATE VIRTUAL SCHEMA v; "
                + "CREATE SCHEMA p SERVER s; "
                + "SET SCHEMA v;"
                + "CREATE VIRTUAL FUNCTION VFUNC(msg varchar) RETURNS varchar AS return msg || 'a';"
                + "CREATE VIRTUAL FUNCTION VFUNC1(msg varchar) RETURNS varchar AS return msg || 'a';"
                + "SET SCHEMA p;"
                + "CREATE FOREIGN FUNCTION SFUNC(msg varchar) "
                + " RETURNS varchar OPTIONS (\"teiid_rel:virtual-function\" 'v.vfunc');"
                + "CREATE FOREIGN FUNCTION IFUNC(msg integer) RETURNS varchar OPTIONS (\"teiid_rel:virtual-function\" 'v.vfunc1');"
                + "CREATE FOREIGN FUNCTION SYSFUNC(msg string) RETURNS integer OPTIONS (\"teiid_rel:virtual-function\" 'ascii');"
                + "CREATE FOREIGN TABLE TBL (COL STRING);").getBytes()), true);
    }

    @Before public void before() throws Exception {
        conn = es.getDriver().connect("jdbc:teiid:test", null);
        ef.clearData();
    }

    @AfterClass public static void afterClass() {
        es.stop();
    }

    @After public void after() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Test public void testPushdown() throws SQLException {
        //note the substitution of sfunc for vfunc
        ef.addData("SELECT SFUNC(TBL.COL) FROM TBL", Arrays.asList(Arrays.asList("a")));
        try (Statement statement = conn.createStatement();) {
            ResultSet rs = statement.executeQuery("select vfunc(col) from tbl");
            assertTrue(rs.next());
        }
    }

    @Test public void testNoPushdown() throws SQLException {
        ef.addData("SELECT TBL.COL FROM TBL", Arrays.asList(Arrays.asList("a")));
        try (Statement statement = conn.createStatement();) {
            ResultSet rs = statement.executeQuery("select vfunc1(col) from tbl");
            assertTrue(rs.next());
        }
    }

    @Test public void testSysPushdown() throws SQLException {
        ef.addData("SELECT SYSFUNC(TBL.COL) FROM TBL", Arrays.asList(Arrays.asList(1)));
        try (Statement statement = conn.createStatement();) {
            ResultSet rs = statement.executeQuery("select ascii(col) from tbl");
            assertTrue(rs.next());
        }
    }

}