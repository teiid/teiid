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
package org.teiid.systemmodel;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;

@SuppressWarnings("nls")
public class TestUsage {

    static Connection connection;
    private static FakeServer server;

    @BeforeClass public static void setUp() throws Exception {
        server = new FakeServer(true);
        server.deployVDB(new ReaderInputStream(new StringReader("<vdb name=\"u1\" version=\"1.0.0\">"
                + "<model name=\"insideVirtualModel\" type=\"VIRTUAL\">\n" +
                "        <metadata type=\"DDL\">\n" +
                "            <![CDATA[\n" +
                "                CREATE VIEW v1 (v1col string) AS SELECT 'a' UNION ALL SELECT 'b';\n" +
                "                CREATE VIEW v2 (v2col string) AS SELECT v1col||'b' FROM v1;\n" +
                "                CREATE VIRTUAL PROCEDURE p1() RETURNS (p1col string) AS\n" +
                "                BEGIN\n" +
                "                    SELECT v2col FROM v2;\n" +
                "                END;\n" +
                "                CREATE VIRTUAL PROCEDURE p2() RETURNS (p2col string) AS\n" +
                "                BEGIN\n" +
                "                    exec p1();\n" +
                "                END;\n" +
                "                CREATE VIEW v7 (v7col string) AS SELECT p1.p1col FROM (CALL p1())AS p1;\n" +
                "                ]]>\n" +
                "        </metadata>\n" +
                "    </model></vdb>"), Charset.forName("UTF-8")), false);
        connection = server.createConnection("jdbc:teiid:u1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @AfterClass public static void tearDown() throws SQLException {
        connection.close();
        server.stop();
    }

    @Test public void testProcedureColumnUsage() throws Exception {
        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("SELECT * FROM SYSADMIN.Usage WHERE SchemaName='insideVirtualModel' AND Name='p1' AND object_type='Column' AND Uses_object_type='Column'");
        TestMMDatabaseMetaData.compareResultSet(rs);
        s.close();
    }

    @Test public void testProcedureColumnUsageOfProcedure() throws Exception {
        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("SELECT * FROM SYSADMIN.Usage WHERE SchemaName='insideVirtualModel' AND Name='p2' AND object_type='Column' AND Uses_object_type='Column'");
        TestMMDatabaseMetaData.compareResultSet(rs);
        s.close();
    }

}
