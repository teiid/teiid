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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;

@SuppressWarnings("nls")
public class TestStats {

    static Connection connection;
    private static FakeServer server;
    static final String VDB = "PartsSupplier";

    @BeforeClass public static void setUp() throws Exception {
        server = new FakeServer(true);
        server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
        connection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @AfterClass public static void tearDown() throws SQLException {
        connection.close();
        server.stop();
    }

    @Test public void testSetTableStats() throws Exception {
        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("select cardinality from sys.tables where name = 'PARTSSUPPLIER.PARTS'");
        rs.next();
        assertEquals(16, rs.getInt(1));
        s.execute("call setTableStats(tableName=>'partssupplier.partssupplier.parts', cardinality=>32)");
        rs = s.executeQuery("select cardinality from sys.tables where name = 'PARTSSUPPLIER.PARTS'");
        rs.next();
        assertEquals(32, rs.getInt(1));
        s.execute("call setTableStats(tableName=>'partssupplier.partssupplier.parts', cardinality=>321100000000)");
        rs = s.executeQuery("select cardinality from sys.tables where name = 'PARTSSUPPLIER.PARTS'");
        rs.next();
        assertEquals(Integer.MAX_VALUE, rs.getInt(1));
    }

    @Test public void testSetColumnStats() throws Exception {
        Statement s = connection.createStatement();
        ResultSet rs = s.executeQuery("select MinRange, MaxRange, DistinctCount, NullCount from sys.columns where name = 'PART_ID'");
        rs.next();
        assertEquals(null, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(-1, rs.getInt(3));
        assertEquals(-1, rs.getInt(4));
        s.execute("call setColumnStats(tableName=>'partssupplier.partssupplier.parts', columnName=>'PART_ID', max=>32, nullcount=>0)");
        rs = s.executeQuery("select MinRange, MaxRange, DistinctCount, NullCount from sys.columns where name = 'PART_ID'");
        rs.next();
        assertEquals(null, rs.getString(1));
        assertEquals("32", rs.getString(2));
        assertEquals(-1, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
    }

    @Test(expected=SQLException.class) public void testSetColumnStatsInvalidColumn() throws Exception {
        Statement s = connection.createStatement();
        s.execute("call setColumnStats(tableName=>'partssupplier.partssupplier.parts', columnName=>'foo', max=>32, nullcount=>0)");
    }
}
