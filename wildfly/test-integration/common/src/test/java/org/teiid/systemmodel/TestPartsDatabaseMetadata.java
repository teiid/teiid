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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;


/**
 * Test the DatabaseMetadata results using the Parts VDB.
 */
@SuppressWarnings("nls")
public class TestPartsDatabaseMetadata {

    static DatabaseMetaData dbMetadata;
    static Connection connection;
    static FakeServer server;
    static final String VDB = "PartsSupplier";

    @BeforeClass public static void setUp() throws Exception {
        server = new FakeServer(true);
        server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
        connection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$
        dbMetadata = connection.getMetaData();
    }

    @AfterClass public static void tearDown() throws SQLException {
        connection.close();
        server.stop();
    }

    @Test public void testExportedKeys()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getExportedKeys(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testImportedKeys()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getImportedKeys(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testPrimaryKeys()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getPrimaryKeys(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcedures()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getProcedures(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcedureColumns()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getProcedureColumns(VDB, null, "%", "%")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testUDTs()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getUDTs(VDB, null, "%", null)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIndexInfo()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getIndexInfo(VDB, null, "%", true, true)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIndexInfoAll()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getIndexInfo(VDB, null, "%", false, true)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCrossReference()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getCrossReference(VDB, null, "%", VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testTypeInfo()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getTypeInfo()); //$NON-NLS-1$
    }

    @Test public void testCatalogs()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getCatalogs()); //$NON-NLS-1$
    }

    @Test public void testSchemas()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getSchemas()); //$NON-NLS-1$
    }

    @Test public void testTables()  throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getTables(VDB, null, "%", null)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testColumns() throws Exception {
        TestMMDatabaseMetaData.compareResultSet(dbMetadata.getColumns(VDB, null, "%", "%")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}
