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

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;


@SuppressWarnings("nls")
public class TestResultSetMetadata extends AbstractMMQueryTestCase {
    FakeServer server;
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

    public TestResultSetMetadata() {
        // this is needed because the result files are generated
        // with another tool which uses tab as delimiter
        super.DELIMITER = "\t"; //$NON-NLS-1$
    }

    @Before public void setUp() throws Exception {
        server = new FakeServer(true);
        server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
        this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @After public void tearDown() throws SQLException {
        closeConnection();
        server.stop();
    }

    private void executeTest(String sql, String[] expected) throws Exception {
        execute(sql);
        java.sql.ResultSet rs = this.internalResultSet;
        assertResultsSetMetadataEquals(rs.getMetaData(), expected);
    }

    private void executePreparedTest(String sql, String[] expected) throws Exception {
        execute(sql, new Object[] {});
        java.sql.ResultSet rs = this.internalResultSet;
        assertResultsSetMetadataEquals(rs.getMetaData(), expected);
    }

    @Test public void testCount()  throws Exception {
        String[] expected = {
                "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",     //$NON-NLS-1$
                "expr1	4	integer	java.lang.Integer	1	null	null	PartsSupplier" //$NON-NLS-1$
        };
        executeTest("select count(*) from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testStar()  throws Exception {
        String[] expected = {
            "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",     //$NON-NLS-1$
            "PART_ID	12	string	java.lang.String	0	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
            "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
            "PART_COLOR	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
            "PART_WEIGHT	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
        };
        executeTest("select * from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testTempGroupStar()  throws Exception {
        String[] expected = {
            "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",     //$NON-NLS-1$
            "PART_ID	12	string	java.lang.String	0	foo	null	PartsSupplier", //$NON-NLS-1$
            "PART_NAME	12	string	java.lang.String	1	foo	null	PartsSupplier", //$NON-NLS-1$
            "PART_COLOR	12	string	java.lang.String	1	foo	null	PartsSupplier", //$NON-NLS-1$
            "PART_WEIGHT	12	string	java.lang.String	1	foo	null	PartsSupplier" //$NON-NLS-1$
        };
        executeTest("select * from (select * from parts) foo where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testCountAndElement()  throws Exception {
        String[] expected = {
            "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",     //$NON-NLS-1$
            "expr1	4	integer	java.lang.Integer	1	null	null	PartsSupplier", //$NON-NLS-1$
            "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
        };
        executeTest("select count(*), part_name from parts where 1=0 group by part_name", expected); //$NON-NLS-1$
    }

    @Test public void testStar_PreparedStatement()  throws Exception {
        String[] expected = {
            "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
            "PART_ID	12	string	java.lang.String	0	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
            "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
            "PART_COLOR	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
            "PART_WEIGHT	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
        };
        executePreparedTest("select * from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testCount_PreparedStatement()  throws Exception {
        String[] expected = {
            "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
            "expr1	4	integer	java.lang.Integer	1	null	null	PartsSupplier" //$NON-NLS-1$
        };
        executePreparedTest("select count(*) from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testCountAndElement_PreparedStatement()  throws Exception {
        String[] expected = {
            "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
            "expr1	4	integer	java.lang.Integer	1	null	null	PartsSupplier", //$NON-NLS-1$
            "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
        };
        executePreparedTest("select count(*), part_name from parts where 1=0 group by part_name", expected); //$NON-NLS-1$
    }
}
