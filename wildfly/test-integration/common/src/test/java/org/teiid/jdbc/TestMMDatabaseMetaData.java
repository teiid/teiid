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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.CoreConstants;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.util.ResultSetUtil;
import org.teiid.net.ServerConnection;


/**
 */
@SuppressWarnings("nls")
public class TestMMDatabaseMetaData {

    private static final boolean REPLACE_EXPECTED = PropertiesUtils.getBooleanProperty(System.getProperties(), "replace_expected", false);
    private static final boolean WRITE_ACTUAL_RESULTS_TO_FILE = false;
    private static final boolean PRINT_RESULTSETS_TO_CONSOLE = false;

    private static final int MAX_COL_WIDTH = 65;

    public static void compareResultSet(ResultSet... rs) throws IOException, SQLException {
        StackTraceElement ste = new Exception().getStackTrace()[1];
        String testName = ste.getMethodName();
        String className = ste.getClassName();
        className = className.substring(className.lastIndexOf('.') + 1);
        testName = className + "/" + testName; //$NON-NLS-1$
        compareResultSet(testName, rs);
    }

    public static void compareResultSet(String testName, ResultSet... rs)
            throws FileNotFoundException, SQLException, IOException {
        FileOutputStream actualOut = null;
        try {
            StringWriter ps = new StringWriter();
            for (int i = 0; i < rs.length; i++) {
                ResultSetUtil.printResultSet(rs[i], MAX_COL_WIDTH, true, ps);
            }
            if (PRINT_RESULTSETS_TO_CONSOLE) {
               System.out.println(ps.toString());
            }
            if (REPLACE_EXPECTED) {
                File actual = new File(UnitTestUtil.getTestDataPath() + "/" +testName+".expected"); //$NON-NLS-1$ //$NON-NLS-2$
                actualOut = new FileOutputStream(actual);
                actualOut.write(ps.toString().getBytes("UTF-8"));
            } else {
                if (WRITE_ACTUAL_RESULTS_TO_FILE) {
                    File actual = new File(UnitTestUtil.getTestDataPath() + "/" +testName+".actual"); //$NON-NLS-1$ //$NON-NLS-2$
                    actualOut = new FileOutputStream(actual);
                }
                InputStreamReader isr = new InputStreamReader(new BufferedInputStream(new FileInputStream(UnitTestUtil.getTestDataPath() + "/"+testName+".expected"))); //$NON-NLS-1$ //$NON-NLS-2$
                assertEquals("Actual data did not match expected", //$NON-NLS-1$
                        ObjectConverterUtil.convertToString(isr),
                        ps.toString());
            }
        } finally {
            if (actualOut != null) {
                actualOut.close();
            }
        }
    }

    static Connection conn = null;

    DatabaseMetaData dbmd;
    private Map<String, Object> expectedMap = new HashMap<String, Object>();

    // constant
    private static final int NO_LIMIT = 0;

    //==============================================================================
    //  The following 2 constants are defined here to provide access to constants
    //  defined in the JDBC 3.0 implementation that comes with the 1.4 JRE.
    //  They are defined this way to allow this class to compile with the 1.3 JRE.
    //==============================================================================
    private static final int ResultSet_HOLD_CURSORS_OVER_COMMIT = 1; // ResultSet.HOLD_CURSORS_OVER_COMMIT == 1
    private static final int ResultSet_CLOSE_CURSORS_AT_COMMIT = 2; // ResultSet.CLOSE_CURSORS_AT_COMMIT  == 2
    private static FakeServer server;

    @Before
    public void setUp() throws Exception {
        dbmd = new DatabaseMetaDataImpl((ConnectionImpl) conn);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
        server.stop();
    }

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        server = new FakeServer(true);
        server.setThrowMetadataErrors(false); //there are invalid views due to aggregate datatype changes
        server.deployVDB("QT_Ora9DS", UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");
        conn = server.createConnection("jdbc:teiid:QT_Ora9DS"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** Test all the non-query methods */
    @Test
    public void testMethodsWithoutParams() throws Exception {
        Class<?> dbmdClass = dbmd.getClass();
        // non-query Methods return String, boolean or int
        Method[] methods = dbmdClass.getDeclaredMethods();

        expectedMap = getExpected();
        //SYS.out.println(" -- total method == " + methods.length + ", non-query == " + expectedMap.size());
        for (int i = 0; i < methods.length; i++) {
            if (expectedMap.containsKey(methods[i].getName())) {

                Object actualValue = null;
                Object expectedValue = null;
                Object expectedReturn = expectedMap.get(methods[i].getName());
                Object[] params = null;

                if (expectedReturn instanceof List<?>) {
                    // has input parameters
                    List<?> returned = (List) expectedReturn;
                    params = (Object[]) returned.get(1);
                    //SYS.out.println(" params == " + params[0]);
                    expectedValue = returned.get(0);
                    actualValue = methods[i].invoke(dbmd, params);
                } else {
                    // without params
                    expectedValue = expectedReturn;
                    actualValue = methods[i].invoke(dbmd, new Object[0]);
                }

                assertEquals(" Expected doesn't match with actual for method - " + //$NON-NLS-1$
                methods[i].getName(), expectedValue, actualValue);
                //SYS.out.println("method [" + methods[i].getName() + " ] 's expectedValue == " + expectedValue + " actual == " + actualValue);
            }
        }
    }

    /** Test all the methods that throw exception */
    @Test
    public void testMethodsWithExceptions() throws Exception {
        Class<?> dbmdClass = dbmd.getClass();
        Method[] methods = dbmdClass.getDeclaredMethods();

        expectedMap = new HashMap<String, Object>(); //none expected
        //SYS.out.println(" -- total method == " + methods.length + ", non-query == " + expectedMap.size());
        for (int i =0; i< methods.length; i++) {
            if (expectedMap.containsKey(methods[i].getName())) {
                methods[i].invoke(dbmd, new Object[0]);
            }
        }
    }

    /** test supportResultSetConcurrency() with params and test them with different input values */
    @Test
    public void testSupportResultSetConcurrency() throws Exception {
        boolean returned = true;
        String functionName = "supportResultSetConcurrency"; //$NON-NLS-1$

        // should return true;
        Object[] params =
            new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY), new Integer(ResultSet.CONCUR_READ_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params =
            new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE), new Integer(ResultSet.CONCUR_READ_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params =
            new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE), new Integer(ResultSet.CONCUR_UPDATABLE)};
        returned = false;
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY), new Integer(ResultSet.CONCUR_UPDATABLE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test supportResultSetHoldability() with params and test them with different input values */
    @Test
    public void testSupportResultSetHoldability() throws Exception {
        boolean returned = false;
        String functionName = "supportResultSetHoldability"; //$NON-NLS-1$

        // should return false;
        Object[] params = new Object[] { new Integer(ResultSet_HOLD_CURSORS_OVER_COMMIT)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return false;
        params = new Object[] { new Integer(ResultSet_CLOSE_CURSORS_AT_COMMIT)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test supportResultSetType() with params and test them with different input values */
    @Test
    public void testSupportResultSetType()  throws Exception {
        boolean returned = true;
        String functionName = "supportResultSetType"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        returned = false;
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test supportsTransactionIsolationLevel() with params and test them with different input values */
    @Test
    public void testSupportsTransactionIsolationLevel()  throws Exception {
        boolean returned = false;
        String functionName = "supportsTransactionIsolationLevel"; //$NON-NLS-1$

        // should return true;
        //TODO: what is transaction isolation level?
        Object[] params = new Object[] { new Integer(1)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test deletesAreDetected() with params and test them with different input values */
    @Test
    public void testDeletesAreDetected() throws Exception {
        boolean returned = false;
        String functionName = "deletesAreDetected"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test insertsAreDetected() with params and test them with different input values */
    @Test
    public void testInsertsAreDetected() throws Exception {
        boolean returned = false;
        String functionName = "insertsAreDetected"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test updatesAreDetected() with params and test them with different input values */
    @Test
    public void testUpdatesAreDetected() throws Exception {
        boolean returned = false;
        String functionName = "updatesAreDetected"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test ownUpdatesAreVisible() with params and test them with different input values */
    @Test
    public void testOwnUpdatesAreVisible() throws Exception {
        boolean returned = false;
        String functionName = "ownUpdatesAreVisible"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test ownInsertsAreVisible() with params and test them with different input values */
    @Test
    public void testOwnInsertsAreVisible() throws Exception {
        boolean returned = false;
        String functionName = "ownInsertsAreVisible"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test othersUpdatesAreVisible() with params and test them with different input values */
    @Test
    public void testOthersUpdatesAreVisible() throws Exception {
        boolean returned = false;
        String functionName = "othersUpdatesAreVisible"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test othersInsertsAreVisible() with params and test them with different input values */
    @Test
    public void testOthersInsertsAreVisible() throws Exception {
        boolean returned = false;
        String functionName = "othersInsertsAreVisible"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test othersInsertsAreVisible() with params and test them with different input values */
    @Test
    public void testOthersDeletesAreVisible() throws Exception {
        boolean returned = false;
        String functionName = "othersDeletesAreVisible"; //$NON-NLS-1$

        // should return true;
        Object[] params = new Object[] { new Integer(ResultSet.TYPE_FORWARD_ONLY)};
        helpTestMethodsWithParams(functionName, returned, params);

        // shoud return true;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_INSENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);

        // should return false;
        params = new Object[] { new Integer(ResultSet.TYPE_SCROLL_SENSITIVE)};
        helpTestMethodsWithParams(functionName, returned, params);
    }

    /** test overloaded method supportsConvert() with params and test them with different input values */
    @Test
    public void testSupportsConvert1() throws Exception {
        assertEquals("Expected doesn't match with actual for method - supportsConvert()", //$NON-NLS-1$
        true, dbmd.supportsConvert());
    }

    /** test overloaded method supportsConvert() without params */
    @Test
    public void testSupportsConvert2() throws Exception {
        // should succeed
        helpTestSupportsConverts(Types.CHAR, Types.CHAR, true);
        helpTestSupportsConverts(Types.CHAR, Types.VARCHAR, true);
        helpTestSupportsConverts(Types.CHAR, Types.LONGVARCHAR, true);
        helpTestSupportsConverts(Types.CHAR, Types.BIT, true);
        helpTestSupportsConverts(Types.CHAR, Types.SMALLINT, true);
        helpTestSupportsConverts(Types.CHAR, Types.TINYINT, true);
        helpTestSupportsConverts(Types.CHAR, Types.INTEGER, true);
        helpTestSupportsConverts(Types.CHAR, Types.BIGINT, true);
        helpTestSupportsConverts(Types.CHAR, Types.FLOAT, true);
        helpTestSupportsConverts(Types.CHAR, Types.REAL, true);
        helpTestSupportsConverts(Types.CHAR, Types.DOUBLE, true);
        helpTestSupportsConverts(Types.CHAR, Types.NUMERIC, true);
        helpTestSupportsConverts(Types.CHAR, Types.DECIMAL, true);
        helpTestSupportsConverts(Types.CHAR, Types.DATE, true);
        helpTestSupportsConverts(Types.CHAR, Types.TIME, true);
        helpTestSupportsConverts(Types.CHAR, Types.TIMESTAMP, true);

        // should fail
        helpTestSupportsConverts(Types.DATE, Types.INTEGER, false);

        // TODO: there are other combination tests
        helpTestSupportsConverts(Types.CHAR, Types.BIGINT, true);
        helpTestSupportsConverts(Types.CHAR, Types.FLOAT, true);
        helpTestSupportsConverts(Types.CHAR, Types.REAL, true);
    }

    ////////////////////Query Related Methods///////////////////////////

    @Test
    public void testUcaseMatchReturnsRows() throws Exception {
        ResultSet rs = null;
        try {
            java.sql.Statement stmt = conn.createStatement();

            rs = stmt.executeQuery("SELECT Name FROM SYS.Tables WHERE UCASE(SchemaName) = 'SYS'"); //$NON-NLS-1$

            int count = 0;
            while(rs.next()) {
                count++;
            }
            assertEquals(16, count);
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
    }

    @Test
    public void testGetColumnsSingleMatch() throws Exception {
        ResultSet rs = dbmd.getColumns(null, "System", "VirtualDatabases", "Name"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs);
    }

    @Test
    public void testGetCatalogs() throws Exception {
        compareResultSet(dbmd.getCatalogs());
    }

    @Test
    public void testGetCrossReference() throws Exception {
        ResultSet rs = dbmd.getCrossReference(null, "BQT1", "SmallA", null, "BQT1", "SmallB");//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        ResultSet rs1 = dbmd.getCrossReference(null, "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSet rs2 = dbmd.getCrossReference("foo", "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        compareResultSet(rs, rs1, rs2);
    }

    @Test public void testGetImportedKeys() throws Exception {
        ResultSet rs = dbmd.getImportedKeys(null, "BQT1", "SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getImportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getImportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetExportedKeys() throws Exception {
        ResultSet rs = dbmd.getExportedKeys(null, "BQT1", "SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getExportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getExportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetIndexInfo() throws Exception {
        ResultSet rs = dbmd.getIndexInfo(null, "System", "KeyColumns", true, true); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getIndexInfo(null, "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getIndexInfo("foo", "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetPrimaryKeys() throws Exception {
        ResultSet rs = dbmd.getPrimaryKeys(null, "BQT1", "SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getPrimaryKeys(null, "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getPrimaryKeys("foo", "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetProcedureColumns() throws Exception {
        ResultSet rs = dbmd.getProcedureColumns(null, null, null, null);
        ResultSet rs1 = dbmd.getProcedureColumns(null, "Foo%", null, null); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getProcedureColumns("foo", "Foo%", null, null); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetProcedures() throws Exception {
        ResultSet rs = dbmd.getProcedures(null, null, null);
        ResultSet rs1 = dbmd.getProcedures(null, "Foo%", null); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getProcedures("foo", "Foo%", null); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetSchemas() throws Exception {
        compareResultSet(dbmd.getSchemas());
    }

    @Test
    public void testGetColumns() throws Exception {
        ResultSet rs = dbmd.getColumns(null, null, null, null);
        compareResultSet(rs);
    }

    @Test
    public void testGetColumns2() throws Exception {
        ResultSet rs = dbmd.getColumns(null, "QT_Ora%", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs);
    }

    @Test
    public void testGetColumns3() throws Exception {
        ResultSet rs = dbmd.getColumns(null, "Foo%", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs);
    }

    @Test
    public void testGetColumns4() throws Exception {
        ResultSet rs = dbmd.getColumns("foo", "Foo%", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
         compareResultSet(rs);
    }


    @Test
    public void testGetColumnPrivileges() throws Exception {
        ResultSet rs = dbmd.getColumnPrivileges(null, "Parts", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSet rs1 = dbmd.getColumnPrivileges(null, "%foo", null, null); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getColumnPrivileges("foo", "%foo", null, null); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetColumnPrivilegesResultSetMetaData() throws Exception {
        ResultSet rs = dbmd.getColumnPrivileges(null, "Parts", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs);
    }

    @Test
    public void testGetTablePrivileges() throws Exception {
        ResultSet rs = dbmd.getTablePrivileges(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs);
    }

    @Test
    public void testGetTablePrivilegesResultSetMetaData() throws Exception {
        ResultSet rs = dbmd.getTablePrivileges(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs);
    }

    @Test
    public void testGetTables_specificTable() throws Exception {
        ResultSet rs = dbmd.getTables(null, "SYSTEM", "VIRTUALDATABASES", null); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs);
    }

    @Test
    public void testGetTables_noTypes() throws Exception {
        ResultSet rs = dbmd.getTables(null, "SYSTEM", "VIRTUALDATABASES", new String[0]); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse(rs.next());
    }

    @Test
    public void testGetTables_specificTableTypes() throws Exception {
        String[] tables = new String[] { "Table" }; //$NON-NLS-1$
        ResultSet rs = dbmd.getTables(null, null, null, tables);
        compareResultSet(rs);
    }

    @Test
    public void testGetTables_specificTableMultipleTypes() throws Exception {
        String[] tables = new String[] { "Table", "View" }; //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs = dbmd.getTables(null, null, null, tables);
        compareResultSet(rs);
    }

    @Test
    public void testGetTables() throws Exception{
        ResultSet rs = dbmd.getTables(null, null, null, null);
        ResultSet rs1 = dbmd.getTables(null, "%foo", null, null); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getTables("foo", "%foo", null, null); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetTables_allTables() throws Exception {
        ResultSet rs = dbmd.getTables(null, null, null, null);
        compareResultSet(rs);
    }

    @Test
    public void testGetTableTypes() throws Exception {
        ResultSet rs = dbmd.getTableTypes();
        compareResultSet(rs);
    }

    @Test
    public void testGetTypeInfo_TotalNumber() throws Exception {
        ResultSet rs = dbmd.getTypeInfo();
        compareResultSet(rs);
    }

    @Test
    public void testGetUDTs() throws Exception{
        ResultSet rs = dbmd.getUDTs(null, null, "%blob%", null); //$NON-NLS-1$
        ResultSet rs1 = dbmd.getUDTs(null, "%foo", "%blob%", null); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getUDTs("foo", "%foo", "%blob%", null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetUDTs_specificTypeName() throws Exception {
        ResultSet rs = dbmd.getUDTs(null, null, "%blob%", null); //$NON-NLS-1$
        compareResultSet(rs);
    }

    @Test
    public void testGetVersionColumns() throws Exception {
        ResultSet rs = dbmd.getVersionColumns(null, null, null);
        ResultSet rs1 = dbmd.getVersionColumns(null, "Foo%", null); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getVersionColumns("foo", "Foo%", null); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetBestRowIdentifier() throws Exception {
        ResultSet rs = dbmd.getBestRowIdentifier(null, null, "SYS.VIRTUALDATABASES", //$NON-NLS-1$
                DatabaseMetaData.bestRowTemporary, true);
        ResultSet rs1 = dbmd.getBestRowIdentifier(null, "%foo", null, 1,true); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getBestRowIdentifier("foo", "%foo", null, 1,true); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetSuperTables() throws Exception {
        ResultSet rs = dbmd.getSuperTables(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs);
    }

    @Test
    public void testGetSuperTypes() throws Exception {
        ResultSet rs = dbmd.getSuperTypes(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs);
    }

    @Test
    public void testGetColumnsWithEscape() throws Exception {
        ResultSet columns = dbmd.getColumns("QT\\_Ora9DS", "BQT1", "SmallA", "IntKey"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        columns.next();
        assertEquals("BQT1", columns.getString(2));//$NON-NLS-1$
        assertFalse(columns.next());
    }

    @Test
    public void testGetCrossReferenceWithEscape() throws Exception {
        ResultSet rs = dbmd.getCrossReference(null, "QT\\_Ora9DS", "BQT1.SmallA", null, null, "BQT1.SmallB");//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSet rs1 = dbmd.getCrossReference(null, "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSet rs2 = dbmd.getCrossReference("foo", "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetExportedKeysWithEscape() throws Exception {
        ResultSet rs = dbmd.getExportedKeys(null, "QT\\_Ora9DS", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getExportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getExportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetImportedKeysWithEscape() throws Exception {
        ResultSet rs = dbmd.getImportedKeys(null, "QT\\_Ora9DS", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getImportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getImportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetIndexInfoWithEscape() throws Exception {
        ResultSet rs = dbmd.getIndexInfo(null, "QT\\_Ora9DS", "SYS.KeyElements", true, true); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs1 = dbmd.getIndexInfo(null, "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getIndexInfo("foo", "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetPrimaryKeysWithEscape() throws Exception {
        ResultSet rs = dbmd.getPrimaryKeys("QT\\_Ora9DS", "BQT1", "SmallA"); //$NON-NLS-1$ //$NON-NLS-2$  //$NON-NLS-3$
        ResultSet rs1 = dbmd.getPrimaryKeys(null, "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSet rs2 = dbmd.getPrimaryKeys("foo", "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetProceduresWithEscape() throws Exception {
        ResultSet rs = dbmd.getProcedures("QT\\_Ora9DS", null, null); //$NON-NLS-1$
        ResultSet rs1 = dbmd.getProcedures(null, "Foo%", null); //$NON-NLS-1$
        ResultSet rs2 = dbmd.getProcedures("foo", "Foo%", null); //$NON-NLS-1$ //$NON-NLS-2$
        compareResultSet(rs, rs1, rs2);
    }

    @Test
    public void testGetFunctions() throws Exception {
        ResultSet rs = dbmd.getFunctions(null, null, null); //$NON-NLS-1$
        ResultSet rs1 = dbmd.getFunctions(null, "pg%", "%pg%"); //$NON-NLS-1$
        compareResultSet(rs, rs1);
    }

    @Test
    public void testGetFunctionColumns() throws Exception {
        ResultSet rs = dbmd.getFunctionColumns(null, null, null, null); //$NON-NLS-1$
        ResultSet rs1 = dbmd.getFunctionColumns(null, "pg%", "%pg%", null); //$NON-NLS-1$
        compareResultSet(rs, rs1);
    }
    ///////////////////////////Helper Method//////////////////////////////

    private void helpTestSupportsConverts(int from, int to, boolean result) throws Exception {
        assertEquals("Expected doesn't match with actual for method - supportsConvert()", //$NON-NLS-1$
        result, dbmd.supportsConvert(from, to));
    }

    private void helpTestMethodsWithParams(String methodName, boolean returned, Object[] params) throws Exception {
        Class<?> dbmdClass = dbmd.getClass();
        Method[] methods = dbmdClass.getDeclaredMethods();

        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equalsIgnoreCase(methodName)) {
                Object actual = methods[i].invoke(dbmd, params);
                assertEquals("Expected doesn't match with actual for method - " + methodName, //$NON-NLS-1$
                new Boolean(returned), actual);
            }
        }
    }

    //////////////////////Expected Result//////////////////

    private Map<String, Object> getExpected() {
        Map<String, Object> expected = new HashMap<String, Object>();
        // return type -- boolean
        expected.put("allProceduresAreCallable", Boolean.TRUE); //$NON-NLS-1$
        expected.put("allTablesAreSelectable", Boolean.TRUE); //$NON-NLS-1$
        expected.put("doesMaxRowSizeIncludeBlobs", Boolean.FALSE); //$NON-NLS-1$
        expected.put("isCatalogAtStart", Boolean.FALSE); //$NON-NLS-1$
        expected.put("isReadOnly", Boolean.FALSE); //$NON-NLS-1$
        expected.put("locatorsUpdateCopy", Boolean.FALSE); //$NON-NLS-1$
        expected.put("nullPlusNonNullIsNull", Boolean.TRUE); //$NON-NLS-1$
        expected.put("nullsAreSortedAtEnd", Boolean.FALSE); //$NON-NLS-1$
        expected.put("nullsAreSortedAtStart", Boolean.FALSE); //$NON-NLS-1$
        expected.put("nullsAreSortedHigh", Boolean.FALSE); //$NON-NLS-1$
        expected.put("nullsAreSortedLow", Boolean.FALSE); //$NON-NLS-1$
        expected.put("storesLowerCaseIdentifiers", Boolean.FALSE); //$NON-NLS-1$
        expected.put("storesLowerCaseQuotedIdentifiers", Boolean.FALSE); //$NON-NLS-1$
        expected.put("storesMixedCaseIdentifiers", Boolean.TRUE); //$NON-NLS-1$
        expected.put("storesMixedCaseQuotedIdentifiers", Boolean.TRUE); //$NON-NLS-1$
        expected.put("storesUpperCaseIdentifiers", Boolean.FALSE); //$NON-NLS-1$
        expected.put("storesUpperCaseQuotedIdentifiers", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsAlterTableWithAddColumn", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsAlterTableWithDropColumn", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsANSI92EntryLevelSQL", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsANSI92FullSQL", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsANSI92IntermediateSQL", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsBatchUpdates", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsCatalogsInDataManipulation", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsCatalogsInIndexDefinitions", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsCatalogsInPrivilegeDefinitions", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsCatalogsInProcedureCalls", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsCatalogsInTableDefinitions", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsColumnAliasing", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsCorrelatedSubqueries", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsCoreSQLGrammar", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsDataDefinitionAndDataManipulationTransactions", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsDataManipulationTransactionsOnly", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsDifferentTableCorrelationNames", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsExpressionsInOrderBy", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsExtendedSQLGrammar", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsFullOuterJoins", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsGetGeneratedKeys", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsGroupBy", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsGroupByBeyondSelect", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsGroupByUnrelated", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsIntegrityEnhancementFacility", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsLikeEscapeClause", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsLimitedOuterJoins", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsMinimumSQLGrammar", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsMixedCaseIdentifiers", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsMixedCaseQuotedIdentifiers", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsOpenCursorsAcrossCommit", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsMultipleResultSets", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsMultipleOpenResults", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsMultipleTransactions", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsNamedParameters", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsNonNullableColumns", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsOpenCursorsAcrossRollback", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsOpenStatementsAcrossCommit", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsOpenStatementsAcrossRollback", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsOrderByUnrelated", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsOuterJoins", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsPositionedDelete", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsPositionedUpdate", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsSavepoints", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsSchemasInDataManipulation", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsSchemasInIndexDefinitions", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsSchemasInPrivilegeDefinitions", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsSchemasInProcedureCalls", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsSchemasInTableDefinitions", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsSelectForUpdate", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsStatementPooling", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsStoredProcedures", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsSubqueriesInComparisons", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsSubqueriesInExists", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsSubqueriesInIns", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsSubqueriesInQuantifieds", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsTableCorrelationNames", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsTransactions", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsUnion", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsUnionAll", Boolean.TRUE); //$NON-NLS-1$
        expected.put("usesLocalFilePerTable", Boolean.FALSE); //$NON-NLS-1$
        expected.put("usesLocalFiles", Boolean.FALSE); //$NON-NLS-1$
        expected.put("usesLocalFilePerTable", Boolean.FALSE); //$NON-NLS-1$

        // return type -- int
        expected.put("getDatabaseMinorVersion", new Integer(ApplicationInfo.getInstance().getMinorReleaseVersion())); //$NON-NLS-1$
        expected.put("getDatabaseMajorVersion", new Integer(ApplicationInfo.getInstance().getMajorReleaseVersion())); //$NON-NLS-1$
        expected.put("getJDBCMajorVersion", new Integer(3)); //$NON-NLS-1$
        expected.put("getJDBCMinorVersion", new Integer(0)); //$NON-NLS-1$
        expected.put("getDefaultTransactionIsolation", Connection.TRANSACTION_READ_COMMITTED); //$NON-NLS-1$
        expected.put("getDriverMajorVersion", new Integer(ApplicationInfo.getInstance().getMajorReleaseVersion())); //$NON-NLS-1$
        expected.put("getDriverMinorVersion", new Integer(ApplicationInfo.getInstance().getMinorReleaseVersion())); //$NON-NLS-1$
        expected.put("getMaxBinaryLiteralLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxCatalogNameLength", 255); //$NON-NLS-1$
        expected.put("getMaxCharLiteralLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxColumnNameLength", new Integer(255)); //$NON-NLS-1$
        expected.put("getMaxColumnsInGroupBy", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxColumnsInIndex", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxColumnsInOrderBy", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxColumnsInSelect", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxColumnsInTable", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxConnections", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxCursorNameLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxIndexLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxProcedureNameLength", new Integer(255)); //$NON-NLS-1$
        expected.put("getMaxRowSize", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxSchemaNameLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxStatementLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxStatements", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxTableNameLength", new Integer(255)); //$NON-NLS-1$
        expected.put("getMaxTablesInSelect", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxUserNameLength", new Integer(255)); //$NON-NLS-1$
        //TODO: change expected value;
        expected.put("getSQLStateType", new Integer(2)); //$NON-NLS-1$

        // return type -- String
        expected.put("getCatalogSeparator", "."); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getCatalogTerm", "VirtualDatabase"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDatabaseProductName", "Teiid Embedded"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDatabaseProductVersion", ApplicationInfo.getInstance().getMajorReleaseVersion()+"."+ApplicationInfo.getInstance().getMinorReleaseVersion()); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDriverName", "Teiid JDBC Driver"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDriverVersion", ApplicationInfo.getInstance().getMajorReleaseVersion()+"."+ApplicationInfo.getInstance().getMinorReleaseVersion()); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getExtraNameCharacters", ".@"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getIdentifierQuoteString", "\""); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getNumericFunctions", DatabaseMetaDataImpl.NUMERIC_FUNCTIONS); //$NON-NLS-1$
        expected.put("getProcedureTerm", "StoredProcedure"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getSchemaTerm", "Schema"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getSearchStringEscape", "\\"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getSQLKeywords", DatabaseMetaDataImpl.KEY_WORDS); //$NON-NLS-1$
        expected.put("getStringFunctions", DatabaseMetaDataImpl.STRING_FUNCTIONS); //$NON-NLS-1$
        expected.put("getSystemFunctions", DatabaseMetaDataImpl.SYSTEM_FUNCTIONS); //$NON-NLS-1$
        expected.put("getTimeDateFunctions", DatabaseMetaDataImpl.DATE_FUNCTIONS); //$NON-NLS-1$
        //expected.put("getUrl", primaryUrl + serverUrl); //$NON-NLS-1$
        expected.put("getUserName", CoreConstants.DEFAULT_ANON_USERNAME); //$NON-NLS-1$

        // return type - Object
        expected.put("getConnection", conn); //$NON-NLS-1$

        return expected;
    }

    @Test
    public void testDatabaseVersions() throws Exception {
        ConnectionImpl impl = Mockito.mock(ConnectionImpl.class);
        Mockito.stub(impl.getConnectionProps()).toReturn(new Properties());
        ServerConnection sconn = Mockito.mock(ServerConnection.class);
        Mockito.stub(sconn.getServerVersion()).toReturn("01.02.03-something");
        Mockito.stub(impl.getServerConnection()).toReturn(sconn);
        DatabaseMetaDataImpl metadata = new DatabaseMetaDataImpl(impl);
        assertEquals(1, metadata.getDatabaseMajorVersion());
        assertEquals(2, metadata.getDatabaseMinorVersion());
    }

}
