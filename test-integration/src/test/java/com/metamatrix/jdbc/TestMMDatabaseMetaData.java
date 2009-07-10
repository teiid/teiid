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

package com.metamatrix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.jdbc.TeiidDriver;

import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.util.ResultSetUtil;

/**
 */
public class TestMMDatabaseMetaData {

    private static final String DQP_CONFIG_FILE = UnitTestUtil.getTestDataPath() + "/bqt/bqt.properties;user=test"; //$NON-NLS-1$

    static Connection conn = null;
    static String primaryUrl = "jdbc:metamatrix:QT_Ora9DS@" + DQP_CONFIG_FILE + ";version=1"; //$NON-NLS-1$ //$NON-NLS-2$
    // URL for local DQP
    static String serverUrl = primaryUrl + ";logLevel=1;partialResultsMode=false"; //$NON-NLS-1$
    
    // URL for integration test
    MMDatabaseMetaData dbmd = null;
    private Map expectedMap = new HashMap();

    // constant 
    private static final int NO_LIMIT = 0;
    // constant value giving the key words used in metamatrix not in SQL-92
    private final static String KEY_WORDS = "OPTION, SHOWPLAN, DEBUG"; //$NON-NLS-1$

    //==============================================================================
    //  The following 2 constants are defined here to provide access to constants
    //  defined in the JDBC 3.0 implementation that comes with the 1.4 JRE.
    //  They are defined this way to allow this class to compile with the 1.3 JRE.
    //==============================================================================
    private static final int ResultSet_HOLD_CURSORS_OVER_COMMIT = 1; // ResultSet.HOLD_CURSORS_OVER_COMMIT == 1
    private static final int ResultSet_CLOSE_CURSORS_AT_COMMIT = 2; // ResultSet.CLOSE_CURSORS_AT_COMMIT  == 2

    @Before
    public void setUp() throws Exception {
        dbmd = new MMDatabaseMetaData((MMConnection) conn);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
    	if (conn != null) {
            conn.close();
        }
    }    
    
    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        Class.forName(TeiidDriver.class.getName());
        conn = DriverManager.getConnection(serverUrl);
    }
    
    /** Test all the non-query methods */
    @Test
    public void testMethodsWithoutParams() throws Exception {
        Class dbmdClass = dbmd.getClass();
        // non-query Methods return String, boolean or int
        Method[] methods = dbmdClass.getDeclaredMethods();

        expectedMap = getExpected();
        //System.out.println(" -- total method == " + methods.length + ", non-query == " + expectedMap.size());
        for (int i = 0; i < methods.length; i++) {
            if (expectedMap.containsKey(methods[i].getName())) {

                Object actualValue = null;
                Object expectedValue = null;
                Object expectedReturn = expectedMap.get(methods[i].getName());
                Object[] params = null;
                
                if (methods[i].getName().equalsIgnoreCase(DQP_CONFIG_FILE))

                if (expectedReturn instanceof List) {
                    // has input parameters
                    List returned = (List) expectedReturn;
                    params = (Object[]) returned.get(1);
                    //System.out.println(" params == " + params[0]);
                    expectedValue = returned.get(0);
                    actualValue = methods[i].invoke(dbmd, params);
                } else {
                    // without params
                    expectedValue = expectedReturn;
                    actualValue = methods[i].invoke(dbmd, new Object[0]);
                }

                assertEquals(" Expected doesn't match with actual for method - " + //$NON-NLS-1$
                methods[i].getName(), expectedValue, actualValue);
                //System.out.println("method [" + methods[i].getName() + " ] 's expectedValue == " + expectedValue + " actual == " + actualValue);
            }
        }
    }

    /** Test all the methods that throw exception */ 
    @Test
    public void testMethodsWithExceptions() throws Exception {
        Class dbmdClass = dbmd.getClass();
        Method[] methods = dbmdClass.getDeclaredMethods();
        
        expectedMap = new HashMap(); //none expected
        //System.out.println(" -- total method == " + methods.length + ", non-query == " + expectedMap.size());
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

    public static final short ALWAYS_NULL = 0;
    public static final short NEVER_NULL = 1;
    public static final short MAY_BE_NULL = 2;
    
    private static final boolean REPLACE_EXPECTED = false;
    private static final boolean WRITE_ACTUAL_RESULTS_TO_FILE = false;
    private static final boolean PRINT_RESULTSETS_TO_CONSOLE = false;
    
    private static final int MAX_COL_WIDTH = 65;

    private FileOutputStream actualOut = null;
    private BufferedReader expectedIn = null;
    private PrintStream stream = null;
    private void initResultSetStreams(String testName) throws FileNotFoundException {
        if (REPLACE_EXPECTED) {
            File actual = new File(UnitTestUtil.getTestDataPath() + "/TestMMDatabaseMetaData/"+testName+".expected"); //$NON-NLS-1$ //$NON-NLS-2$
            actualOut = new FileOutputStream(actual);
        } else {
            if (WRITE_ACTUAL_RESULTS_TO_FILE) {
                File actual = new File(UnitTestUtil.getTestDataPath() + "/TestMMDatabaseMetaData/"+testName+".actual"); //$NON-NLS-1$ //$NON-NLS-2$
                actualOut = new FileOutputStream(actual);
            }
            File expected = new File(UnitTestUtil.getTestDataPath() + "/TestMMDatabaseMetaData/"+testName+".expected"); //$NON-NLS-1$ //$NON-NLS-2$
            expectedIn = new BufferedReader(new FileReader(expected));
        }
        PrintStream defaultStream = null;
        if (PRINT_RESULTSETS_TO_CONSOLE) {
            defaultStream = new PrintStream(System.out) {
                // System.out should be protected from being closed.
                public void close() {}
            };
        }
        stream = ResultSetUtil.getPrintStream(actualOut, expectedIn, defaultStream);

    }
    
    private void closeResultSetTestStreams() throws IOException {
        stream.close();
        if (actualOut != null) {
            actualOut.close();
        }
        if (expectedIn != null) {
            expectedIn.close();
        }
    }
    
    public void checkColumn(String expectedName, short nullable, Class type, String metadataName, Object value) {
        if(nullable == ALWAYS_NULL) {
            assertNull("Column " + expectedName + " should always be null but was: " + value, value);             //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if(nullable == NEVER_NULL) {
            assertNotNull("Column " + expectedName + " should never be null but was", value);             //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        if(value != null) {
            assertTrue("Column " + expectedName + " is of wrong type", value.getClass().isAssignableFrom(type)); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        assertEquals("Got incorrect column name", expectedName, metadataName); //$NON-NLS-1$
    }
    
    public void checkNullConstant(Object value) {        
        assertNotNull("Got null nullability constant", value); //$NON-NLS-1$
        assertTrue("Nullability constant is wrong type: " + value.getClass(), value.getClass().equals(Integer.class)); //$NON-NLS-1$
        Integer intValue = (Integer) value;
        assertTrue("Got invalid nullability constant: " + value, //$NON-NLS-1$
            intValue.intValue() == DatabaseMetaData.columnNoNulls || 
            intValue.intValue() == DatabaseMetaData.columnNullable || 
            intValue.intValue() == DatabaseMetaData.columnNullableUnknown  
        );
    }

    @Test
    public void testUcaseMatchReturnsNoRows() throws Exception {
        initResultSetStreams("testGetColumnsSingleMatchQuery"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            java.sql.Statement stmt = conn.createStatement();
            
            // Returns 24 rows:
            //rs = stmt.executeQuery("SELECT Name FROM System.Groups WHERE ModelName = 'System' OPTION DEBUG");
            
            // Returns 0 rows (but should be identical and return 24 rows):
            rs = stmt.executeQuery("SELECT Name FROM System.Groups WHERE UCASE(ModelName) = 'SYSTEM'"); //$NON-NLS-1$

            int count = 0;
            while(rs.next()) {
                count++;
                //System.out.println("table="+rs.getString(1));
            }
            assertEquals(25, count);
            
            //System.out.println(((com.metamatrix.jdbc.api.Statement)stmt).getDebugLog());

        } finally {
            if(rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }

    @Test
    public void testGetColumnsSingleMatch() throws Exception {
        initResultSetStreams("testGetColumnsSingleMatch"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            //List expected = getExpectedColumns();
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getColumns(null, null, "System.VirtualDatabases", "Name"); //$NON-NLS-1$ //$NON-NLS-2$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                         Collections.EMPTY_LIST,
                         ResultSetUtil.getUnequalLines(stream));
        } finally {
            if(rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }

    @Test
    public void testGetCatalogs() throws Exception {
        initResultSetStreams("testGetCatalogs"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getCatalogs();
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                         Collections.EMPTY_LIST,
                         ResultSetUtil.getUnequalLines(stream));
        } finally {
            if (rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }

    @Test
    public void testGetCrossReference() throws Exception {
        initResultSetStreams("testGetCrossReference"); //$NON-NLS-1$
        ResultSet rs = null;
        try { 
            DatabaseMetaData dbmd = conn.getMetaData();
            stream.println("getCrossReference1"); //$NON-NLS-1$
            rs = dbmd.getCrossReference(null, null, "BQT1.SmallA", null, null, "BQT1.SmallB");//$NON-NLS-1$ //$NON-NLS-2$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        // Check for empty
        stream.println("getCrossReference2"); //$NON-NLS-1$
        rs = dbmd.getCrossReference(null, "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        // Check for empty
        stream.println("getCrossReference3"); //$NON-NLS-1$
        rs = dbmd.getCrossReference("foo", "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();          
        closeResultSetTestStreams();
    }
        
    @Test
    public void testGetImportedKeys() throws Exception {
        initResultSetStreams("testGetImportedKeys"); //$NON-NLS-1$
        ResultSet rs = null;
        try { 
            DatabaseMetaData dbmd = conn.getMetaData();
            stream.println("getImportedKeys1"); //$NON-NLS-1$
            rs = dbmd.getImportedKeys(null, null, "BQT1.SmallA"); //$NON-NLS-1$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        stream.println("getImportedKeys2"); //$NON-NLS-1$
        rs = dbmd.getImportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close(); 

        stream.println("getImportedKeys3"); //$NON-NLS-1$
        rs = dbmd.getImportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();        
        closeResultSetTestStreams();
    }

    @Test
    public void testGetExportedKeys() throws Exception {
        initResultSetStreams("testGetExportedKeys"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getExportedKeys(null, null, "BQT1.SmallA"); //$NON-NLS-1$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getExportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();
        
        rs = dbmd.getExportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
       
    @Test
    public void testGetIndexInfo() throws Exception {
        initResultSetStreams("testGetIndexInfo"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            //ResultSet rs = dbmd.getIndexInfo(null, null, "BQT1.SmallA", true, true); //$NON-NLS-1$
            rs = dbmd.getIndexInfo(null, null, "System.KeyElements", true, true); //$NON-NLS-1$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getIndexInfo(null, "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();  
        
        rs = dbmd.getIndexInfo("foo", "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();   
        closeResultSetTestStreams();
    }
        
    @Test
    public void testGetPrimaryKeys() throws Exception {
        initResultSetStreams("testGetPrimaryKeys"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            //ResultSet rs = dbmd.getPrimaryKeys(null, null, "SYSTEM.VIRTUALDATABASES"); //$NON-NLS-1$
            
            // This is only a way to do unit test. Actually, the primary key of BQT.smallA
            // should be tested here, the only tables should be queried are just system
            // tables. 
            rs = dbmd.getPrimaryKeys(null, null, "BQT1.SmallA"); //$NON-NLS-1$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        // Check for empty
        rs = dbmd.getPrimaryKeys(null, "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();  
        
        rs = dbmd.getPrimaryKeys("foo", "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetProcedureColumns() throws Exception {
        initResultSetStreams("testGetProcedureColumns"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getProcedureColumns(null, null, null, null);
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getProcedureColumns(null, "Foo%", null, null); //$NON-NLS-1$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        rs = dbmd.getProcedureColumns("foo", "Foo%", null, null); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetProcedures() throws Exception {
        initResultSetStreams("testGetProcedures"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getProcedures(null, null, null);
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getProcedures(null, "Foo%", null); //$NON-NLS-1$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        rs = dbmd.getProcedures("foo", "Foo%", null); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetSchemas() throws Exception {
        initResultSetStreams("testGetSchemas"); //$NON-NLS-1$
        ResultSet rs = null;    
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getSchemas();

            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                       Collections.EMPTY_LIST,
                       ResultSetUtil.getUnequalLines(stream));
        } finally {
            if(rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }
    @Test
    public void testGetColumns() throws Exception {
        initResultSetStreams("testGetColumns"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        stream.println("getColumns1"); //$NON-NLS-1$
        ResultSet rs = dbmd.getColumns(null, null, null, null);
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();    
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetColumns2() throws Exception {
        initResultSetStreams("testGetColumns2"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        
      stream.println("getColumns2"); //$NON-NLS-1$
      ResultSet rs = dbmd.getColumns(null, "QT_Ora%", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);

        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();    
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetColumns3() throws Exception {
        initResultSetStreams("testGetColumns3"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        
      stream.println("getColumns3"); //$NON-NLS-1$
      ResultSet rs = dbmd.getColumns(null, "Foo%", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);

        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();    
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetColumns4() throws Exception {
        initResultSetStreams("testGetColumns4"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        
        stream.println("getColumns4"); //$NON-NLS-1$
         ResultSet rs = dbmd.getColumns("foo", "Foo%", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);

        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();    
        closeResultSetTestStreams();
    }    
    

    @Test
    public void testGetColumnPrivileges() throws Exception {
        initResultSetStreams("testGetColumnPrivileges"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getColumnPrivileges(null, "Parts", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        
        rs = dbmd.getColumnPrivileges(null, "%foo", null, null); //$NON-NLS-1$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        rs = dbmd.getColumnPrivileges("foo", "%foo", null, null); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetColumnPrivilegesResultSetMetaData() throws Exception {
        initResultSetStreams("testGetColumnPrivilegesResultSetMetaData"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getColumnPrivileges(null, "Parts", "%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                   Collections.EMPTY_LIST,
                   ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetTablePrivileges() throws Exception {
        initResultSetStreams("testGetTablePrivileges"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getTablePrivileges(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                   Collections.EMPTY_LIST,
                   ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    

    @Test
    public void testGetTablePrivilegesResultSetMetaData() throws Exception {
        initResultSetStreams("testGetTablePrivilegesResultSetMetaData"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getTablePrivileges(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                   Collections.EMPTY_LIST,
                   ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }

    @Test
    public void testGetTables_specificTable() throws Exception {
        initResultSetStreams("testGetTables_specificTable"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            //List expected = getExpectedColumns();
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getTables(null, null, "SYSTEM.VIRTUALDATABASES", null); //$NON-NLS-1$ 
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                       Collections.EMPTY_LIST,
                       ResultSetUtil.getUnequalLines(stream));
        } finally {
            if (rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }

    @Test
    public void testGetTables_specificTableTypes() throws Exception {
        initResultSetStreams("testGetTables_specificTableTypes"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            
            String[] tables = new String[] { "Table" }; //$NON-NLS-1$
            
            rs = dbmd.getTables(null, null, null, tables); 
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                       Collections.EMPTY_LIST,
                       ResultSetUtil.getUnequalLines(stream));
        } finally {
            if (rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }

    @Test
    public void testGetTables_specificTableMultipleTypes() throws Exception {
        initResultSetStreams("testGetTables_specificTableMultipleTypes"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            
            String[] tables = new String[] { "Table", "View" }; //$NON-NLS-1$ //$NON-NLS-2$
            
            rs = dbmd.getTables(null, null, null, tables); 
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                       Collections.EMPTY_LIST,
                       ResultSetUtil.getUnequalLines(stream));
        } finally {
            if (rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }
    
    @Test
    public void testGetTables() throws Exception{
        initResultSetStreams("testGetTables"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getTables(null, null, null, null); 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();
        
        rs = dbmd.getTables(null, "%foo", null, null); //$NON-NLS-1$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        rs = dbmd.getTables("foo", "%foo", null, null); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetTables_allTables() throws Exception {
        initResultSetStreams("testGetTables_allTables"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getTables(null, null, null, null); 
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                         Collections.EMPTY_LIST,
                         ResultSetUtil.getUnequalLines(stream));
        } finally {
            if (rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }

    @Test
    public void testGetTableTypes() throws Exception {
        initResultSetStreams("testGetTableTypes"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getTableTypes();
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                         Collections.EMPTY_LIST,
                         ResultSetUtil.getUnequalLines(stream));
        } finally {
            if (rs != null) {
                rs.close();
            }
            closeResultSetTestStreams();
        }
    }

    @Test
    public void testGetTypeInfo_TotalNumber() throws Exception {
        initResultSetStreams("testGetTypeInfo_TotalNumber"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getTypeInfo();
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                         Collections.EMPTY_LIST,
                         ResultSetUtil.getUnequalLines(stream));
        } finally { 
            if(rs != null) {
                rs.close();    
            }
            closeResultSetTestStreams();
        }
    }
    
    @Test
    public void testGetUDTs() throws Exception{
        initResultSetStreams("testGetUDTs"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getUDTs(null, null, "%blob%", null); //$NON-NLS-1$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();
        
        rs = dbmd.getUDTs(null, "%foo", "%blob%", null); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();
        
        rs = dbmd.getUDTs("foo", "%foo", "%blob%", null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetUDTs_specificTypeName() throws Exception {
        initResultSetStreams("testGetUDTs_specificTypeName"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getUDTs(null, null, "%blob%", null); //$NON-NLS-1$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
            assertEquals("Actual data did not match expected", //$NON-NLS-1$
                         Collections.EMPTY_LIST,
                         ResultSetUtil.getUnequalLines(stream));
        } finally { 
            if(rs != null) {
                rs.close();    
            }
            closeResultSetTestStreams();
        }
    }
        
    @Test
    public void testGetVersionColumns() throws Exception {
        initResultSetStreams("testGetVersionColumns"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getVersionColumns(null, null, null);
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs = dbmd.getVersionColumns(null, "Foo%", null); //$NON-NLS-1$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        rs = dbmd.getVersionColumns("foo", "Foo%", null); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }

    @Test
    public void testGetBestRowIdentifier() throws Exception {
        initResultSetStreams("testGetBestRowIdentifier"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getBestRowIdentifier(null, null, "SYSTEM.VIRTUALDATABASES", //$NON-NLS-1$
                DatabaseMetaData.bestRowTemporary, true);
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getBestRowIdentifier(null, "%foo", null, 1,true); //$NON-NLS-1$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        rs = dbmd.getBestRowIdentifier("foo", "%foo", null, 1,true); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetSuperTables() throws Exception {
        initResultSetStreams("testSuperTables"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getSuperTables(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                   Collections.EMPTY_LIST,
                   ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetSuperTypes() throws Exception {
        initResultSetStreams("testGetSuperTypes"); //$NON-NLS-1$
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getSuperTypes(null, "Parts", "%"); //$NON-NLS-1$ //$NON-NLS-2$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                   Collections.EMPTY_LIST,
                   ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetColumnsWithEscape() throws Exception {
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet columns = dbmd.getColumns(null, "QT\\_Ora9DS", "BQT1.SmallA", "IntKey"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        columns.next();
        assertEquals("QT_Ora9DS", columns.getString(2));//$NON-NLS-1$  
        assertFalse(columns.next());
    }
        
    @Test
    public void testGetCrossReferenceWithEscape() throws Exception {
        initResultSetStreams("testGetCrossReference"); //$NON-NLS-1$
        ResultSet rs = null;
        try { 
            DatabaseMetaData dbmd = conn.getMetaData();
            stream.println("getCrossReference1"); //$NON-NLS-1$
            rs = dbmd.getCrossReference(null, "QT\\_Ora9DS", "BQT1.SmallA", null, null, "BQT1.SmallB");//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        // Check for empty
        stream.println("getCrossReference2"); //$NON-NLS-1$
        rs = dbmd.getCrossReference(null, "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        // Check for empty
        stream.println("getCrossReference3"); //$NON-NLS-1$
        rs = dbmd.getCrossReference("foo", "Foo%", "%", null, null, "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();          
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetExportedKeysWithEscape() throws Exception {
        initResultSetStreams("testGetExportedKeys"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getExportedKeys(null, "QT\\_Ora9DS", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getExportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();
        
        rs = dbmd.getExportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetImportedKeysWithEscape() throws Exception {
        initResultSetStreams("testGetImportedKeys"); //$NON-NLS-1$
        ResultSet rs = null;
        try { 
            DatabaseMetaData dbmd = conn.getMetaData();
            stream.println("getImportedKeys1"); //$NON-NLS-1$
            rs = dbmd.getImportedKeys(null, "QT\\_Ora9DS", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        stream.println("getImportedKeys2"); //$NON-NLS-1$
        rs = dbmd.getImportedKeys(null, "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close(); 

        stream.println("getImportedKeys3"); //$NON-NLS-1$
        rs = dbmd.getImportedKeys("foo", "Foo%", "%"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();        
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetIndexInfoWithEscape() throws Exception {
        initResultSetStreams("testGetIndexInfo"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            //ResultSet rs = dbmd.getIndexInfo(null, null, "BQT1.SmallA", true, true); //$NON-NLS-1$
            rs = dbmd.getIndexInfo(null, "QT\\_Ora9DS", "System.KeyElements", true, true); //$NON-NLS-1$ //$NON-NLS-2$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getIndexInfo(null, "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();  
        
        rs = dbmd.getIndexInfo("foo", "Foo%", "%", true, false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();   
        closeResultSetTestStreams();
    }
        
    @Test
    public void testGetPrimaryKeysWithEscape() throws Exception {
        initResultSetStreams("testGetPrimaryKeys"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            //ResultSet rs = dbmd.getPrimaryKeys(null, null, "SYSTEM.VIRTUALDATABASES"); //$NON-NLS-1$
            
            // This is only a way to do unit test. Actually, the primary key of BQT.smallA
            // should be tested here, the only tables should be queried are just system
            // tables. 
            rs = dbmd.getPrimaryKeys(null, "QT\\_Ora9DS", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        // Check for empty
        rs = dbmd.getPrimaryKeys(null, "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();  
        
        rs = dbmd.getPrimaryKeys("foo", "Foo%", "BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    
    @Test
    public void testGetProceduresWithEscape() throws Exception {
        initResultSetStreams("testGetProcedures"); //$NON-NLS-1$
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getProcedures(null, "QT\\_Ora9DS", null); //$NON-NLS-1$
            ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        } finally { 
            if(rs != null) {
                rs.close();    
            }
        }
        
        rs = dbmd.getProcedures(null, "Foo%", null); //$NON-NLS-1$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        rs.close();   
        
        rs = dbmd.getProcedures("foo", "Foo%", null); //$NON-NLS-1$ //$NON-NLS-2$ 
        ResultSetUtil.printResultSet(rs, MAX_COL_WIDTH, true, stream);
        assertEquals("Actual data did not match expected", //$NON-NLS-1$
                     Collections.EMPTY_LIST,
                     ResultSetUtil.getUnequalLines(stream));
        rs.close();
        closeResultSetTestStreams();
    }
    ///////////////////////////Helper Method//////////////////////////////

    private void helpTestSupportsConverts(int from, int to, boolean result) throws Exception {
        assertEquals("Expected doesn't match with actual for method - supportsConvert()", //$NON-NLS-1$
        result, dbmd.supportsConvert(from, to));
    }

    private void helpTestMethodsWithParams(String methodName, boolean returned, Object[] params) throws Exception {
        Class dbmdClass = dbmd.getClass();
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

    private Map getExpected() {
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
        expected.put("nullsAreSortedLow", Boolean.TRUE); //$NON-NLS-1$
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
        expected.put("supportsExpressionsInOrderBy", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsExtendedSQLGrammar", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsFullOuterJoins", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsGetGeneratedKeys", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsGroupBy", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsGroupByBeyondSelect", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsGroupByUnrelated", Boolean.FALSE); //$NON-NLS-1$
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
        expected.put("supportsNamedParameters", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsNonNullableColumns", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsOpenCursorsAcrossRollback", Boolean.FALSE); //$NON-NLS-1$
        expected.put("supportsOpenStatementsAcrossCommit", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsOpenStatementsAcrossRollback", Boolean.TRUE); //$NON-NLS-1$
        expected.put("supportsOrderByUnrelated", Boolean.FALSE); //$NON-NLS-1$
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
        expected.put("getDefaultTransactionIsolation", new Integer(Connection.TRANSACTION_NONE)); //$NON-NLS-1$
        expected.put("getDriverMajorVersion", new Integer(ApplicationInfo.getInstance().getMajorReleaseVersion())); //$NON-NLS-1$
        expected.put("getDriverMinorVersion", new Integer(ApplicationInfo.getInstance().getMinorReleaseVersion())); //$NON-NLS-1$
        expected.put("getMaxBinaryLiteralLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
        expected.put("getMaxCatalogNameLength", new Integer(NO_LIMIT)); //$NON-NLS-1$
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
        expected.put("getCatalogSeparator", ""); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getCatalogTerm", ""); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDatabaseProductName", "MetaMatrix Query"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDatabaseProductVersion", "5.5"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDriverName", "MetaMatrix Query JDBC Driver"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getDriverVersion", ApplicationInfo.getInstance().getMajorReleaseVersion()+"."+ApplicationInfo.getInstance().getMinorReleaseVersion()); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getExtraNameCharacters", ".@"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getIdentifierQuoteString", "\""); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getNumericFunctions", MMDatabaseMetaData.NUMERIC_FUNCTIONS); //$NON-NLS-1$
        expected.put("getProcedureTerm", "StoredProcedure"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getSchemaTerm", "VirtualDatabase"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getSearchStringEscape", "\\"); //$NON-NLS-1$ //$NON-NLS-2$
        expected.put("getSQLKeywords", KEY_WORDS); //$NON-NLS-1$
        expected.put("getStringFunctions", MMDatabaseMetaData.STRING_FUNCTIONS); //$NON-NLS-1$
        expected.put("getSystemFunctions", MMDatabaseMetaData.SYSTEM_FUNCTIONS); //$NON-NLS-1$
        expected.put("getTimeDateFunctions", MMDatabaseMetaData.DATE_FUNCTIONS); //$NON-NLS-1$
        //expected.put("getUrl", primaryUrl + serverUrl); //$NON-NLS-1$
        expected.put("getUserName", null); //$NON-NLS-1$

        // return type - Object
        expected.put("getConnection", conn); //$NON-NLS-1$

        return expected;
    }
    
}
