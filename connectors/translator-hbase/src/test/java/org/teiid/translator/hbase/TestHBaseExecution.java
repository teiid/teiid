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
package org.teiid.translator.hbase;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import javax.naming.Context;
import javax.resource.ResourceException;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
//import org.junit.Ignore;
import org.junit.Test;
import org.teiid.core.util.SimpleMock;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.TypeFacility;

/**
 * How to run TestHBaseExecution?
 * 
 *  1. HBase setup 
 *     
 *     Use the steps 1.2.1 and 1.2.2 in 'http://hbase.apache.org/book/quickstart.html' install Standalone HBase
 *
 *  2. Phoenix setup
 *     
 *     Use the Installation steps in 'http://phoenix.apache.org/download.html' to copy phoenix-core.jar to HBase lib directory to finish Phoenix installation.
 *     
 *     Note that: the phoenix-client.jar as phoenix driver used to setup data source in JBoss Container or run run unit test in j2se environment
 *     
 *  3. Add phoenix-client.jar to classpath 
 *  
 *  4. Start HBase server, comment out @Ignore annotation, execute Junit Test
 *    
 *     Note that: If HBase not run on 127.0.0.1, please change JDBC_URL to point to a correct zookeeper quorum
 *                If Fully Distributed HBase(multiple RegionServer) be run, please change JDBC_URL to point to a correct zookeeper quorum, the sample like 'jdbc:phoenix [ :<zookeeper quorum> [ :<port number> ] [ :<root node> ] ]'
 */
@Ignore
@SuppressWarnings("nls")
public class TestHBaseExecution {
    
    static final String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    static final String JDBC_URL = "jdbc:phoenix:127.0.0.1:2181";
    static final String JDBC_USER = "";
    static final String JDBC_PASS = "";
    
    static Connection conn = null;
    
    //This for demonstrating Phoenix's upper casting mapping, assume city, name, amount and product are Column Qualifiers in HBase, Customer are table name in HBase, all are lower words
    static final String CUSTOMER = "CREATE TABLE IF NOT EXISTS \"Customer\"(\"ROW_ID\" VARCHAR PRIMARY KEY, \"customer\".\"city\" VARCHAR, \"customer\".\"name\" VARCHAR, \"sales\".\"amount\" VARCHAR, \"sales\".\"product\" VARCHAR)";
    static final String TYPES_TEST = "CREATE TABLE IF NOT EXISTS TypesTest (ROW_ID VARCHAR PRIMARY KEY, f.q1 VARCHAR, f.q2 VARBINARY, f.q3 VARCHAR, f.q4 BOOLEAN, f.q5 TINYINT, f.q6 TINYINT, f.q7 SMALLINT, f.q8 SMALLINT, f.q9 INTEGER, f.q10 INTEGER, f.q11 BIGINT, f.q12 BIGINT, f.q13 FLOAT, f.q14 FLOAT, f.q15 DOUBLE, f.q16 DECIMAL, f.q17 DECIMAL, f.q18 DATE, f.q19 TIME, f.q20 TIMESTAMP)";
    static final String TIMES_TEST = "CREATE TABLE IF NOT EXISTS TimesTest(ROW_ID VARCHAR PRIMARY KEY, f.column1 DATE, f.column2 TIME, f.column3 TIMESTAMP)";
    
    @BeforeClass
    public static void init() throws Exception {
        
        EmbeddedServer server = new EmbeddedServer();
        
        HBaseExecutionFactory executionFactory = new HBaseExecutionFactory();
        executionFactory.start();
        server.addTranslator("translator-hbase", executionFactory);
        
        DataSource ds = TestHBaseUtil.setupDataSource("java:/hbaseDS", JDBC_DRIVER, JDBC_URL, JDBC_USER, JDBC_PASS);
        Connection c = ds.getConnection();
        TestHBaseUtil.executeUpdate(c, CUSTOMER);
    	TestHBaseUtil.executeUpdate(c, TYPES_TEST);
    	TestHBaseUtil.executeUpdate(c, TIMES_TEST);
    	
    	TestHBaseUtil.insertTestData(c);
        
    	TestHBaseUtil.close(c);
    	
    	System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "bitronix.tm.jndi.BitronixInitialContextFactory");
        
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setTransactionManager(SimpleMock.createSimpleMock(TransactionManager.class));
        server.start(config);
        server.deployVDB(new FileInputStream(new File("src/test/resources/hbase-vdb.xml")));
        conn = server.getDriver().connect("jdbc:teiid:hbasevdb", null);
    }
    
    @Test
    public void testInsert() throws Exception {
        TestHBaseUtil.executeUpdate(conn, "INSERT INTO Customer VALUES('108', 'Beijing', 'Kylin Soong', '$8000.00', 'Crystal Orange')");
        TestHBaseUtil.executeUpdate(conn, "INSERT INTO Customer(PK, city, name) VALUES ('109', 'Beijing', 'Kylin Soong')");
    }
    
    @Test
    public void testBatchedInsert() throws SQLException {
        TestHBaseUtil.executeBatchedUpdate(conn, "INSERT INTO Customer VALUES (?, ?, ?, ?, ?)", 2);
        TestHBaseUtil.executeBatchedUpdate(conn, "INSERT INTO Customer(PK, city, name, amount, product) VALUES (?, ?, ?, ?, ?)", 2);
        TestHBaseUtil.executeBatchedUpdate(conn, "INSERT INTO Customer VALUES (?, ?, ?, ?, ?)", 1);
    }
    
    @Test
    public void testConditionAndOr() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK='105' OR name='John White'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK='105' AND name='John White'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK='105' AND (name='John White' OR name='Kylin Soong')");
    }
    
    /**
     * =     Equal
     * >     Greater than
     * <     Less than
     * >=     Greater than or equal
     * <=     Less than or equal
     * BETWEEN     Between an inclusive range
     * LIKE     Search for a pattern
     * IN     To specify multiple possible values for a column
     */
    @Test
    public void testConditionComparison() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK = '108'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK > '108'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK < '108'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK >= '108'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK <= '108'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK BETWEEN '105' AND '108'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK LIKE '10%'");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer WHERE PK IN ('105', '106')");
    }
    
    @Test
    public void testSelect() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer");
        TestHBaseUtil.executeQuery(conn, "SELECT city, amount FROM Customer");
        TestHBaseUtil.executeQuery(conn, "SELECT DISTINCT city FROM Customer");
        TestHBaseUtil.executeQuery(conn, "SELECT city, amount FROM Customer WHERE PK='105'");
    }
    
    @Test
    public void testSelectOrderBy() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer ORDER BY PK");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer ORDER BY PK ASC");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer ORDER BY PK DESC");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer ORDER BY name, city DESC");
    }
    
    @Test
    public void testSelectGroupBy() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT COUNT(PK) FROM Customer WHERE name='John White'");
        TestHBaseUtil.executeQuery(conn, "SELECT name, COUNT(PK) FROM Customer GROUP BY name");
        TestHBaseUtil.executeQuery(conn, "SELECT name, COUNT(PK) FROM Customer GROUP BY name HAVING COUNT(PK) > 1");
        TestHBaseUtil.executeQuery(conn, "SELECT name, city, COUNT(PK) FROM Customer GROUP BY name, city");
        TestHBaseUtil.executeQuery(conn, "SELECT name, city, COUNT(PK) FROM Customer GROUP BY name, city HAVING COUNT(PK) > 1");
    }
    
    @Test
    public void testSelectLimit() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer LIMIT 3");
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM Customer ORDER BY PK DESC LIMIT 3");
    }
    
    @Test
    public void testTimesTypes() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM TimesTest");
        
        Date date = new Date(new java.util.Date().getTime());
        Time time = new Time(new java.util.Date().getTime());
        Timestamp timestramp = new Timestamp(new java.util.Date().getTime());
        timestramp.setNanos(1000);
        
        PreparedStatement pstmt = null ;
        try {
            pstmt = conn.prepareStatement("INSERT INTO TimesTest VALUES (?, ?, ?, ?)");
            for(int i = 0 ; i < 2 ; i ++) {
                pstmt.setString(1, 100 + i + "");
                pstmt.setDate(2, date);
                pstmt.setTime(3, time);
                pstmt.setTimestamp(4, timestramp);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            if(!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            TestHBaseUtil.close(pstmt);
        }
    }
    
    /*
     * Teiid: https://docs.jboss.org/author/display/TEIID/Supported+Types
     * 
     * Phoenix: http://phoenix.apache.org/language/datatypes.html
     */
    @Test
    public void testDataTypes() throws Exception{
        TestHBaseUtil.executeQuery(conn, "SELECT * FROM TypesTest");
        TestHBaseUtil.executeBatchedUpdateDataType(conn, "INSERT INTO TypesTest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 1);
        TestHBaseUtil.executeBatchedUpdateDataType(conn, "INSERT INTO TypesTest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 10);
        
        Statement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT * FROM TypesTest WHERE PK = '10001'");
            if(rs.next()) {
                assertEquals(rs.getObject(1).getClass(), TypeFacility.RUNTIME_TYPES.STRING);
                assertEquals(rs.getObject(2).getClass(), TypeFacility.RUNTIME_TYPES.STRING);
                assertEquals(rs.getObject(3).getClass(), byte[].class);
                assertEquals(rs.getObject(4).getClass(), TypeFacility.RUNTIME_TYPES.CHAR);
                assertEquals(rs.getObject(5).getClass(), TypeFacility.RUNTIME_TYPES.BOOLEAN);
                assertEquals(rs.getObject(6).getClass(), TypeFacility.RUNTIME_TYPES.BYTE);
                assertEquals(rs.getObject(7).getClass(), TypeFacility.RUNTIME_TYPES.BYTE);
                assertEquals(rs.getObject(8).getClass(), TypeFacility.RUNTIME_TYPES.SHORT);
                assertEquals(rs.getObject(9).getClass(), TypeFacility.RUNTIME_TYPES.SHORT);
                assertEquals(rs.getObject(10).getClass(), TypeFacility.RUNTIME_TYPES.INTEGER);
                assertEquals(rs.getObject(11).getClass(), TypeFacility.RUNTIME_TYPES.INTEGER);
                assertEquals(rs.getObject(12).getClass(), TypeFacility.RUNTIME_TYPES.LONG);
                assertEquals(rs.getObject(13).getClass(), TypeFacility.RUNTIME_TYPES.LONG);
                assertEquals(rs.getObject(14).getClass(), TypeFacility.RUNTIME_TYPES.FLOAT);
                assertEquals(rs.getObject(15).getClass(), TypeFacility.RUNTIME_TYPES.FLOAT);
                assertEquals(rs.getObject(16).getClass(), TypeFacility.RUNTIME_TYPES.DOUBLE);
                assertEquals(rs.getObject(17).getClass(), TypeFacility.RUNTIME_TYPES.BIG_DECIMAL);
                assertEquals(rs.getObject(18).getClass(), TypeFacility.RUNTIME_TYPES.BIG_DECIMAL);
                assertEquals(rs.getObject(19).getClass(), TypeFacility.RUNTIME_TYPES.DATE);
                assertEquals(rs.getObject(20).getClass(), TypeFacility.RUNTIME_TYPES.TIME);
                assertEquals(rs.getObject(21).getClass(), TypeFacility.RUNTIME_TYPES.TIMESTAMP);
            }            
        } catch (Exception e) {
            throw e ;
        } finally {
            TestHBaseUtil.close(rs, stmt);
        }
    }
    
    @Test
    public void testFunctions() throws Exception {
        TestHBaseUtil.executeQuery(conn, "SELECT COUNT(PK) AS totalCount FROM Customer WHERE name = 'Kylin Soong'");
    }
    
    @Test
    public void testConnection() throws ResourceException, SQLException {
        assertNotNull(conn);
        DatabaseMetaData dbmd = conn.getMetaData();
        assertEquals(true, dbmd.supportsGetGeneratedKeys());
    }
    
    @AfterClass 
    public static void tearDown() throws SQLException{
        if(null != conn){
            conn.close();
        }
    }
    
}
