package org.teiid.translator.jdbc.ucanaccess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.naming.Context;
import javax.transaction.TransactionManager;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.SimpleMock;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.TranslatorException;

import bitronix.tm.resource.jdbc.PoolingDataSource;

public class TestUCanAccessTranslator {
	
	public static void main(String[] args) throws VirtualDatabaseException, TranslatorException, ConnectorManagerException, FileNotFoundException, IOException, SQLException {
		init();
		executeQuery(conn, "SELECT * FROM DATATYPE_TEST");
	}
	
	static Connection conn = null;
	
	@BeforeClass
	public static void init() throws TranslatorException, VirtualDatabaseException, ConnectorManagerException, FileNotFoundException, IOException, SQLException {
		
		System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "bitronix.tm.jndi.BitronixInitialContextFactory");
		
		EmbeddedServer server = new EmbeddedServer();
		
		UCanAccessExecutionFactory executionFactory = new UCanAccessExecutionFactory();
		executionFactory.start();
        server.addTranslator("translator-ucanaccess", executionFactory);
        
        setupTestDataSource();
        
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setTransactionManager(SimpleMock.createSimpleMock(TransactionManager.class));
        server.start(config);
        server.deployVDB(new FileInputStream(new File("src/test/resources/ucanaccess/vdb.xml")));
        conn = server.getDriver().connect("jdbc:teiid:UCanAccessVDB", null);
	}
	
	@Test
	public void testInsert() throws SQLException {
		executeUpdate(conn, "INSERT INTO T21 VALUES(100, 't')");
		executeQuery(conn, "SELECT * FROM T21");
		executeUpdate(conn, "DELETE FROM T21 WHERE ID = 100");
	}
	
	@Test
	public void testMetadata() throws SQLException {
		
		Set<String> nameSet = new HashSet<String>();
		DatabaseMetaData databaseMetaData = conn.getMetaData();
		
		ResultSet result = databaseMetaData.getTables("UCanAccessVDB", "TestData", null, null );
		while(result.next()) {
		    String tableName = result.getString(3);
		    nameSet.add(tableName);
		}
		
		result = databaseMetaData.getTables("UCanAccessVDB", "TestUCanAccess", null, null );
		while(result.next()) {
		    String tableName = result.getString(3);
		    nameSet.add(tableName);
		}
		
		close(result, null);
		
		assertEquals(6, nameSet.size());
		assertTrue(nameSet.contains("EmpDataView"));
		assertTrue(nameSet.contains("EMPDATA"));
		assertTrue(nameSet.contains("EMPDATA_TEST"));
		assertTrue(nameSet.contains("T20"));
		assertTrue(nameSet.contains("T21"));
		assertTrue(nameSet.contains("T21View"));
		
//		assertTrue(nameSet.contains(""));
		
	}
	
	@Test
	public void testSelect() throws SQLException {
		executeQuery(conn, "SELECT * FROM EMPDATA");
		executeQuery(conn, "SELECT * FROM EMPDATA_TEST");
		executeQuery(conn, "SELECT * FROM EmpDataView");
		
		executeQuery(conn, "SELECT * FROM DATATYPE_TEST");
		
		try {
			JDBCUtil.printTableColumn(conn, "SELECT * FROM DATATYPE_TEST");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		executeQuery(conn, "");
	}
	
	@Test
	public void testFunctions() throws SQLException {
		executeQuery(conn, "SELECT ASCII('A') FROM T20");
		executeQuery(conn, "SELECT CURDATE() FROM T20");
		executeQuery(conn, "SELECT CURTIME() FROM T20");

	}
	
	@Test
	public void testAccessLike() throws SQLException {
		executeQuery(conn, "SELECT * FROM T21 ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21 WHERE DESCR = 'aba' ORDER BY ID DESC");
		executeQuery(conn, "select * from T21 WHERE DESCR like 'a*a' ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21 WHERE DESCR like 'a*a' AND '1'='1' ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21 WHERE DESCR like 'a%a' ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21 WHERE DESCR like 'a%a' AND '1'='1' ORDER BY ID DESC");

		executeQuery(conn, "SELECT * FROM T21View ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21View WHERE DESCR = 'aba' ORDER BY ID DESC");
		executeQuery(conn, "select * from T21View WHERE DESCR like 'a*a' ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21View WHERE DESCR like 'a*a' AND '1'='1' ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21View WHERE DESCR like 'a%a' ORDER BY ID DESC");
		executeQuery(conn, "SELECT * FROM T21View WHERE DESCR like 'a%a' AND '1'='1' ORDER BY ID DESC");
		
	}
	
	@Test
	public void testAggregateFunctions() throws SQLException {

		executeQuery(conn, "SELECT * FROM T20");
		executeQuery(conn, "SELECT COUNT(*) FROM T20");
		executeQuery(conn, "SELECT COUNT(ID) FROM T20");
		executeQuery(conn, "SELECT SUM(ID) FROM T20");
		executeQuery(conn, "SELECT AVG(ID) FROM T20");
		executeQuery(conn, "SELECT MIN(ID) FROM T20");
		executeQuery(conn, "SELECT MAX(ID) FROM T20");
		
	}
	
	private static void setupTestDataSource() {
		
		PoolingDataSource pds = new PoolingDataSource();
		pds.setUniqueName("java:/UCanAccessDS");
        pds.setClassName("bitronix.tm.resource.jdbc.lrc.LrcXADataSource");
        pds.setMaxPoolSize(5);
        pds.setAllowLocalTransactions(true);
        pds.getDriverProperties().put("user", "");
        pds.getDriverProperties().put("password", "");
        pds.getDriverProperties().put("url", "jdbc:ucanaccess://src/test/resources/ucanaccess/ODBCTesting.accdb");
        pds.getDriverProperties().put("driverClassName", "net.ucanaccess.jdbc.UcanaccessDriver");
        pds.init();
		
	}
	
	static void executeQuery(Connection conn, String sql) throws SQLException {
		
		System.out.println("Query: " + sql);
                
        Statement stmt = null;
        ResultSet rs = null;
        
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData metadata = rs.getMetaData();
            int columns = metadata.getColumnCount();
            for (int row = 1; rs.next(); row++) {
                System.out.print(row + ": ");
                for (int i = 0; i < columns; i++) {
                    if (i > 0) {
                        System.out.print(", ");
                    }
                    System.out.print(rs.getObject(i + 1));
                }
                System.out.println();
            }
        }  finally {
            close(rs, stmt);
        }
        
        System.out.println();
        
    }
	
	static boolean executeUpdate(Connection conn, String sql) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
		}  finally {
			close(null, stmt);
		}
		return true;
	}
	
	static void close(ResultSet rs, Statement stmt) {

        if (null != rs) {
            try {
                rs.close();
                rs = null;
            } catch (SQLException e) {
            }
        }
        
        if(null != stmt) {
            try {
                stmt.close();
                stmt = null;
            } catch (SQLException e) {
            }
        }
    }

	@AfterClass
	public static void destory() throws SQLException{
		if(null != conn){
            conn.close();
        }
	}

}
