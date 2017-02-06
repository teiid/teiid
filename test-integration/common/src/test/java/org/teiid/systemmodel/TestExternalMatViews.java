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

package org.teiid.systemmodel;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.util.ResultSetUtil;
import org.teiid.logging.LogManager;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.h2.H2ExecutionFactory;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

@SuppressWarnings("nls")
public class TestExternalMatViews {
    private static boolean DEBUG = false;
    
	private Connection conn;
	private FakeServer server;
	private static DataSource h2DataSource;
	
	private static final String statusTable = "CREATE TABLE status\n" + 
			"(\n" + 
			"  VDBName varchar(50) not null,\n" + 
			"  VDBVersion varchar(50) not null,\n" + 
			"  SchemaName varchar(50) not null,\n" + 
			"  Name varchar(256) not null,\n" + 
			"  TargetSchemaName varchar(50),\n" + 
			"  TargetName varchar(256) not null,\n" + 
			"  Valid boolean not null,\n" + 
			"  LoadState varchar(25) not null,\n" + 
			"  Cardinality long,\n" + 
			"  Updated timestamp not null,\n" + 
			"  LoadNumber long not null,\n" + 
			"  PRIMARY KEY (VDBName, VDBVersion, SchemaName, Name)\n" + 
			")";

	@BeforeClass
	public static void beforeClass() throws Exception {
		h2DataSource = getDatasource();
		
		Connection c = h2DataSource.getConnection();
		assertNotNull(c);
		c.createStatement().execute(statusTable);
		
		String matView = "CREATE table mat_v1 (col int primary key, col1 varchar(50))";
		String matView2 = "CREATE table mat_v2 (col int primary key, col1 varchar(50), loadnum long)";
		String matViewStage = "CREATE table mat_v1_stage (col int primary key, col1 varchar(50))";
		c.createStatement().execute(matView);
		c.createStatement().execute(matViewStage);
		c.createStatement().execute(matView2);
		c.createStatement().execute("CREATE table G1 (e1 int primary key, e2 varchar(50), LoadNumber long)");
		c.close();
	}
	
	@AfterClass
	public static void afterClass() {
		
	}
	
	@Before 
	public void setUp() throws Exception {
    	server = new FakeServer(true);
    	
    	if (DEBUG)
		LogManager.setLogListener(new org.teiid.logging.Logger() {
			@Override
			public void shutdown() {
			}
			@Override
			public void removeMdc(String key) {
			}
			@Override
			public void putMdc(String key, String val) {
			}
			
			@Override
			public void log(int level, String context, Throwable t, Object... msg) {
				StringBuilder sb = new StringBuilder();
				for (Object str:msg) {
					sb.append(str.toString());
				}
				System.out.println(sb.toString());
			}
			
			@Override
			public void log(int level, String context, Object... msg) {
				StringBuilder sb = new StringBuilder();
				for (Object str:msg) {
					sb.append(str.toString());
				}
				System.out.println(sb.toString());
			}
			
			@Override
			public boolean isEnabled(String context, int msgLevel) {
				return msgLevel <= 4;
			}
		});    	
    	Connection c = h2DataSource.getConnection();
    	c.createStatement().execute("delete from status");
    	c.createStatement().execute("delete from mat_v1");
    	c.createStatement().execute("delete from mat_v1_stage");
    	c.createStatement().execute("delete from mat_v2");
    }
	
	@After 
	public void tearDown() throws Exception {
		if (conn != null) {
			conn.close();
		}
		if (server != null) {
			server.stop();
		}
	}
	
	@Test
	public void testSwapScriptWithEagerUpdate() throws Exception {
		withSwapScripts(true);
	}

	@Test
	public void testSwapScriptWithFullRefresh() throws Exception {
		withSwapScripts(false);
	}
	
	private void withSwapScripts(boolean useUpdateScript) throws Exception {
		HardCodedExecutionFactory hcef = setupData();
		ModelMetaData sourceModel = setupSourceModel();
		ModelMetaData matViewModel = setupMatViewModel();

		ModelMetaData viewModel = new ModelMetaData();
		viewModel.setName("view1");
		viewModel.setModelType(Type.VIRTUAL);
		viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
				+ "OPTIONS (MATERIALIZED true, "
				+ "MATERIALIZED_TABLE 'matview.MAT_V1', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, " 
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATERIALIZED_STAGE_TABLE\" 'matview.MAT_V1_STAGE', "
                + "\"teiid_rel:MATVIEW_BEFORE_LOAD_SCRIPT\" 'execute matview.native(''truncate table MAT_V1_STAGE'')', "
                + "\"teiid_rel:MATVIEW_AFTER_LOAD_SCRIPT\"  "
                			+ "'begin "
                				+ "execute matview.native(''ALTER TABLE MAT_V1 RENAME TO MAT_V1_TEMP'');"
                				+ "execute matview.native(''ALTER TABLE MAT_V1_STAGE RENAME TO MAT_V1'');"
                				+ "execute matview.native(''ALTER TABLE MAT_V1_TEMP RENAME TO MAT_V1_STAGE''); "
                			+ "end', "
                + "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'THROW_EXCEPTION') "
				+ "AS select col, col1 from source.physicalTbl");
		server.deployVDB("comp", sourceModel, viewModel, matViewModel);
		
		Thread.sleep(1000);
		
		assertTest(useUpdateScript, hcef);
	}
	
	@Test
	public void testMergeDeleteWithFullRefresh() throws Exception {
		withMergeDelete(false);
	}
	
	@Test
	public void testMergeDeleteWithEagarUpdates() throws Exception {
		withMergeDelete(true);
	}
	
	private void withMergeDelete(boolean useUpdateScript) throws Exception {
		HardCodedExecutionFactory hcef = setupData();
		ModelMetaData sourceModel = setupSourceModel();
		ModelMetaData matViewModel = setupMatViewModel();
		
		ModelMetaData viewModel = new ModelMetaData();
		viewModel.setName("view1");
		viewModel.setModelType(Type.VIRTUAL);
		viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
				+ "OPTIONS (MATERIALIZED true, "
				+ "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, " 
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
				+ "AS select col, col1 from source.physicalTbl");
		server.deployVDB("comp", sourceModel, viewModel, matViewModel);
		
		Thread.sleep(1000);
		
		assertTest(useUpdateScript, hcef);
	}

	@Test
	public void testVDBImportScopeWarning() throws Exception {
        H2ExecutionFactory executionFactory = new H2ExecutionFactory();
        executionFactory.setSupportsDirectQueryProcedure(true);
        executionFactory.start();
        server.addTranslator("h2", executionFactory);
        server.addConnectionFactory("java:/matview-ds", h2DataSource);      
	    server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("child-vdb.xml")));
	    server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("parent-vdb.xml")));
	    
	    Thread.sleep(1000);
	    
	    String uidQuery = "SELECT UID FROM Sys.Tables WHERE VDBName = 'parent' AND SchemaName = 'VM1' AND Name = 'G1'";

	    conn = server.createConnection("jdbc:teiid:parent");
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(uidQuery);
        rs.next();
        String uid = rs.getString(1);
        
        String scopeQuery = "SELECT \"value\" from SYS.Properties WHERE UID = '"+uid
                +"' AND Name = '{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE'";
        s = conn.createStatement();
        rs = s.executeQuery(scopeQuery);
        rs.next();
        assertEquals("IMPORTED", rs.getString(1)); // check if the default switching working
        
        s = conn.createStatement();
        rs = s.executeQuery("select * from VM1.G1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("2", rs.getString(2));
        
        Connection c = h2DataSource.getConnection();
        rs = c.createStatement().executeQuery("SELECT VDBVersion FROm Status WHERE VDBName = 'child'");
        rs.next();
        assertEquals(1, rs.getInt(1)); // 1 means IMPORTED
        
        rs = s.executeQuery("select * from VM2.G1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("2", rs.getString(2));

        rs = c.createStatement().executeQuery("SELECT VDBVersion FROm Status WHERE VDBName = 'parent'");
        rs.next();
        assertEquals(0, rs.getInt(1)); // 0 means FULL
        
        conn.close();
        c.close();
	}
	
	@Test
	public void testInternalFullRefresh() throws Exception {
		internalWithSameExternalProcedures(false);
	}
	
	@Test
	public void testInternalWithEargerUpdates() throws Exception {
		internalWithSameExternalProcedures(true);
	}
	
	private void internalWithSameExternalProcedures(boolean useUpdateScript) throws Exception {
		HardCodedExecutionFactory hcef = setupData();
		ModelMetaData sourceModel = setupSourceModel();
		ModelMetaData matViewModel = setupMatViewModel();

		ModelMetaData viewModel = new ModelMetaData();
		viewModel.setName("view1");
		viewModel.setModelType(Type.VIRTUAL);
		viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
				+ "OPTIONS (MATERIALIZED true, "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
				+ "\"teiid_rel:MATVIEW_UPDATABLE\" true, "
				+ "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true) " 
				+ "AS select col, col1 from source.physicalTbl");
		server.deployVDB("comp", sourceModel, viewModel, matViewModel);
		
		Thread.sleep(1000);
		
		assertTest(useUpdateScript, hcef);
	}
	
	private void assertTest(boolean useUpdateScript, HardCodedExecutionFactory hcef)
			throws Exception, SQLException, InterruptedException {
		
		conn = server.createConnection("jdbc:teiid:comp");
		
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select * from view1.v1 order by col");
		rs.next();
		assertEquals(1, rs.getInt(1));
		assertEquals("town", rs.getString(2));
		
		rs.next();
		assertEquals(2, rs.getInt(1));
		assertEquals("state", rs.getString(2));
		
		rs.next();
		assertEquals(3, rs.getInt(1));
		assertEquals("country", rs.getString(2));
		
		rs.close();
		s.close();
				
		// 1st data change event, explicit update to matview using updateMatview, this should reset TTL
		hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl",
			Arrays.asList(
					Arrays.asList(1, "city"), // update
					Arrays.asList(2, "state"),
					// delete
					Arrays.asList(4, "USA"))); // insert
		
		hcef.addData("SELECT physicalTbl.col FROM physicalTbl",
				Arrays.asList(
						Arrays.asList(1), 
						Arrays.asList(2),
						Arrays.asList(4)));
				
		if (useUpdateScript) {
			Connection admin = server.createConnection("jdbc:teiid:comp");
			CallableStatement stmt = admin.prepareCall("{? = call SYSADMIN.updateMatView(schemaName=>'view1', viewName=>'v1', refreshCriteria=>'v1.col in(1,3,4)')}");
			stmt.registerOutParameter(1, Types.INTEGER);
			stmt.execute();
			assertTrue(stmt.getInt(1) <= 4);
			admin.close();
		} else {
			Thread.sleep(3000);
		}
		
		Statement s1;
		ResultSet rs1;
		s1 = conn.createStatement();
		rs1 = s1.executeQuery("select * from view1.v1 order by col");
		rs1.next();
		assertEquals(1, rs1.getInt(1));
		assertEquals("city", rs1.getString(2));
		
		rs1.next();
		assertEquals(2, rs1.getInt(1));
		assertEquals("state", rs1.getString(2));
		
		rs1.next();
		assertEquals(4, rs1.getInt(1));
		assertEquals("USA", rs1.getString(2));
		
		rs1.close();
		s1.close();
	}

	private HardCodedExecutionFactory setupData() throws TranslatorException {
	    return setupData(false);
	}

	private HardCodedExecutionFactory setupData(final boolean supportsEQ) throws TranslatorException {
		H2ExecutionFactory executionFactory = new H2ExecutionFactory();
		executionFactory.setSupportsDirectQueryProcedure(true);
		executionFactory.start();
		server.addTranslator("translator-h2", executionFactory);
		server.addConnectionFactory("java:/matview-ds", h2DataSource);		

		HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
		    @Override
    		public boolean supportsCompareCriteriaEquals() {
		        return supportsEQ;
    		}  
		};
		hcef.addData("SELECT physicalTbl.col, physicalTbl.col1 FROM physicalTbl",
				Arrays.asList(
						Arrays.asList(1, "town"), 
						Arrays.asList(2, "state"),
						Arrays.asList(3, "country")));
		hcef.addData("SELECT physicalTbl.col FROM physicalTbl",
				Arrays.asList(
						Arrays.asList(1), 
						Arrays.asList(2),
						Arrays.asList(3)));

		server.addTranslator("fixed", hcef);
		return hcef;
	}

	private ModelMetaData setupMatViewModel() {
		ModelMetaData matViewModel = new ModelMetaData();
		matViewModel.setName("matview");
		matViewModel.setModelType(Type.PHYSICAL);
		matViewModel.addSourceMapping("s2", "translator-h2", "java:/matview-ds");
		matViewModel.addProperty("importer.schemaPattern", "PUBLIC");
		matViewModel.addProperty("importer.tableTypes", "TABLE");
		matViewModel.addProperty("importer.useFullSchemaName", "false");
		return matViewModel;
	}

	private ModelMetaData setupSourceModel() {
		ModelMetaData sourceModel = new ModelMetaData();
		sourceModel.setName("source");
		sourceModel.setModelType(Type.PHYSICAL);
		sourceModel.addSourceMetadata("DDL", "create foreign table physicalTbl (col integer, col1 string) options (updatable true);");
		sourceModel.addSourceMapping("s1", "fixed", null);
		return sourceModel;
	}	

	private static DataSource getDatasource() throws SQLException {
		final org.h2.Driver h2Driver = new org.h2.Driver();
		final Properties props = new Properties();		
		DataSource ds = new DataSource() {
			@Override
			public PrintWriter getLogWriter() throws SQLException {
				return null;
			}
			@Override
			public int getLoginTimeout() throws SQLException {
				return 0;
			}
			@Override
			public Logger getParentLogger() throws SQLFeatureNotSupportedException {
				return null;
			}
			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
			}
			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
			}
			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				return false;
			}

			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				return null;
			}

			@Override
			public Connection getConnection() throws SQLException {
				return h2Driver.connect("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", props);
			}
			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return h2Driver.connect("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", props);
			}
		};
		return ds;
	}
	
	@Test
	public void test() throws Exception {
		HardCodedExecutionFactory hcef = setupData();
		ModelMetaData sourceModel = setupSourceModel();
		ModelMetaData matViewModel = setupMatViewModel();
		
		ModelMetaData viewModel = new ModelMetaData();
		viewModel.setName("view1");
		viewModel.setModelType(Type.VIRTUAL);
		viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
				+ "OPTIONS (MATERIALIZED true, "
				+ "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, " 
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
				+ "AS select col, col1 from source.physicalTbl");
		server.deployVDB("comp", sourceModel, viewModel, matViewModel);		
		
		Connection admin = server.createConnection("jdbc:teiid:comp");
		execute(admin, "select * from SYSADMIN.Usage where Name = 'mat_v1'");
		admin.close();
	}	
	
    @Test
    public void testInternalWriteThroughMativew() throws Exception {
        HardCodedExecutionFactory hcef = setupData(true);
        ModelMetaData sourceModel = setupSourceModel();
        ModelMetaData matViewModel = setupMatViewModel();
        
        ModelMetaData viewModel = new ModelMetaData();
        viewModel.setName("view1");
        viewModel.setModelType(Type.VIRTUAL);
        viewModel.addSourceMetadata("DDL", "CREATE VIEW v1 (col integer primary key, col1 string) "
                + "OPTIONS (MATERIALIZED true,"
                + "UPDATABLE true, "
                + "MATERIALIZED_TABLE 'matview.MAT_V2', "
                + "\"teiid_rel:MATVIEW_TTL\" 3000, "
                + "\"teiid_rel:MATVIEW_WRITE_THROUGH\" true, "
                + "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" true, " 
                + "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'matview.STATUS', "
                + "\"teiid_rel:MATVIEW_LOADNUMBER_COLUMN\" 'loadnum') "
                + "AS select col, col1 from source.physicalTbl");
        server.deployVDB("comp", sourceModel, viewModel, matViewModel);     
        
        Connection c = server.createConnection("jdbc:teiid:comp");
        
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(3, rs.getInt(1));
        
        hcef.addUpdate("INSERT INTO physicalTbl (col, col1) VALUES (4, 'continent')", new int[] {1});
        s.execute("insert into v1 (col, col1) values (4, 'continent')");
        assertEquals(1, s.getUpdateCount());
        
        rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(4, rs.getInt(1));
        
        hcef.addUpdate("DELETE FROM physicalTbl WHERE physicalTbl.col1 = 'continent'", new int[] {1});
        s.execute("delete from v1 where v1.col1 = 'continent'");
        assertEquals(1, s.getUpdateCount());
        
        rs = s.executeQuery("select count(*) from v1");
        rs.next();
        assertEquals(3, rs.getInt(1));
        
        hcef.addUpdate("UPDATE physicalTbl SET col1 = 'town' WHERE physicalTbl.col1 = 'city'", new int[] {1});
        s.execute("update v1 set col1 = 'town' where col1 = 'city'");
        assertEquals(1, s.getUpdateCount());
        
        rs = s.executeQuery("select col, col1 from v1 where col = 1");
        rs.next();
        assertEquals("town", rs.getString(2));
    }
	
    public static boolean execute(Connection connection, String sql) throws Exception {
        boolean hasRs = true;
        try {
            Statement statement = connection.createStatement();
            hasRs = statement.execute(sql);
            if (!hasRs) {
                int cnt = statement.getUpdateCount();
                if (DEBUG) {
                    System.out.println("----------------\r");
                    System.out.println("Updated #rows: " + cnt);
                    System.out.println("----------------\r");
                }
            } else {
                ResultSet results = statement.getResultSet();
                if (DEBUG) {
                    ResultSetUtil.printResultSet(results);
                }
                results.close();
            }
            statement.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return hasRs;
    }	
}
