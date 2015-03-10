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
package org.teiid.example;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.sql.DataSource;

import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.hbase.HBaseExecutionFactory;

import bitronix.tm.resource.jdbc.PoolingDataSource;

@SuppressWarnings("nls")
public class TeiidEmbeddedHBaseDataSource {
	
	private static void execute(Connection connection, String sql, boolean closeConn) throws Exception {
		try {
			Statement statement = connection.createStatement();
			
			boolean hasResults = statement.execute(sql);
			if (hasResults) {
				ResultSet results = statement.getResultSet();
				ResultSetMetaData metadata = results.getMetaData();
				int columns = metadata.getColumnCount();
				System.out.println("Results");
				for (int row = 1; results.next(); row++) {
					System.out.print(row + ": ");
					for (int i = 0; i < columns; i++) {
						if (i > 0) {
							System.out.print(",");
						}
						System.out.print(results.getString(i+1));
					}
					System.out.println();
				}
				results.close();
			}
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null && closeConn) {
				connection.close();
			}
		}		
	}

	public static void main(String[] args) throws Exception {
		
		DataSource ds = setupPhoenixDataSource("java:/hbaseDS", JDBC_DRIVER, JDBC_URL, JDBC_USER, JDBC_PASS);
		initSamplesData(ds);
		
		System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "bitronix.tm.jndi.BitronixInitialContextFactory");
		
		EmbeddedServer server = new EmbeddedServer();
		
		HBaseExecutionFactory factory = new HBaseExecutionFactory();
		factory.start();
		server.addTranslator("translator-hbase", factory);
		
		server.start(new EmbeddedConfiguration());
    	
		server.deployVDB(new FileInputStream(new File("hbase-vdb.xml")));
		
		Connection c = server.getDriver().connect("jdbc:teiid:hbasevdb", null);
		
		execute(c, "SELECT * FROM Customer", false);
		execute(c, "SELECT * FROM Customer ORDER BY name, city DESC", true);
		
		server.stop();
	}
	
	private static void initSamplesData(DataSource ds) throws Exception {
		Connection conn = ds.getConnection();
		// init test data
		execute(conn, CUSTOMER, false);
		execute(conn, "UPSERT INTO \"Customer\" VALUES('101', 'Los Angeles, CA', 'John White', '$400.00', 'Chairs')", false);
		execute(conn, "UPSERT INTO \"Customer\" VALUES('102', 'Atlanta, GA', 'Jane Brown', '$200.00', 'Lamps')", false);
		execute(conn, "UPSERT INTO \"Customer\" VALUES('103', 'Pittsburgh, PA', 'Bill Green', '$500.00', 'Desk')", false);
		execute(conn, "UPSERT INTO \"Customer\" VALUES('104', 'St. Louis, MO', 'Jack Black', '$8000.00', 'Bed')", false);
		execute(conn, "UPSERT INTO \"Customer\" VALUES('105', 'Los Angeles, CA', 'John White', '$400.00', 'Chairs')", true);
	}

	static final String CUSTOMER = "CREATE TABLE IF NOT EXISTS \"Customer\"(\"ROW_ID\" VARCHAR PRIMARY KEY, \"customer\".\"city\" VARCHAR, \"customer\".\"name\" VARCHAR, \"sales\".\"amount\" VARCHAR, \"sales\".\"product\" VARCHAR)";
	
	static final String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    static final String JDBC_URL = "jdbc:phoenix:127.0.0.1:2181";
    static final String JDBC_USER = "";
    static final String JDBC_PASS = "";

	static PoolingDataSource pds = null ;
	
	private static DataSource setupPhoenixDataSource(String jndiName, String driver, String url, String user, String pass) {
        if (null != pds)
            return pds;
        
        pds = new PoolingDataSource();
        pds.setUniqueName(jndiName);
        pds.setClassName("bitronix.tm.resource.jdbc.lrc.LrcXADataSource");
        pds.setMaxPoolSize(5);
        pds.setAllowLocalTransactions(true);
        pds.getDriverProperties().put("user", user);
        pds.getDriverProperties().put("password", pass);
        pds.getDriverProperties().put("url", url);
        pds.getDriverProperties().put("driverClassName", driver);
        pds.init();
        
        return pds;
    }
}
