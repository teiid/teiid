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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.teiid.jdbc.TeiidDataSource;

public class JDBCClient {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("usage: JDBCClient <vdb> <sql-command>");
			System.exit(-1);
		}

		System.out.println("Executing using the TeiidDriver");
		execute(getDriverConnection(args[0]), args[1]);

		System.out.println("");
		System.out.println("Executing using the TeiidDataSource");
		// this is showing how to make a Data Source connection. 
		execute(getDataSourceConnection(args[0]), args[1]);
	}
	
	static Connection getDriverConnection(String vdb) throws Exception {
		String url = "jdbc:metamatrix:"+vdb+"@../../deploy.properties";
		Class.forName("org.teiid.jdbc.TeiidDriver");
		
		return DriverManager.getConnection(url,"admin", "teiid");		
	}
	
	static Connection getDataSourceConnection(String vdb) throws Exception {
		TeiidDataSource ds = new TeiidDataSource();
		ds.setDatabaseName(vdb);
		ds.setUser("admin");
		ds.setPassword("teiid");
		ds.setEmbeddedBootstrapFile("../../deploy.properties");
		/* Alternatively server mode would be		
		 * ds.setServerName("localhost");
		 * ds.setPortNumber(31000);
		 */
		return ds.getConnection();
	}
	
	public static void execute(Connection connection, String sql) throws Exception {
		try {
			Statement statement = connection.createStatement();
			
			ResultSet results = statement.executeQuery(sql);
			
			ResultSetMetaData metadata = results.getMetaData();
			int columns = metadata.getColumnCount();
			
			while(results.next()) {
				for (int i = 0; i < columns; i++) {
					System.out.print(results.getString(i+1));
					System.out.print(",");
				}
				System.out.println("");
			}
			results.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}		
	}
}
