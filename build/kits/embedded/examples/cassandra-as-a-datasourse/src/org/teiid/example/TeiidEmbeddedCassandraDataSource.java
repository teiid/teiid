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
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.teiid.resource.adapter.cassandra.CassandraManagedConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.cassandra.CassandraExecutionFactory;

@SuppressWarnings("nls")
public class TeiidEmbeddedCassandraDataSource {
	
	private static String ADDRESS = "127.0.0.1";
	private static String KEYSPACE = "demo";
	
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
		
		initCassandraProperties();
		
		EmbeddedServer server = new EmbeddedServer();
		
		CassandraExecutionFactory factory = new CassandraExecutionFactory();
		factory.start();
		server.addTranslator("translator-cassandra", factory);
		
		CassandraManagedConnectionFactory managedconnectionFactory = new CassandraManagedConnectionFactory();
		managedconnectionFactory.setAddress(ADDRESS);
		managedconnectionFactory.setKeyspace(KEYSPACE);
		server.addConnectionFactory("java:/demoCassandra", managedconnectionFactory.createConnectionFactory());

		server.start(new EmbeddedConfiguration());
    	
		server.deployVDB(new FileInputStream(new File("cassandra-vdb.xml")));
		
		Connection c = server.getDriver().connect("jdbc:teiid:users", null);
		
		execute(c, "SELECT * FROM UsersView", true);
		
		server.stop();
	}

	private static void initCassandraProperties() throws IOException {
		
		Properties prop = new Properties();
		InputStream input = null;
		
		try { 
			input = new FileInputStream("cassandra.properties");
			prop.load(input);
			
			if(prop.getProperty("cassandra.address") != null) {
				ADDRESS = prop.getProperty("cassandra.address");
			}
			
			if(prop.getProperty("cassandra.keyspace") != null){
				KEYSPACE = prop.getProperty("cassandra.keyspace");
			}
	 
		} catch (IOException e) {
			throw e;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
