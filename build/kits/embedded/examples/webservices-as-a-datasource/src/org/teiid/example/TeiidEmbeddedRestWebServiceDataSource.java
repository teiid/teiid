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

import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ws.WSExecutionFactory;

@SuppressWarnings("nls")
public class TeiidEmbeddedRestWebServiceDataSource {
	
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
		
		EmbeddedServer server = new EmbeddedServer();
		
		WSExecutionFactory factory = new WSExecutionFactory();
		factory.start();
		server.addTranslator("translator-rest", factory);
		
		WSManagedConnectionFactory managedconnectionFactory = new WSManagedConnectionFactory();
		server.addConnectionFactory("java:/CustomerRESTWebSvcSource", managedconnectionFactory.createConnectionFactory());

		server.start(new EmbeddedConfiguration());
    	
		server.deployVDB(new FileInputStream(new File("restwebservice-vdb.xml")));
		
		Connection c = server.getDriver().connect("jdbc:teiid:restwebservice", null);
		
		execute(c, "SELECT * FROM CustomersView", true);
		
		server.stop();
	}

}
