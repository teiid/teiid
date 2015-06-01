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

import static org.teiid.example.util.JDBCUtils.execute;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

import org.teiid.resource.adapter.mongodb.MongoDBManagedConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.mongodb.MongoDBExecutionFactory;

@SuppressWarnings("nls")
public class TeiidEmbeddedMongoDBDataSource {
	
	private static String SERVERLIST = "127.0.0.1:27017" ;
	private static String DBNAME = "mydb" ;

	public static void main(String[] args) throws Exception {
		
		initMongoProperties();
		
		EmbeddedServer server = new EmbeddedServer();
		
		MongoDBExecutionFactory factory = new MongoDBExecutionFactory();
		factory.start();
		server.addTranslator("translator-mongodb", factory);
		
		MongoDBManagedConnectionFactory managedconnectionFactory = new MongoDBManagedConnectionFactory();
		managedconnectionFactory.setRemoteServerList(SERVERLIST);
		managedconnectionFactory.setDatabase(DBNAME);
		server.addConnectionFactory("java:/mongoDS", managedconnectionFactory.createConnectionFactory());

		server.start(new EmbeddedConfiguration());
    	
		server.deployVDB(new FileInputStream(new File("mongodb-vdb.xml")));
		
		Connection c = server.getDriver().connect("jdbc:teiid:nothwind", null);
		
		execute(c, "DELETE FROM Employee", false);
		execute(c, "INSERT INTO Employee(employee_id, FirstName, LastName) VALUES (1, 'Teiid', 'JBoss')", false);
		execute(c, "INSERT INTO Employee(employee_id, FirstName, LastName) VALUES (2, 'Teiid', 'JBoss')", false);
		execute(c, "SELECT * FROM Employee", true);
		
		server.stop();
	}

	private static void initMongoProperties() throws IOException {
		
		Properties prop = new Properties();
		InputStream input = null;
		
		try { 
			input = new FileInputStream("mongodb.properties");
			prop.load(input);
			
			if(prop.getProperty("server.list") != null) {
				SERVERLIST = prop.getProperty("server.list");
			}
			
			if(prop.getProperty("db.name") != null){
				DBNAME = prop.getProperty("db.name");
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
