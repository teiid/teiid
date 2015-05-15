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
package org.teiid.example.basic;

import static org.teiid.example.util.JDBCUtils.executeQuery;
import static org.teiid.example.util.JDBCUtils.executeUpdate;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.mongodb.MongoDBManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.mongodb.MongoDBExecutionFactory;

public class TeiidEmbeddedMongoDBDataSource  extends ExampleBase {
	
	private static String SERVERLIST = "127.0.0.1:27017" ;
	private static String DBNAME = "mydb" ;
	
	@Override
	public void execute(String vdb) throws Exception {
		
		initMongoProperties();

		server = new EmbeddedServer();
		
		MongoDBExecutionFactory factory = new MongoDBExecutionFactory();
		factory.start();
		server.addTranslator("translator-mongodb", factory);
		
		MongoDBManagedConnectionFactory managedconnectionFactory = new MongoDBManagedConnectionFactory();
		managedconnectionFactory.setRemoteServerList(SERVERLIST);
		managedconnectionFactory.setDatabase(DBNAME);
		server.addConnectionFactory("java:/mongoDS", managedconnectionFactory.createConnectionFactory());
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:nothwind", null);
		
		executeUpdate(conn, "DELETE FROM Employee");
		executeUpdate(conn, "INSERT INTO Employee(employee_id, FirstName, LastName) VALUES (1, 'Teiid', 'JBoss')");
		executeUpdate(conn, "INSERT INTO Employee(employee_id, FirstName, LastName) VALUES (2, 'Teiid', 'JBoss')");
		executeQuery(conn, "SELECT * FROM Employee");
		
		tearDown();
	}

	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedMongoDBDataSource().execute(FileUtils.readFileContent("mongodb-as-a-datasource", "mongodb-vdb.xml"));
	}

	private void initMongoProperties() throws IOException {
		
		Properties prop = new Properties();
		InputStream input = null;
		
		try { 
			input = new FileInputStream(FileUtils.readFile("mongodb-as-a-datasource", "mongodb.properties"));
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
