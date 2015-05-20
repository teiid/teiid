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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.cassandra.CassandraManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.cassandra.CassandraExecutionFactory;

public class TeiidEmbeddedCassandraDataSource extends ExampleBase {
	
	private static String ADDRESS = "127.0.0.1";
	private static String KEYSPACE = "demo";
	
	public void execute(String vdb) throws Exception {
        execute(vdb, null);
    }
	
	@Override
	public void execute(String vdb, ArrayBlockingQueue<String> queue) throws Exception {
		
		initCassandraProperties();

		server = new EmbeddedServer();
		
		CassandraExecutionFactory factory = new CassandraExecutionFactory();
		factory.start();
		server.addTranslator("translator-cassandra", factory); //$NON-NLS-1$
		
		CassandraManagedConnectionFactory managedconnectionFactory = new CassandraManagedConnectionFactory();
		managedconnectionFactory.setAddress(ADDRESS);
		managedconnectionFactory.setKeyspace(KEYSPACE);
		server.addConnectionFactory("java:/demoCassandra", managedconnectionFactory.createConnectionFactory()); //$NON-NLS-1$
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:users", null); //$NON-NLS-1$
		
		executeQuery(conn, "SELECT * FROM UsersView", queue); //$NON-NLS-1$
		
		tearDown();
		
		add(queue, "Exit"); //$NON-NLS-1$
	}

	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedCassandraDataSource().execute(FileUtils.readFileContent("cassandra-as-a-datasourse", "cassandra-vdb.xml")); //$NON-NLS-1$ //$NON-NLS-2$
	}

	private static void initCassandraProperties() throws IOException {
		
		Properties prop = new Properties();
		InputStream input = null;
		
		try { 
			input = new FileInputStream(FileUtils.readFile("cassandra-as-a-datasourse", "cassandra.properties")); //$NON-NLS-1$ //$NON-NLS-2$
			prop.load(input);
			
			if(prop.getProperty("cassandra.address") != null) { //$NON-NLS-1$
				ADDRESS = prop.getProperty("cassandra.address"); //$NON-NLS-1$
			}
			
			if(prop.getProperty("cassandra.keyspace") != null){ //$NON-NLS-1$
				KEYSPACE = prop.getProperty("cassandra.keyspace"); //$NON-NLS-1$
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
