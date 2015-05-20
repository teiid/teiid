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
import java.util.concurrent.ArrayBlockingQueue;

import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ws.WSExecutionFactory;

public class TeiidEmbeddedRestWebServiceDataSource extends ExampleBase {
	
    public void execute(String vdb) throws Exception {
        execute(vdb, null);
    }
    
	@Override
	public void execute(String vdb, ArrayBlockingQueue<String> queue) throws Exception {

		server = new EmbeddedServer();
		
		WSExecutionFactory factory = new WSExecutionFactory();
		factory.start();
		server.addTranslator("translator-rest", factory); //$NON-NLS-1$ 
		
		WSManagedConnectionFactory managedconnectionFactory = new WSManagedConnectionFactory();
		server.addConnectionFactory("java:/CustomerRESTWebSvcSource", managedconnectionFactory.createConnectionFactory()); //$NON-NLS-1$ 

		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:restwebservice", null); //$NON-NLS-1$ 
		
		executeQuery(conn, "SELECT customernumber, customername, city, country FROM CustomersView", queue); //$NON-NLS-1$ 
		
		tearDown();
		
		add(queue, "Exit"); //$NON-NLS-1$
	}

	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedRestWebServiceDataSource().execute(FileUtils.readFileContent("webservices-as-a-datasource", "restwebservice-vdb.xml")); //$NON-NLS-1$  //$NON-NLS-2$ 
	}

	

}
