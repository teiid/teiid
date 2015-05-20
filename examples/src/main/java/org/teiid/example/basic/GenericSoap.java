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

import java.io.ByteArrayInputStream;
import java.sql.CallableStatement;
import java.util.concurrent.ArrayBlockingQueue;

import org.teiid.core.types.SQLXMLImpl;
import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ws.WSExecutionFactory;
import org.teiid.util.StAXSQLXML;

/**
 * This example shows invoking a generic soap service.
 * 
 * Other web service options include:
 * - soap service defined by wsdl
 * - arbitrary http call (could be rest)
 * - odata
 * etc.
 * 
 */
public class GenericSoap extends ExampleBase {
	
	static final String GET_ALL = "<GetAllStateInfo xmlns=\"http://www.teiid.org/stateService/\"/>";
	static final String GET_ONE = "<GetStateInfo xmlns=\"http://www.teiid.org/stateService/\"><stateCode xmlns=\"\">CA</stateCode></GetStateInfo>";
	
	public void execute(String vdb) throws Exception {
	    execute(vdb, null);
	}
	
	@Override
	public void execute(String vdb, ArrayBlockingQueue<String> queue) throws Exception {

		server = new EmbeddedServer();
		
		WSExecutionFactory ef = new WSExecutionFactory();
		ef.start();
		server.addTranslator("translator-ws", ef);//$NON-NLS-1$
		
		WSManagedConnectionFactory wsmcf = new WSManagedConnectionFactory();
		server.addConnectionFactory("java:/StateServiceWebSvcSource", wsmcf.createConnectionFactory());//$NON-NLS-1$
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:StateServiceVDB", null);//$NON-NLS-1$
		
		CallableStatement cStmt = conn.prepareCall("{call invoke(?, ?, ?, ?, ?)}");//$NON-NLS-1$
		cStmt.setString(1, "SOAP11");//$NON-NLS-1$
		cStmt.setString(2, "");
		cStmt.setObject(3, new SQLXMLImpl(GET_ALL));
		cStmt.setString(4, "http://localhost:8080/StateService/stateService/StateServiceImpl?WSDL");//$NON-NLS-1$
		cStmt.setBoolean(5, Boolean.TRUE);
		cStmt.execute();
		StAXSQLXML xml = (StAXSQLXML) cStmt.getObject(1);
		add(queue, xml.getString());
		
		add(queue,"\n"); //$NON-NLS-1$
		
		cStmt = conn.prepareCall("{call invoke(?, ?, ?, ?, ?)}");//$NON-NLS-1$
		cStmt.setString(1, "SOAP11");
		cStmt.setString(2, "");
		cStmt.setObject(3, new SQLXMLImpl(GET_ONE));
		cStmt.setString(4, "http://localhost:8080/StateService/stateService/StateServiceImpl?WSDL");//$NON-NLS-1$
		cStmt.setBoolean(5, Boolean.TRUE);
		cStmt.execute();
		xml = (StAXSQLXML) cStmt.getObject(1);
		add(queue, xml.getString());
		
		tearDown();
		
		add(queue, "Exit");
	}	
	
	public static void main(String[] args) throws Exception {
		new GenericSoap().execute(FileUtils.readFileContent("generic-soap", "webservice-vdb.xml"));
	}
}
