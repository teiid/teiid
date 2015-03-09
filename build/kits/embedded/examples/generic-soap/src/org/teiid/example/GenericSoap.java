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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stax.StAXSource;

import org.teiid.core.types.SQLXMLImpl;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
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
@SuppressWarnings("nls")
public class GenericSoap {
	
	static final String GET_ALL = "<GetAllStateInfo xmlns=\"http://www.teiid.org/stateService/\"/>";
	static final String GET_ONE = "<GetStateInfo xmlns=\"http://www.teiid.org/stateService/\"><stateCode xmlns=\"\">CA</stateCode></GetStateInfo>";
	
	public static void main(String[] args) throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		
		WSExecutionFactory ef = new WSExecutionFactory();
		ef.start();
		es.addTranslator("translator-ws", ef);
		
		//add a connection factory
		WSManagedConnectionFactory wsmcf = new WSManagedConnectionFactory();
		es.addConnectionFactory("java:/StateServiceWebSvcSource", wsmcf.createConnectionFactory());
		
		es.start(new EmbeddedConfiguration());
		
		es.deployVDB(new FileInputStream(new File("webservice-vdb.xml")));
		
		Connection c = es.getDriver().connect("jdbc:teiid:StateServiceVDB", null);
		
		CallableStatement cStmt = c.prepareCall("{call invoke(?, ?, ?, ?, ?)}");
		cStmt.setString(1, "SOAP11");
		cStmt.setString(2, "");
		cStmt.setObject(3, getSQLXML(GET_ALL));
		cStmt.setString(4, "http://localhost:8080/StateService/stateService/StateServiceImpl?WSDL");
		cStmt.setBoolean(5, Boolean.TRUE);
		cStmt.execute();
		StAXSQLXML xml = (StAXSQLXML) cStmt.getObject(1);
		List<String> namelist = getResult(xml.getSource(StAXSource.class).getXMLStreamReader());
		for(String item : namelist) {
			System.out.println(item);
		}
		
		cStmt = c.prepareCall("{call invoke(?, ?, ?, ?, ?)}");
		cStmt.setString(1, "SOAP11");
		cStmt.setString(2, "");
		cStmt.setObject(3, getSQLXML(GET_ONE));
		cStmt.setString(4, "http://localhost:8080/StateService/stateService/StateServiceImpl?WSDL");
		cStmt.setBoolean(5, Boolean.TRUE);
		cStmt.execute();
		xml = (StAXSQLXML) cStmt.getObject(1);
		namelist = getResult(xml.getSource(StAXSource.class).getXMLStreamReader());
		System.out.println(namelist);
		
		c.close();
		es.stop();
	}

	private static List<String> getResult(XMLStreamReader reader) throws XMLStreamException {
		List<String> stateNames = new ArrayList<String>();
		while (true) {
			if (reader.getEventType() == XMLStreamConstants.END_DOCUMENT) {
				break;
			}		
			if (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
				String cursor = reader.getLocalName();
				if (cursor.equals("Name")) {
					reader.next();
					String value = reader.getText();
					stateNames.add(value);
				}
//				if (cursor.equals("Abbreviation")) {
//					reader.next();
//					String value = reader.getText();
//					System.out.print(value + " ");
//				}
//				if (cursor.equals("Capital")) {
//					reader.next();
//					String value = reader.getText();
//					System.out.print(value + " ");
//				}
//				if (cursor.equals("YearOfStatehood")) {
//					reader.next();
//					String value = reader.getText();
//					System.out.println(value + " ");
//				}
			}
			reader.next();
		}
		return stateNames;
	}

	private static Object getSQLXML(String request) {
		return new SQLXMLImpl(request);
	}	

}
