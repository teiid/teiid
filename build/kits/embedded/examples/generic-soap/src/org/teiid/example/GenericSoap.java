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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.resource.cci.ConnectionFactory;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ws.WSExecutionFactory;

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
	
	public static void main(String[] args) throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		
		WSExecutionFactory ef = new WSExecutionFactory();
		
		//can use message mode if full control over the soap envelope is desired
		//ef.setDefaultServiceMode(Mode.MESSAGE);
		
		es.addTranslator("ws", ef);
		
		//add a connection factory
		WSManagedConnectionFactory wsmcf = new WSManagedConnectionFactory();
		ConnectionFactory cf = wsmcf.createConnectionFactory();
		es.addConnectionFactory("ws", cf);
		
		//deploy a simple vdb with a web service model
		ModelMetaData mmd = new ModelMetaData();
		mmd.setName("y");
		mmd.addSourceMapping("ws", "ws", "ws");
		es.deployVDB("x", mmd);
		
		
		Connection c = es.getDriver().connect("jdbc:teiid:x", null);
		Statement s = c.createStatement();
		
		//note the request is just the body contents since we are in payload mode, not message
		//here the action is used as the soap action for a soap 1.1 call
		ResultSet rs = s.executeQuery("select * from (call invoke(endpoint=>'http://www.w3schools.com/webservices/tempconvert.asmx', "
				+ "action=>'http://www.w3schools.com/webservices/CelsiusToFahrenheit',"
				+ "binding=>'SOAP11',"
				+ "request=>'"
				+ "<CelsiusToFahrenheit xmlns=\"http://www.w3schools.com/webservices/\"> <Celsius>12</Celsius> </CelsiusToFahrenheit>"
				+ "')) x");
		rs.next();
		
		//show the result message body
		System.out.println(rs.getString(1));
		
		//you can also post process the results.  for example with xmltable to extract the desired information
		
		c.close();
		es.stop();
	}	

}
