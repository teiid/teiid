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

package com.metamatrix.connector.xml.soap;

import java.util.List;

import junit.framework.TestCase;

import org.jdom.Attribute;
import org.jdom.Element;

public class SOAPDocBuilderTest extends TestCase {

	private final String USERNAME = "moi";
	private final String PASSWORD = "secret";

	
	public void testAddWSSecurityUserToken() {
		Element header = new Element("header");
		SOAPDocBuilder.addWSSecurityUserToken(header, USERNAME, PASSWORD);
		List elements = header.getChildren();
		assertEquals(1, elements.size());
		Element element = (Element) elements.get(0);
		assertEquals("Security", element.getName());
		
		elements = element.getChildren();
		assertEquals(1, elements.size());
		element = (Element) elements.get(0);
		assertEquals("UsernameToken", element.getName());
		List attributes = element.getAttributes();
		assertEquals("Number of attributes is wrong", 1, attributes.size());
		Attribute attr = (Attribute) attributes.get(0);
		assertEquals("Attribute name is wrong", "Id", attr.getName());
		assertEquals("Attribute value is wrong", "mm-soap", attr.getValue());
		
		elements = element.getChildren();
		assertEquals(4, elements.size());
		
		element = (Element) elements.get(0);
		assertEquals("Username", element.getName());
		assertEquals(USERNAME, element.getValue());
		
		element = (Element) elements.get(1);
		assertEquals("Password", element.getName());
		assertEquals(PASSWORD, element.getValue());
		
		element = (Element) elements.get(2);
		assertEquals("Nonce", element.getName());
		assertNotNull("The nonce is null", element.getValue());
		
		element = (Element) elements.get(3);
		assertEquals("Created", element.getName());
		assertNotNull("The creation time is null", element.getValue());
	}
	
	public void testaddSoapBasicAuth() {
		Element header = new Element("header");
		SOAPDocBuilder.addSoapBasicAuth(header, "moi", "secret");
		List children = header.getChildren();
		assertEquals(1, children.size());
		
		Element element = (Element) children.get(0);
		assertEquals("BasicAuth", element.getName());
		List attributes = element.getAttributes();
		assertEquals("Number of attributes is wrong", 1, attributes.size());
		Attribute attr = (Attribute) attributes.get(0);
		assertEquals("Attribute name is wrong", "mustUnderstand", attr.getName());
		assertEquals("Attribute value is wrong", "1", attr.getValue());
		
		children = element.getChildren();
		element = (Element) children.get(0);
		assertEquals("Name", element.getName());
		assertEquals(USERNAME, element.getValue());
		
		element = (Element) children.get(1);
		assertEquals("Password", element.getName());
		assertEquals(PASSWORD, element.getValue());
	}

}
