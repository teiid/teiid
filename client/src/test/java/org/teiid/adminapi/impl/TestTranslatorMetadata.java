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
package org.teiid.adminapi.impl;

import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestTranslatorMetadata {

	@Test
	public void testFormat() throws Exception {
		TranslatorMetaData tm = new TranslatorMetaData();
		
		tm.setExecutionFactoryClass("org.teiid.resource.adapter.jdbc.JDBCExecutionFactory");
		tm.setXaCapable(true);
		tm.setName("Oracle");
		tm.addProperty("ExtensionTranslationClassName", "org.teiid.translator.jdbc.oracle.OracleSQLTranslator");
		
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {TranslatorMetaData.class});
		Marshaller marshell = jc.createMarshaller();
		marshell.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,new Boolean(true));
		
		StringWriter sw = new StringWriter();
		marshell.marshal(tm, sw);
				
		System.out.println(sw.toString());		
	}
}
