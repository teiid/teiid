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
package org.teiid.deployers;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.junit.Test;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.TranslatorMetaData;
import org.teiid.translator.ExecutionFactory;


@SuppressWarnings("nls")
public class TestTranslatorMetadata {

	@Test
	public void testFormat() throws Exception {
		
		TranslatorMetaDataGroup group = new TranslatorMetaDataGroup();
		TranslatorMetaData tm = new TranslatorMetaData();
		group.translators.add(tm);
		
		tm.setExecutionFactoryClass(ExecutionFactory.class);
		tm.setName("Oracle");
		tm.addProperty("ExtensionTranslationClassName", "org.teiid.translator.jdbc.oracle.OracleSQLTranslator");
		
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {TranslatorMetaDataGroup.class});
		Marshaller marshell = jc.createMarshaller();
		marshell.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,new Boolean(true));
		
		StringWriter sw = new StringWriter();
		marshell.marshal(group, sw);
				
		System.out.println(sw.toString());		
		
		Unmarshaller un = jc.createUnmarshaller();
		group = (TranslatorMetaDataGroup)un.unmarshal(new StringReader(sw.toString()));
		
		tm = group.getTranslators().get(0);
		
		assertEquals("Oracle", tm.getName());
		assertEquals(ExecutionFactory.class.getName(), tm.getPropertyValue(Translator.EXECUTION_FACTORY_CLASS));
		assertEquals("org.teiid.translator.jdbc.oracle.OracleSQLTranslator", tm.getPropertyValue("ExtensionTranslationClassName"));
		
	}
}
