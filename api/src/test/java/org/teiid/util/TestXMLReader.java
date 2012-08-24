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

package org.teiid.util;

import static org.junit.Assert.*;

import java.io.StringReader;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.stax.StAXSource;

import org.junit.Test;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ObjectConverterUtil;

@SuppressWarnings("nls")
public class TestXMLReader {
	
	@Test public void testStreaming() throws Exception {
		StringBuilder xmlBuilder = new StringBuilder();
		xmlBuilder.append("<?xml version=\"1.0\"?><root>");
		for (int i = 0; i < 1000; i++) {
			xmlBuilder.append("<a></a>");
			xmlBuilder.append("<b></b>");
		}
		xmlBuilder.append("</root>");
		String xml = xmlBuilder.toString();
		
		StAXSource source = new StAXSource(XMLType.getXmlInputFactory().createXMLEventReader(new StringReader(xml)));
		XMLReader is = new XMLReader(source, XMLOutputFactory.newFactory());
		String str = ObjectConverterUtil.convertToString(is);
		assertEquals(xml, str);
	}

}
