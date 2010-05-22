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

package org.teiid.translator.xml.streaming;

import java.util.List;
import java.util.Map;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.StremableDocument;
import org.teiid.translator.xml.ExecutionInfo;
import org.teiid.translator.xml.SAXFilterProvider;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * Converts a XML InputStream into an List containing results based upon data
 * from ExecutionInfo. Elements of the List are Lists that represent rows in the table.
 * 
 */
public class StreamingResultsProducer {

	private StreamingRowCollector collector;
	private ElementProcessor elementProcessor;

    public StreamingResultsProducer(ExecutionInfo info, SAXFilterProvider filter) throws TranslatorException {
    	
		Map<String, String> namespace = info.getPrefixToNamespacesMap();
		XMLReader reader;
		try {
			reader = ReaderFactory.getXMLReader(filter);
		} catch (SAXException e) {
			throw new TranslatorException(e);
		} 
		
    	elementProcessor = new ElementProcessor(info);
        collector = new StreamingRowCollector(namespace, reader, elementProcessor);
    }

	/**
	 * 
	 * @param xml the xml Document
	 * @param xpaths the paths defined at the table level
	 * @return result set rows
	 * @throws TranslatorException
	 */
    public List<Object[]> getResult(StremableDocument xml, List<String> xpaths) throws TranslatorException {
		
		List<Object[]> rows;
		try {
			rows = collector.getElements(xml, xpaths);
		} catch (InvalidPathException e) {
			throw new TranslatorException(e);
		}
		
		return rows;
	}
}
