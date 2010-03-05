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
package com.metamatrix.connector.xml.streaming;

import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.xml.transform.Source;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.BasicManagedConnectionFactory;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.Document;
import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.XMLRelationalExecution;
import com.metamatrix.connector.xml.base.XMLBaseManagedConnectionFactory;

public abstract class BaseStreamingExecution implements XMLRelationalExecution {

	private List<Object[]> results = new ArrayList<Object[]>();
	private int resultIndex = 0;

	// injected state
	protected Select query;
	protected RuntimeMetadata metadata;
	protected ExecutionContext context;
	protected XMLBaseManagedConnectionFactory config;

	public BaseStreamingExecution(Select command, XMLBaseManagedConnectionFactory config, RuntimeMetadata rtMetadata, ExecutionContext context) {
		this.query = command;
		this.metadata = rtMetadata;
		this.context = context;
		this.config = config;
	}

	public void cancel() throws ConnectorException {
		// nothing to do
	}
	
	private StreamingResultsProducer getStreamingProducer() throws ConnectorException {		
		return new StreamingResultsProducer(getExecutionInfo(), getSaxFilterProvider());
	}	
	
	private List<String> getXPaths() {
        XPathSplitter splitter = new XPathSplitter();
        try {
			return splitter.split(getExecutionInfo().getTableXPath());
		} catch (InvalidPathException e) {
			e.printStackTrace();
		}		
		return null;
	}
	/**
	 * Earlier implementations retrieved the XML in the execute method.  Because this can be any
	 * number of documents of any size, this caused memory problems because the xml was 
	 * completely realized in memory.  In this impl the setup work is done in execute and
	 * the xml is streamed in the next function.
	 */
	public List next() throws ConnectorException, DataNotAvailableException {
		this.context.keepExecutionAlive(true);
		
		List result = null;
		if (this.resultIndex == 0) {
			fillInResults();
		}
		
		if(resultIndex < results.size()) {
			result = Arrays.asList(results.get(resultIndex));
			++resultIndex;
		}
		return result;
	}

	private void fillInResults() throws ConnectorException {
		List<Object[]> rows;
		StreamingResultsProducer streamProducer = getStreamingProducer();
		Iterator<Document> streamIter = getStreamProducer().getXMLDocuments();
		while (streamIter.hasNext()) {
			Document xml = streamIter.next();
			// TODO: add stream filter class. --rareddy
			rows = streamProducer.getResult(xml, getXPaths());
			if (rows.isEmpty()) {
				continue;
			}
			results.addAll(rows);
		}
	}
	
    protected SQLXML convertToXMLType(Source value) throws ConnectorException {
    	if (value == null) {
    		return null;
    	}
    	Object result = this.config.getTypeFacility().convertToRuntimeType(value);
    	if (!(result instanceof SQLXML)) {
    		throw new ConnectorException(DataPlugin.Util.getString("unknown_object_type_to_tranfrom_xml")); //$NON-NLS-1$
    	}
    	return (SQLXML)result;
    }	
    
	@Override
	public SAXFilterProvider getSaxFilterProvider() throws ConnectorException {
		if (this.config.getSaxFilterProviderClass() == null) {
			return null;
		}
		return BasicManagedConnectionFactory.getInstance(SAXFilterProvider.class, this.config.getSaxFilterProviderClass(), null, null);
	}    
	
	
//	public SQLXML wrapInputStream() {
//		String className = env.getInputStreamFilterClass();
//		if (className == null) {
//			return stream;
//		}
//		
//		ArrayList ctors = new ArrayList();
//		ctors.add(stream);
//		return BasicManagedConnectionFactory.getInstance(FilterInputStream.class, className, ctors, null);	
//	}
}