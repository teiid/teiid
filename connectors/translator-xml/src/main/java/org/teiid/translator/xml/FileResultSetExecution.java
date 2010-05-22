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
package org.teiid.translator.xml;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.xml.streaming.InvalidPathException;
import org.teiid.translator.xml.streaming.StreamingResultsProducer;
import org.teiid.translator.xml.streaming.XPathSplitter;

public class FileResultSetExecution implements ResultSetExecution {
	public static final String PARM_FILE_NAME_TABLE_PROPERTY_NAME = "FileName"; //$NON-NLS-1$
	
	private ExecutionInfo executionInfo;
	private int docNumber = 0;
	private File[] content;
	private XMLExecutionFactory executionFactory;
	private List<StremableDocument> resultDocuments = null;
	private StreamingResultsProducer streamProducer;
	private List<Object[]> currentRowSet;
	private int currentRow = 0;
	
	
	public FileResultSetExecution(List<CriteriaDesc> requestParams, ExecutionInfo executionInfo, XMLExecutionFactory executionFactory, FileConnection connection) throws TranslatorException {

		this.executionInfo = executionInfo;

		String tableName = this.executionInfo.getLocation();
        if (tableName == null) {
        	tableName = this.executionInfo.getOtherProperties().get(PARM_FILE_NAME_TABLE_PROPERTY_NAME);
        }
        
        this.content = connection.getFiles(tableName);
		
		validateParams();

		this.executionInfo.setParameters(requestParams);
		this.executionFactory = executionFactory;
		this.streamProducer = new StreamingResultsProducer(this.executionInfo, this.executionFactory.getSaxFilterProvider());
	}

	/**
	 * Validates that the query can be supported.  Probably better suited to a call out from QueryAnalyzer.
	 * @throws ConnectorException
	 */
	private void validateParams() throws TranslatorException {
        for (int i = 0; i < this.executionInfo.getRequestedColumns().size(); i++) {
            OutputXPathDesc xPath = (OutputXPathDesc)this.executionInfo.getRequestedColumns().get(i);
            if (xPath.isParameter()) {
                throw new TranslatorException(XMLPlugin.getString("FileExecutor.input.not.supported.on.files")); //$NON-NLS-1$
            }
        }
    }	
	
	@Override
	public void execute() throws TranslatorException {
		if (this.content != null) {
			this.resultDocuments = new ArrayList<StremableDocument>();
			int i = 0;
			for(File f:this.content) {
				this.resultDocuments.add(getDocumentStream(f, i++));
			}
		}	
	}
	
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		if (this.currentRowSet == null) {
			while(this.docNumber < resultDocuments.size()) {
				this.currentRowSet = streamProducer.getResult(this.resultDocuments.get(this.docNumber++), getXPaths());
				this.currentRow = 0;
				if (this.currentRowSet.isEmpty()) {
					continue;
				}
			}
		}
		
		if (this.currentRowSet != null) {
			if (this.currentRow <= this.currentRowSet.size()) {
				List result =  Arrays.asList(this.currentRowSet.get(this.currentRow++));
				if(this.currentRow == this.currentRowSet.size()) {
					this.currentRowSet = null;
				}
				return result;
			}
		}
		return null;
	}

	private List<String> getXPaths() throws TranslatorException {
        XPathSplitter splitter = new XPathSplitter();
        try {
			return splitter.split(this.executionInfo.getTableXPath());
		} catch (InvalidPathException e) {
			throw new TranslatorException(e);
		}		
	}	
	
	private StremableDocument getDocumentStream(final File xmlFile, int fileNumber) {
		InputStreamFactory isf = new InputStreamFactory() {
			@Override
			public InputStream getInputStream() throws IOException {
				return new BufferedInputStream(new FileInputStream(xmlFile));
			}
		};				
		return new StremableDocument(this.executionFactory.convertToXMLType(isf), xmlFile.getName()+fileNumber);
	}	
	
	@Override
	public void cancel() throws TranslatorException {
		
	}

	@Override
	public void close() throws TranslatorException {
		
	}	
}
