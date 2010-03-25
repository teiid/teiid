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
package org.teiid.connector.xmlsource.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.Select;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.common.types.InputStreamFactory;
import com.metamatrix.connector.xml.Document;
import com.metamatrix.connector.xml.ResultProducer;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.QueryAnalyzer;
import com.metamatrix.connector.xml.streaming.BaseStreamingExecution;
import com.metamatrix.connector.xml.streaming.DocumentImpl;

public class FileResultSetExecution extends BaseStreamingExecution implements ResultProducer{

	private ExecutionInfo executionInfo;
	public static final String PARM_FILE_NAME_TABLE_PROPERTY_NAME = "FileName"; //$NON-NLS-1$
	private FileManagedConnectionFactory env;
	private File content;
	
	public FileResultSetExecution(Select command, FileManagedConnectionFactory config, RuntimeMetadata metadata, ExecutionContext context) {
		super(command, config, metadata, context);
		this.env = config;
	}

	@Override
	public void execute() throws ConnectorException {

			QueryAnalyzer analyzer = new QueryAnalyzer(query);
			this.executionInfo = analyzer.getExecutionInfo();
			
			String tableName = this.executionInfo.getLocation();
	        if (tableName == null) {
	        	tableName = this.executionInfo.getOtherProperties().get(PARM_FILE_NAME_TABLE_PROPERTY_NAME);
	        }
	        
	        if (tableName == null) {
	        	tableName = this.env.getFileName();
	        }
	        
	        if (tableName == null) {
	        	this.content = new File(this.env.getDirectoryLocation());
	        }
	        else {
	        	this.content = new File(this.env.getDirectoryLocation(), tableName);
	        }
	        
			validateParams();
			List<CriteriaDesc[]> requestPerms = analyzer.getRequestPerms();

			if (requestPerms.size() > 1) {
				throw new AssertionError("The QueryAnalyzer produced > 1 request permutation");
			}

			List<CriteriaDesc> criteriaList = Arrays.asList(requestPerms.get(0));
			this.executionInfo.setParameters(criteriaList);
	}
	
	/**
	 * Validates that the query can be supported.  Probably better suited to a call out from QueryAnalyzer.
	 * @throws ConnectorException
	 */
	private void validateParams() throws ConnectorException {
        for (int i = 0; i < this.executionInfo.getRequestedColumns().size(); i++) {
            OutputXPathDesc xPath = (OutputXPathDesc)this.executionInfo.getRequestedColumns().get(i);
            if (xPath.isParameter()) {
                throw new ConnectorException(XMLSourcePlugin.Util.getString("FileExecutor.input.not.supported.on.files")); //$NON-NLS-1$
            }
        }
    }
	
	@Override
	public ResultProducer getStreamProducer() throws ConnectorException {
		return this;
	}

	public Iterator<Document> getXMLDocuments() throws ConnectorException {
		return new XMLFileIterator(this.content, this.env.getCharacterEncodingScheme());
	}

	@Override
	public void close() throws ConnectorException {
		// Nothing to do
	}	
	
	@Override
	public ExecutionInfo getExecutionInfo() {
		return this.executionInfo;
	}	
	
	private class XMLFileIterator implements Iterator<Document> {

		private int docNumber = 0;
		private File[] files;
		private String encoding;
		private String queryId;

		public XMLFileIterator(File content, String encoding) {
			if (content.isDirectory()) {
				this.files = getFilesInDir(content);
			}
			else {
				files = new File[1];
				files[0] = content;
			}
			this.encoding = encoding;
			this.queryId = content.getName();
		}
		
		@Override
		public boolean hasNext() {
			return docNumber < files.length;
		}

		@Override
		public Document next() {
			if(!hasNext()) {
				throw new NoSuchElementException();
			}
			Document document;
			try {
				document = getDocument();
			} catch (ConnectorException e) {
				throw new NoSuchElementException(e.getMessage());
			}
			++docNumber;
			return document;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		private Document getDocument() throws ConnectorException {
			SQLXML xml = getDocumentStream(this.files[this.docNumber]);
			return new DocumentImpl(xml, queryId + docNumber);
		}
		
		private SQLXML getDocumentStream(final File xmlFile) throws ConnectorException {
			env.getLogger().logTrace("XML Connector Framework: retrieving document from " + xmlFile.getName()); //$NON-NLS-1$
	
			InputStreamFactory isf = new InputStreamFactory(this.encoding) {
				@Override
				public InputStream getInputStream() throws IOException {
					return new BufferedInputStream(new FileInputStream(xmlFile));
				}
			};				
			
			env.getLogger().logTrace("XML Connector Framework: retrieved file " + xmlFile.getName()); //$NON-NLS-1$
			return convertToXMLType(isf);
		}
		
	    private File[] getFilesInDir(File dirFile) {
	    	return dirFile.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".xml");
				}
			});
	    }		
	}

}
