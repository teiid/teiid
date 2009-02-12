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


package com.metamatrix.connector.xml.file;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;

import org.jdom.Document;

import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.NamedDocumentExecutor;
import com.metamatrix.connector.xml.SAXFilterProvider;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.CriteriaDesc;
import com.metamatrix.connector.xml.base.DocumentInfo;
import com.metamatrix.connector.xml.base.LoggingInputStreamFilter;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.Response;
import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.connector.xml.base.XMLExtractor;
import com.metamatrix.connector.xml.cache.DocumentCache;
import com.metamatrix.connector.xml.cache.IDocumentCache;

public class FileExecutor implements DocumentProducer, NamedDocumentExecutor {

    public static final String PARM_FILE_NAME_TABLE_PROPERTY_NAME = "FileName"; //$NON-NLS-1$

    private String[] m_docs;

    private String m_directory;

    private FileConnectorState m_state;
    
    private XMLExecution execution;
    
    private XMLExtractor xmlExtractor;
    
    public FileExecutor(FileConnectorState state, XMLExecution execution) throws ConnectorException {
        m_state = state;
        this.execution = execution;
        String cacheLocation = m_state.getCacheLocation();
        File cacheFolder = (cacheLocation == null || cacheLocation.trim().length() ==0) ? null : new File(cacheLocation);
        xmlExtractor = new XMLExtractor(m_state.getMaxInMemoryStringSize(), m_state.isPreprocess(), m_state.isLogRequestResponse(), cacheFolder, m_state.getLogger());
        validateParams();
        String tableFileName = getTableFileName();
        String xmlFileName = m_state.getFileName();
        if (tableFileName != null && tableFileName.trim().length() == 0) {
        	tableFileName = null;
        }        
        if (xmlFileName.trim().length() == 0) {
        	xmlFileName = null;
        }
        String xmlFileDir = m_state.getDirectoryPath();

        m_directory = normalizePath(xmlFileDir);

        if (tableFileName == null && xmlFileName == null) {
            validateDirectory();
        } else {
            validateFile(tableFileName, xmlFileName, xmlFileDir);
        }
    }
    public int getDocumentCount() throws ConnectorException
    {
    	return m_docs.length;
    }

    public String getCacheKey(int i) throws ConnectorException
    {
        String myXmlFileName = m_directory + m_docs[i];
        return myXmlFileName;
    }

    public InputStream getDocumentStream(int i) throws ConnectorException
    {
        try {
        	String xmlFileName = m_directory + m_docs[i];
            File xmlFile = new File(xmlFileName);
            m_state.getLogger().logDetail("XML Connector Framework: retrieving document from " + xmlFileName); //$NON-NLS-1$
        	InputStream retval = new FileInputStream(xmlFile);
        	m_state.getLogger().logDetail("XML Connector Framework: retrieved file " + xmlFileName); //$NON-NLS-1$
        	return retval;
        } catch (IOException ioe) {
            throw new ConnectorException(ioe);
        }
    }
    
	/* There is no scenario in the File implementation that a query will be broken into
    	multiple invocations by the QueryProcessor.  So Invocaction number is ignored 
    	in this implementation, and instead we use the documentNumber.
    */
    public Response getXMLResponse(int invocationNumber) throws ConnectorException
    {
		IDocumentCache cache = execution.getCache();
		CriteriaDesc criterion = execution.getInfo().getResponseIDCriterion(); 
		ExecutionContext exeContext = execution.getExeContext();
        String requestID = exeContext.getRequestIdentifier();
        String partID = exeContext.getPartIdentifier();
        String executionID = exeContext.getExecutionCountIdentifier();
        String cacheReference = requestID + partID + executionID + Integer.toString(invocationNumber);
        String cacheKey;
        Response result;
		if(null != criterion) {
			cacheKey = (String)(criterion.getValues().get(0));
			result =  new Response(cacheKey, this, cache, cacheReference);
            execution.getConnection().getConnector().createCacheObjectRecord(requestID, partID, executionID,
                  Integer.toString(invocationNumber), cacheKey);
		} else {
           int documentCount = getDocumentCount();
           String[] cacheKeys = new String[documentCount];
           XMLDocument[] docs = new XMLDocument[documentCount];
           for (int docNumber = 0; docNumber < documentCount; docNumber++) {
               cacheKey = getCacheKey(docNumber);
               XMLDocument doc = DocumentCache.cacheLookup(cache, cacheKey, cacheReference);
               if(doc == null) {
                   String documentDistinguishingId = "";
                   if (documentCount > 1) {
                       documentDistinguishingId = ((NamedDocumentExecutor)this).getDocumentName(docNumber);
                   }
                   DocumentInfo info;
                   SAXFilterProvider provider = null;
                   provider = m_state.getSAXFilterProvider();
                   InputStream responseBody = getDocumentStream(docNumber);
                   InputStream filteredStream = addStreamFilters(responseBody, m_state.getLogger());
                   info = xmlExtractor.createDocumentFromStream(filteredStream, documentDistinguishingId, provider);
                   
                   Document domDoc = info.m_domDoc;
                   doc = new XMLDocument(domDoc, info.m_externalFiles);

                   cache.addToCache(cacheKey, doc, info.m_memoryCacheSize, cacheReference);
                   execution.getConnection().getConnector().createCacheObjectRecord(requestID, partID, executionID,
                         Integer.toString(docNumber), cacheKey);
               }
               docs[docNumber] = doc;
               cacheKeys[docNumber] = cacheKey;
           }
           result = new Response(docs, cacheKeys, this, cache, cacheReference);
        }
        
        return result;
    }

	public InputStream addStreamFilters(InputStream response, ConnectorLogger logger) throws ConnectorException {
		
		if(m_state.isLogRequestResponse()) {
			response = new LoggingInputStreamFilter(response, logger);
		}
		
		InputStream filter = null;
    	try {
    		Class pluggableFilter = Thread.currentThread().getContextClassLoader().loadClass(m_state.getPluggableInputStreamFilterClass());
    		Constructor ctor = pluggableFilter.getConstructor(
    				new Class[] { java.io.InputStream.class, com.metamatrix.connector.api.ConnectorLogger.class});
    		filter = (InputStream) ctor.newInstance(new Object[] {response, logger});
    	} catch (Exception cnf) {
    		throw new ConnectorException(cnf);
    	}
		return filter;
	}
	
    ////////////////////////////////////////////////////////
	// Implementation of the NamedDocumentExecutor Interface
    public String getDocumentName(int i) throws ConnectorException
    {
        return m_docs[i];
	}
    ////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////
	// Implementation of the RequestResponseDocumentProducer Interface
    public Serializable getRequestObject(int i) throws ConnectorException {
		return m_docs[i];       
	}

	public XMLDocument recreateDocument(Serializable requestObject) throws ConnectorException {
		String request = (String) requestObject;
		XMLDocument doc = null;
		try {
        	String xmlFileName = request;
            File xmlFile = new File(xmlFileName);
            m_state.getLogger().logDetail("XML Connector Framework: retrieving document from " + xmlFileName); //$NON-NLS-1$
        	InputStream retval = new FileInputStream(xmlFile);
        	m_state.getLogger().logDetail("XML Connector Framework: retrieved file " + xmlFileName); //$NON-NLS-1$
        	SAXFilterProvider provider = null;
        	try {
        		provider = m_state.getSAXFilterProvider();
        	} catch (Exception e) {
        		throw new ConnectorException(e);
        	}
        	DocumentInfo info = xmlExtractor.createDocumentFromStream(retval, request, provider);
        	doc = new XMLDocument();
        	doc.setContextRoot(info.m_domDoc);
        	doc.setExternalFiles(info.m_externalFiles);
        } catch (IOException ioe) {
            throw new ConnectorException(ioe);
        }				
		return doc;
	}
	////////////////////////////////////////////////////////
	
    private String getTableFileName() {
		String retval = execution.getInfo().getLocation();
        if (retval == null) {
            retval = execution.getInfo().getOtherProperties().getProperty(PARM_FILE_NAME_TABLE_PROPERTY_NAME);
        }
		return retval;
	}

	private String normalizePath(String path) {
        if (path.endsWith(File.separator)) {
            return path;            
        } else {
            return new String(path + File.separator);
        }
    }

    private void validateParams() throws ConnectorException {
        for (int i = 0; i < execution.getInfo().getRequestedColumns().size(); i++) {
            OutputXPathDesc xPath = (OutputXPathDesc) execution.getInfo()
                    .getRequestedColumns().get(i);
            if (xPath.isParameter()) {
                throw new ConnectorException(
                		com.metamatrix.connector.xml.file.Messages.getString("FileExecutor.input.not.supported.on.files")); //$NON-NLS-1$
            }
        }
    }

    /**
     * Initializes the internal list of XML Documents.  Throws a ConnectorException in the 
     * event that the directory contains no .xml files.
     * @throws ConnectorException
     */
    private void validateDirectory() throws ConnectorException {
        File dirFile = new File(m_directory);
        if(dirFile.exists()) {
        	String[] files = dirFile.list();
        	ArrayList xmlFiles = new ArrayList();
        	for( int i = 0; i < files.length; i++) {
        		boolean valid = validateFile(files[i], m_directory, true);
        		if(valid) {
        			xmlFiles.add(files[i]);
        		}
        	}
        	files = new String[xmlFiles.size()];
        	xmlFiles.toArray(files);
        	m_docs = files;
        	if (m_docs.length <= 0) {
                throw new ConnectorException(
                		com.metamatrix.connector.xml.file.Messages.getString("FileExecutor.empty.directory")); //$NON-NLS-1$
            }
        } else {
        	throw new ConnectorException(
            		com.metamatrix.connector.xml.file.Messages.getString("FileExecutor.mising.directory")); //$NON-NLS-1$
        }
    }

    private boolean validateFile(String fileName, String directory, boolean validateExtension) {
        boolean result = false;
    	String myXmlFileName = normalizePath(directory) + fileName;
        File xmlFile = new File(myXmlFileName);
        if (xmlFile.isFile()) {
            if (validateExtension) {
                if (myXmlFileName.toLowerCase().endsWith(".xml")) { //$NON-NLS-1$
                    result = true;
                }
            } else {
                result = true;
            }
        }
        return result;
	}

	private void validateFile(String tableFileName, String xmlFileName,
            String xmlFileDir) throws ConnectorException {
        m_docs = new String[1];
        m_docs[0] = (tableFileName == null) ? xmlFileName : tableFileName;
        boolean valid = validateFile(m_docs[0], xmlFileDir, false);
        if (!valid) {
            throw new ConnectorException(
            		com.metamatrix.connector.xml.file.Messages.getString("FileExecutor.not.file")); //$NON-NLS-1$
        }
    }
}
