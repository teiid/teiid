/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.ISelectSymbol;
import com.metamatrix.connector.metadata.runtime.Element;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.xml.MockXMLExecution;
import com.metamatrix.connector.xml.base.ExecutionInfo;
import com.metamatrix.connector.xml.base.OutputXPathDesc;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;
import com.metamatrix.connector.xml.base.XMLDocument;
import com.metamatrix.connector.xml.base.XMLExecutionImpl;
import com.metamatrix.core.util.UnitTestUtil;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestFileExecutor extends TestCase {

    /**
     * Constructor for FileExecutorTest.
     * @param arg0
     */
    public TestFileExecutor(String arg0) {
        super(arg0);
    }

    public void testValidate() {
    	try {
    		FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger(false));
        	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
        	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
        	MockXMLExecution exec = new MockXMLExecution(conn, meta);
        	FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    }
    
    public void testValidateSourceFromModel() {
    	try {
    		FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger(false));
        	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
        	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
        	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    		ExecutionInfo info = exec.getInfo();
        	Properties props = info.getOtherProperties();
        	props.put(FileExecutor.PARM_FILE_NAME_TABLE_PROPERTY_NAME, "state_college.xml");
        	info.setOtherProperties(props);
        	FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    	
    }
    
    public void testValidateSourceDirectory() {
    	try {
    		FileConnectorState state = new FileConnectorState();
        	state.setLogger(new SysLogger(false));
        	Properties props;
        	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
        	state.setFileName(null);
        	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
        	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
        	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    		ExecutionInfo info = exec.getInfo();
        	props = info.getOtherProperties();
        	props.put(FileExecutor.PARM_FILE_NAME_TABLE_PROPERTY_NAME, " ");
        	info.setOtherProperties(props);
        	FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    	
    }
    
    public void testValidateSourceDirectoryEmpty() {
	try {
		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	state.setFileName(null);
    	File nullDir = UnitTestUtil.getTestScratchFile("devnull");
    	if(!nullDir.exists()) {
    		assertTrue("could not create directory nulldir", nullDir.mkdir());
    	}
    	assertTrue("nulldir is not a directory", nullDir.isDirectory());
    	assertTrue("nulldir is not empty", nullDir.list().length == 0);
    	state.setDirectoryPath(nullDir.getAbsolutePath());
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
		ExecutionInfo info = exec.getInfo();
    	Properties props = info.getOtherProperties();
    	props.put(FileExecutor.PARM_FILE_NAME_TABLE_PROPERTY_NAME, " ");
    	info.setOtherProperties(props);
    	FileExecutor executor = new FileExecutor(state, exec);
		fail("empty directory should not validate");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}
    	
    }
    
    public void testValidateFileAsDirectory() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	state.setFileName(null);
    	File nullDir = UnitTestUtil.getTestScratchFile("devnull");
    	if(!nullDir.exists()) {
    		assertTrue("could not create directory nulldir", nullDir.mkdir());
    	}
    	assertTrue("nulldir is not a directory", nullDir.isDirectory());
    	assertTrue("nulldir is not empty", nullDir.list().length == 0);
    	state.setDirectoryPath(nullDir.getAbsolutePath());
    	state.setFileName(nullDir.getName());
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    	
    		FileExecutor executor = new FileExecutor(state, exec);
			fail("directory specified in filename should fail");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}	
    }
    
    public void testValidateParameter() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	String strQuery = "select DefaultedValue from DefaultedRequiredValueTable";
    	
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
		ExecutionInfo info = exec.getInfo();
		info.setColumnCount(1);
		ArrayList param = new ArrayList();
    			
			Element elem = getElement(strQuery);
			OutputXPathDesc desc = new OutputXPathDesc(elem);
			param.add(desc);
			info.setRequestedColumns(param);
	    	FileExecutor executor = new FileExecutor(state, exec);
			fail("should not validate a query with param columns");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}
    }
    
    public void testValidateNotXML() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	state.setFileName("StateCollege.vdb");
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    	
    		FileExecutor executor = new FileExecutor(state, exec);
		} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
    }

    public void testGetXMLDocument() {
    	try {
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	IQuery query = ProxyObjectFactory.getDefaultIQuery(ProxyObjectFactory.getStateCollegeVDBLocation(),
		"select Company_id from Company");
    	XMLExecutionImpl exec = ProxyObjectFactory.getDefaultXMLExecution(query, ProxyObjectFactory.getStateCollegeVDBLocation());
			exec.execute();
        	FileExecutor executor = new FileExecutor(state, exec);
	    	XMLDocument[] docs = executor.getXMLResponse(0).getDocuments();
	    	assertEquals(1, docs.length);
	    	assertNotNull(docs[0]);
    	} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

    }
    
    public void testGetXMLDocumentCache() {
    	try {
    	FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	IQuery query = ProxyObjectFactory.getDefaultIQuery(ProxyObjectFactory.getStateCollegeVDBLocation(),
		"select Company_id from Company");
    	XMLExecutionImpl exec = ProxyObjectFactory.getDefaultXMLExecution(query, ProxyObjectFactory.getStateCollegeVDBLocation());
			exec.execute();
			FileExecutor executor = new FileExecutor(state, exec);
	    	XMLDocument[] docs = executor.getXMLResponse(0).getDocuments();
	    	assertEquals(1, docs.length);
	    	assertNotNull(docs[0]);
	    	docs = executor.getXMLResponse(0).getDocuments();
	    	assertEquals(1, docs.length);
	    	assertNotNull(docs[0]);
    	} catch (ConnectorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

    }
 
    
    public void testGetXMLDocumentError() {
    	try {
            
    		FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(getEnv(ProxyObjectFactory.getDefaultFileProps()));
    	String fileName = "empty.xml";
    	File tempFile = new File(ProxyObjectFactory.getDocumentsFolder() + "/" + fileName);
    	if(!tempFile.exists()) {
    		try {
    			tempFile.createNewFile();
    		} catch(IOException ioe) {
    			fail("could not create temp file");
    		}
    	}
    	state.setFileName("empty.xml");
    	XMLConnectionImpl conn = ProxyObjectFactory.getDefaultXMLConnection();
    	RuntimeMetadata meta = ProxyObjectFactory.getDefaultRuntimeMetadata(ProxyObjectFactory.getStateCollegeVDBLocation());
    	MockXMLExecution exec = new MockXMLExecution(conn, meta);
    		FileExecutor executor = new FileExecutor(state, exec);
			assertTrue(tempFile.delete());
			tempFile = null;
			XMLDocument[] docs = executor.getXMLResponse(0).getDocuments();
			fail("should not be able to get non-existant document");
		} catch (ConnectorException e) {
			assertNotNull(e);
		}
    }
    
    
    private Element getElement(String query) throws ConnectorException {
    	return getElement(query, 0);
    }
    
    private Element getElement(String query, int colLocation)
			throws ConnectorException {
    	String vdbPath = ProxyObjectFactory.getStateCollegeVDBLocation();
        RuntimeMetadata metadata = ProxyObjectFactory.getDefaultRuntimeMetadata(vdbPath);
        IQuery iquery = ProxyObjectFactory.getDefaultIQuery(vdbPath, query);
    	ISelectSymbol symbol = (ISelectSymbol) iquery.getSelect().getSelectSymbols().get(colLocation);
    	IExpression expr = symbol.getExpression();
    	MetadataID elementID = (MetadataID) ((IElement) expr).getMetadataID();
    	Element elem = (Element) metadata.getObject(elementID);
    	return elem;        		
	}
    
    private ConnectorEnvironment getEnv(Properties props) {
    	return EnvironmentUtility.createEnvironment(props);
    }


}
