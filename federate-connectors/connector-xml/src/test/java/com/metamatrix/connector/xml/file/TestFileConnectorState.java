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

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.base.XMLExecutionImpl;
import com.metamatrix.data.exception.ConnectorException;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestFileConnectorState extends TestCase {

    /**
     * Constructor for FileConnectorStateTest.
     * @param arg0
     */
    public TestFileConnectorState(String arg0) {
        super(arg0);
    }
    
    
/*    public static Test suite() {
    	return new CoverageDecorator(FileConnectorStateTest.class, new Class[] {FileConnectorState.class}); 
    }
*/    

    public void testSetGetState() {
    	try {
    		Properties props = ProxyObjectFactory.getDefaultFileProps();
    		FileConnectorState state = new FileConnectorState();
	    	state.setLogger(new SysLogger(false));
    		state.setState(EnvironmentUtility.createEnvironment(props));
	    	Properties retProps = state.getState();
	    	assertNotNull(retProps);
	    	assertEquals(props.getProperty(FileConnectorState.FILE_NAME), retProps.getProperty(FileConnectorState.FILE_NAME));
	    	assertEquals(props.getProperty(FileConnectorState.DIRECTORY_PATH),
	    			retProps.getProperty(FileConnectorState.DIRECTORY_PATH));
	    } catch (ConnectorException e) {
	    	fail(e.getMessage());
	    }
    }

    public void testGetExecutor() {
       	Properties props = ProxyObjectFactory.getDefaultFileProps();
    	FileConnectorState state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	try {
    		state.setState(EnvironmentUtility.createEnvironment(props));
        	XMLExecutionImpl exen = ProxyObjectFactory.getDefaultXMLExecution(ProxyObjectFactory.getStateCollegeVDBLocation());
        	final int maxBatch = 50;
			exen.execute(ProxyObjectFactory.getDefaultIQuery(ProxyObjectFactory.getStateCollegeVDBLocation(), 
					"select Company_id from Company"), maxBatch);
			DocumentProducer exec = (DocumentProducer) state.makeExecutor(exen);
	    	assertNotNull(exec);
		} catch (ConnectorException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
    	
    }

    /*
     * Class under test for void FileConnectorState()
     */
    public void testFileConnectorState() {
    	FileConnectorState state = new FileConnectorState();
    	assertNotNull(state);
    }

    /*
     * Class under test for void FileConnectorState(Properties)
     */
    public void testFileConnectorStateProperties() {
       	try {
       		Properties props = ProxyObjectFactory.getDefaultFileProps();
	       	FileConnectorState state = new FileConnectorState();
	    	state.setLogger(new SysLogger(false));
	    	state.setState(EnvironmentUtility.createEnvironment(props));
	    	Properties retProps = state.getState();
	    	assertNotNull(retProps);
	    	assertEquals(props.getProperty(FileConnectorState.FILE_NAME), retProps.getProperty(FileConnectorState.FILE_NAME));
	    	assertEquals(props.getProperty(FileConnectorState.DIRECTORY_PATH), 
	    			retProps.getProperty(FileConnectorState.DIRECTORY_PATH));
       	} catch (ConnectorException e) {
	    	fail(e.getMessage());
	    }
    }


    public void testSetGetFileName() {
    	FileConnectorState state = new FileConnectorState();
    	String fileName = "foo.xml";
    	state.setFileName(fileName);
    	assertEquals(fileName, state.getFileName());
    	state.setFileName(null);
    	assertEquals("", state.getFileName());
    }

    public void testSetGetDirectoryPath() {
    	FileConnectorState state = new FileConnectorState();
    	String dir = "c:\temp";
    	state.setDirectoryPath(dir);
    	assertEquals(dir, state.getDirectoryPath());
    	state.setDirectoryPath(null);
    	assertEquals("", state.getDirectoryPath());
    }
}
