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

package com.metamatrix.connector.xml.base;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.file.FileConnectorState;


public class TestXMLConnector extends TestCase {
    
    private ConnectorEnvironment m_env;
    private ExecutionContext m_secCtx;
    
	public TestXMLConnector() {
		super();
	}

	/**
	 * @param arg0
	 */
	public TestXMLConnector(String arg0) {
		super(arg0);
	}
    
    @Override
	public void setUp() {        
        m_env = ProxyObjectFactory.getDefaultTestConnectorEnvironment();                
        
        m_secCtx = ProxyObjectFactory.getDefaultSecurityContext();        
    }
        
    public void testInitMethod() {
        //init test environment
        XMLConnector connector = new XMLConnector();          
        try {        
        	connector.start(m_env);
            assertNotNull("state is null", connector.getState());
            XMLConnectorState state = connector.getState();
            Properties testFileProps = ProxyObjectFactory.getDefaultFileProps();
            assertEquals(state.getMaxMemoryCacheSizeKB(), 
                    Integer.parseInt((String) testFileProps.get(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE)));
            assertEquals(state.getMaxFileCacheSizeKB(), 
                    Integer.parseInt((String) testFileProps.get(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE)));
            assertEquals(state.getCacheLocation(), (String) testFileProps.get(XMLConnectorStateImpl.FILE_CACHE_LOCATION));
            int expectedTimeout = Integer.parseInt((String) testFileProps.get(XMLConnectorStateImpl.CACHE_TIMEOUT));
            assertEquals(state.getCacheTimeoutSeconds(), expectedTimeout);
            assertNotNull("Logger is null", connector.getLogger());
        } catch (ConnectorException ex) {
        	ex.printStackTrace();
            fail(ex.getMessage());         
        }
        
    }
    
    public void testStart() throws Exception {
        XMLConnector connector = new XMLConnector();
         connector.start(m_env);
    }
    
    public void testStop() {
     XMLConnector connector = new XMLConnector();
     
     try {
      connector.start(m_env);
      
     } catch (ConnectorException ex) {
      ex.printStackTrace();
      fail(ex.getMessage());
     }
     assertNotNull(connector);
     connector.stop();
    }
    
    
    public void testGetConnection() {
    	XMLConnector connector = new XMLConnector();
                        
        try {
         connector.start(m_env);
         XMLConnectionImpl conn = (XMLConnectionImpl) connector.getConnection(m_secCtx);
         assertNotNull("XMLConnectionImpl is null", conn);
         
         // is the connector ref set?
         assertEquals(connector, conn.getConnector());
         
         //is the query id set?
         assertEquals(m_secCtx.getRequestIdentifier(), conn.getQueryId());
         
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    } 
    
    
    public void testUnsetState() {
        XMLConnector connector = new XMLConnector();
        
        try {
            XMLConnectionImpl conn = (XMLConnectionImpl) connector.getConnection(m_secCtx);
            fail("connector created a connection with unset state");           
        } catch (ConnectorException e) {
            
        }       
    }
    
    public void testInitializeFailure() {
    	XMLConnector connector = new XMLConnector();
    	try {
    		Properties testFileProps = new Properties(); 
    		testFileProps.put(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("5000"));
            testFileProps.put(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("50"));
            testFileProps.put(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("50"));
            testFileProps.put(XMLConnectorStateImpl.CACHE_ENABLED, Boolean.TRUE);
            testFileProps.put(XMLConnectorStateImpl.FILE_CACHE_LOCATION, new String("./test/cache"));
            testFileProps.setProperty(XMLConnectorState.STATE_CLASS_PROP, "sure.to.Fail");
            
            testFileProps.put(FileConnectorState.FILE_NAME, "state_college.xml");
            String localPath = "test/documents";
            String ccPath = "checkout/XMLConnectorFramework/" + localPath;
            if (new File(localPath).exists()) {
            	testFileProps.put(FileConnectorState.DIRECTORY_PATH, localPath);
            } else {
            	if (new File(ccPath).exists()) {
            		testFileProps.put(FileConnectorState.DIRECTORY_PATH, ccPath);
            	} else {
            		testFileProps.put(FileConnectorState.DIRECTORY_PATH, "");
            	}
            }
    	    ConnectorEnvironment env = EnvironmentUtility.createEnvironment(testFileProps);
    		connector.start(env);
    		fail("connector should have failed on get state");
    	} catch (ConnectorException e) {
    		assertTrue(true);
    	}
    }
    
    public void testLoggingInit() {
        XMLConnector connector = new XMLConnector();
        
        try {
            connector.start(m_env);
            assertNotNull(connector.getLogger());
            connector.getLogger().logInfo("Logger is properly initialized");
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());            
        }
        
    }
}
