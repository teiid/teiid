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

package com.metamatrix.connector.xml.base;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.Execution;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.connector.xml.XMLExecution;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestXMLConnectorState extends TestCase {

	private Properties m_testFileProps;

    /**
     * Constructor for XMLConnectorStateTest.
     * @param arg0
     */
    public TestXMLConnectorState(String arg0) {
        super(arg0);
    }
    
  public void setUp() {
        m_testFileProps = new Properties();
        m_testFileProps.setProperty(XMLConnectorStateImpl.CACHE_TIMEOUT, new String("5000"));
        m_testFileProps.setProperty(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE, new String("50"));
        m_testFileProps.setProperty(XMLConnectorStateImpl.MAX_IN_MEMORY_STRING_SIZE, new String("1280"));
        m_testFileProps.setProperty(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE, new String("50"));
        m_testFileProps.setProperty(XMLConnectorStateImpl.CACHE_ENABLED, new String("true"));
        m_testFileProps.setProperty(XMLConnectorStateImpl.FILE_CACHE_LOCATION, new String("./test/cache"));
        m_testFileProps.setProperty(XMLConnectorStateImpl.SAX_FILTER_PROVIDER_CLASS, "com.metamatrix.connector.xml.base.NoExtendedFilters");
        m_testFileProps.setProperty(XMLConnectorStateImpl.QUERY_PREPROCESS_CLASS, "com.metamatrix.connector.xml.base.NoQueryPreprocessing");
        m_testFileProps.put(XMLConnectorStateImpl.CONNECTOR_CAPABILITES, "com.metamatrix.connector.xml.base.XMLCapabilities");
    }

    public void testXMLConnectorState() {
        XMLConnectorState state = new TestXMLConnectorStateImpl();
        assertNotNull(state);
    }

    public void testSetGetState() {
    	XMLConnectorStateImpl state = new TestXMLConnectorStateImpl();
        try {
        	state.setLogger(new SysLogger(false));
        	state.setState(EnvironmentUtility.createEnvironment(m_testFileProps));
            
        } catch (ConnectorException ce) {
         ce.printStackTrace();
         fail(ce.getMessage());
        }
    	assertNotNull(state.getState());
    	assertEquals(m_testFileProps.getProperty(XMLConnectorStateImpl.CACHE_TIMEOUT), 
    			state.getState().getProperty(XMLConnectorStateImpl.CACHE_TIMEOUT));    	
    	assertEquals(m_testFileProps.getProperty(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE), 
    			state.getState().getProperty(XMLConnectorStateImpl.MAX_FILE_CACHE_SIZE));
    	assertEquals(m_testFileProps.getProperty(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE), 
    			state.getState().getProperty(XMLConnectorStateImpl.MAX_MEMORY_CACHE_SIZE));
    	assertEquals(m_testFileProps.getProperty(XMLConnectorStateImpl.FILE_CACHE_LOCATION), 
    			state.getState().getProperty(XMLConnectorStateImpl.FILE_CACHE_LOCATION));
    }
    
    private class TestXMLConnectorStateImpl extends XMLConnectorStateImpl {
    	
    	TestXMLConnectorStateImpl() {
    		super();
    	}
    	

		/* (non-Javadoc)
		 * @see com.metamatrix.connector.xml.base.XMLConnectorStateImpl#getExecutor(com.metamatrix.connector.xml.base.XMLExecutionImpl)
		 */
		public DocumentProducer makeExecutor(XMLExecutionImpl info) {
			return null;
		}


		public DocumentProducer makeExecutor(Execution info) throws ConnectorException {
			// TODO Auto-generated method stub
			return null;
		}


		public DocumentProducer makeExecutor(XMLExecution info) throws ConnectorException {
			// TODO Auto-generated method stub
			return null;
		}


		public Connection getConnection(CachingConnector connector, ExecutionContext context, ConnectorEnvironment environment) throws ConnectorException {
			// TODO Auto-generated method stub
			return null;
		}
    }

}
