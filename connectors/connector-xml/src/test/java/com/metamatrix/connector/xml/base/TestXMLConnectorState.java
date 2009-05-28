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

import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.XMLConnectorState;

/**
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
    
  @Override
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
    	XMLConnectorState state = new TestXMLConnectorStateImpl();
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

		public Connection getConnection(CachingConnector connector, ExecutionContext context, ConnectorEnvironment environment) throws ConnectorException {
			return null;
		}
    }

}
