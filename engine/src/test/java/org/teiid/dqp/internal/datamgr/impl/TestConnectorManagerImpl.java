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

/*
 * Date: Sep 17, 2003
 * Time: 5:36:02 PM
 */
package org.teiid.dqp.internal.datamgr.impl;

import static org.junit.Assert.*;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorIdentity;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.dqp.internal.cache.ResultSetCache;
import org.teiid.dqp.internal.datamgr.impl.TestConnectorWorkItem.QueueResultsReceiver;
import org.teiid.dqp.internal.pooling.connector.ConnectionPool;
import org.teiid.dqp.internal.pooling.connector.FakeSourceConnectionFactory;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.cache.FakeCache;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.service.ConnectorStatus;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.FakeMetadataService;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;

/**
 * JUnit test for TestConnectorManagerImpl
 */
public final class TestConnectorManagerImpl {
    private Properties helpGetAppProps() {
        Properties appProperties = new Properties();

        appProperties.setProperty(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, "AFakeConnectorBinding"); //$NON-NLS-1$
        appProperties.setProperty(ConnectorPropertyNames.MAX_RESULT_ROWS, "10"); //$NON-NLS-1$
        appProperties.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, FakeConnector.class.getName());

        return appProperties;
    }

    // =========================================================================
    //                         T E S T   C A S E S
    // =========================================================================

    @Test public void testStartFailsWithNullRequiredProp() throws Exception {
        ConnectorManager cm = new ConnectorManager();
        Properties appProperties = helpGetAppProps();
        // Remove required property
        appProperties.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, ""); //$NON-NLS-1$
        
        try {
            startConnectorManager(cm, appProperties);
    		cm.stop();
            fail("Able to start ConnectorManager with null required props."); //$NON-NLS-1$
        } catch (ApplicationLifecycleException e) {
        	assertEquals("Connector is missing required property ConnectorClass or wrong value supplied AFakeConnectorBinding<null> ", e.getMessage()); //$NON-NLS-1$
        } 
    }

    @Test public void testReceive() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	startConnectorManager(cm, helpGetAppProps());
        
        AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        QueueResultsReceiver receiver = new QueueResultsReceiver();
        cm.executeRequest(receiver, request);
        assertNotNull(receiver.getResults().poll(1000, TimeUnit.MILLISECONDS));
        cm.stop();
    }
    
    @Test public void testConnectorCapabilitiesOverride() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	startConnectorManager(cm, helpGetAppProps());

    	AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
    	
    	SourceCapabilities caps = cm.getCapabilities(request.getRequestID(), null, Mockito.mock(DQPWorkContext.class));
    	assertFalse(caps.supportsCapability(Capability.CRITERIA_EXISTS));
    	assertFalse(caps.supportsCapability(Capability.QUERY_EXCEPT));

    	
    	ConnectorManager cmnew = new ConnectorManager();
    	Properties props = helpGetAppProps();
    	props.setProperty("supportsExistsCriteria", "true"); //$NON-NLS-1$ //$NON-NLS-2$
    	props.setProperty("supportsExcept", "true"); //$NON-NLS-1$ //$NON-NLS-2$
    	startConnectorManager(cmnew, props);

    	SourceCapabilities capsnew = cmnew.getCapabilities(request.getRequestID(), null, Mockito.mock(DQPWorkContext.class));
    	assertTrue(capsnew.supportsCapability(Capability.CRITERIA_EXISTS));
    	assertTrue(capsnew.supportsCapability(Capability.QUERY_EXCEPT));
    }

	private void startConnectorManager(ConnectorManager cm, Properties props)
			throws ApplicationLifecycleException {
		cm.initialize(props);
        ApplicationEnvironment env = new ApplicationEnvironment();
        env.bindService(DQPServiceNames.METADATA_SERVICE, new FakeMetadataService());
        env.bindService(DQPServiceNames.TRANSACTION_SERVICE, new FakeTransactionService());
        cm.start(env);
	}
    
    @Test public void testIsXA() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
        Properties props = new Properties();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, FakeConnector.class.getName());
        props.setProperty(ConnectorPropertyNames.IS_XA, Boolean.TRUE.toString());
        startConnectorManager(cm, props);
        assertTrue(cm.isXa());
        cm.stop();
    }
    
    @Test public void testIsXA_Failure() throws Exception {
        ConnectorManager cm = new ConnectorManager();
        Properties props = new Properties();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, FakeSourceConnectionFactory.class.getName());
        props.setProperty(ConnectorPropertyNames.IS_XA, Boolean.TRUE.toString());
        try {
        	startConnectorManager(cm, props);
        } catch (ApplicationLifecycleException e) {
        	assertEquals("Connector \"Unknown_Binding_Name<null>\" was configured to support XA transactions, but the connector is not an XAConnector", e.getMessage()); //$NON-NLS-1$
        }
        cm.stop();
    }
    
    @Test public void testCaching() throws Exception {
    	ConnectorManager cm = new ConnectorManager() {
    		@Override
    		protected ResultSetCache createResultSetCache(Properties rsCacheProps) {
    			assertEquals(String.valueOf(3600000), rsCacheProps.get(ResultSetCache.RS_CACHE_MAX_AGE));
    			return new ResultSetCache(rsCacheProps, new FakeCache.FakeCacheFactory());
    		}
    	};
        Properties props = new Properties();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, FakeConnector.class.getName());
        props.setProperty(ConnectorPropertyNames.USE_RESULTSET_CACHE, Boolean.TRUE.toString());
        props.setProperty(ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, String.valueOf(-1));
        startConnectorManager(cm, props);
        ConnectorWrapper wrapper = cm.getConnector();
        FakeConnector fc = (FakeConnector)wrapper.getActualConnector();
        assertEquals(0, fc.getConnectionCount());
        assertEquals(0, fc.getExecutionCount());
        AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        request.setUseResultSetCache(true);
        QueueResultsReceiver receiver = new QueueResultsReceiver();
        cm.executeRequest(receiver, request);
        AtomicResultsMessage arm = receiver.getResults().poll(1000, TimeUnit.MILLISECONDS);
        assertEquals(-1, arm.getFinalRow());
        //get the last batch - it will be 0 sized
        cm.requstMore(request.getAtomicRequestID());
        assertNotNull(receiver.getResults().poll(1000, TimeUnit.MILLISECONDS));
        cm.closeRequest(request.getAtomicRequestID());
        assertEquals(1, fc.getConnectionCount());
        assertEquals(1, fc.getExecutionCount());

        //this request should hit the cache
        AtomicRequestMessage request1 = TestConnectorWorkItem.createNewAtomicRequestMessage(2, 1);
        request1.setUseResultSetCache(true);
        QueueResultsReceiver receiver1 = new QueueResultsReceiver();
        cm.executeRequest(receiver1, request1);
        arm = receiver1.getResults().poll(1000, TimeUnit.MILLISECONDS);
        assertEquals(5, arm.getFinalRow());
        assertEquals(1, fc.getConnectionCount());
        assertEquals(1, fc.getExecutionCount());
        
        cm.stop();
    }
    
    @Test public void testDefect19049() throws Exception {
        ConnectorManager cm = new ConnectorManager();
        Properties props = new Properties();
        final String connectorName = FakeConnector.class.getName();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, connectorName);
        URLClassLoader cl = new URLClassLoader(new URL[0]);
        startConnectorManager(cm, props);
        ((FakeConnector)cm.getConnector().getActualConnector()).setClassloader(cl);
        AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        QueueResultsReceiver receiver = new QueueResultsReceiver();
        cm.executeRequest(receiver, request);
        assertNotNull(receiver.getResults().poll(1000, TimeUnit.MILLISECONDS));
        cm.stop();
    }

    @Test public void testConnectorStatus() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	assertEquals(ConnectorStatus.NOT_INITIALIZED, cm.getStatus());
    	
    	Properties props = new Properties();
    	Connector mockConnector = Mockito.mock(Connector.class);
    	Connection mockConnection = Mockito.mock(Connection.class);
    	
    	Mockito.stub(mockConnector.getConnection((ExecutionContext)Mockito.anyObject())).toReturn(mockConnection);        
    	
        final String connectorName = mockConnector.getClass().getName();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, connectorName);
        startConnectorManager(cm, props);
        cm.setConnector(new ConnectorWrapper(mockConnector)); // to make them same connector

        // no identity can be defined
        assertEquals(ConnectorStatus.UNABLE_TO_CHECK, cm.getStatus());
    }
    
    @Test public void testConnectorStatus_alive() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	assertEquals(ConnectorStatus.NOT_INITIALIZED, cm.getStatus());
    	
    	Connector mockConnector = Mockito.mock(Connector.class);
    	Connection mockConnection = Mockito.mock(Connection.class);
    	ConnectorIdentity mockIdentity = Mockito.mock(ConnectorIdentity.class);
    	
    	Mockito.stub(mockConnector.getConnection((ExecutionContext)Mockito.anyObject())).toReturn(mockConnection);        
        Mockito.stub(mockConnector.createIdentity(null)).toReturn(mockIdentity);
        Mockito.stub(mockConnection.isAlive()).toReturn(true);
    	
        ConnectorWrapper wrapper = new ConnectorWrapper(mockConnector);
        
        wrapper.updateStatus();
        assertEquals(ConnectorStatus.OPEN, wrapper.getStatus());
    }
    
    @Test public void testConnectorStatus_unavailable() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	assertEquals(ConnectorStatus.NOT_INITIALIZED, cm.getStatus());
    	
    	Connector mockConnector = Mockito.mock(Connector.class);
    	Connection mockConnection = Mockito.mock(Connection.class);
    	ConnectorIdentity mockIdentity = Mockito.mock(ConnectorIdentity.class);
    	
    	Mockito.stub(mockConnector.getConnection((ExecutionContext)Mockito.anyObject())).toReturn(mockConnection);        
        Mockito.stub(mockConnector.createIdentity(null)).toReturn(mockIdentity);
        Mockito.stub(mockConnection.isAlive()).toReturn(false);
    	
        ConnectorWrapper wrapper = new ConnectorWrapper(mockConnector);
        
        wrapper.updateStatus();
        assertEquals(ConnectorStatus.DATA_SOURCE_UNAVAILABLE, wrapper.getStatus());
    }    
    
    @Test public void testConnectorStatus_exception() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	assertEquals(ConnectorStatus.NOT_INITIALIZED, cm.getStatus());
    	
    	Connector mockConnector = Mockito.mock(Connector.class);
    	ConnectorIdentity mockIdentity = Mockito.mock(ConnectorIdentity.class);
    	
    	Mockito.stub(mockConnector.getConnection((ExecutionContext)Mockito.anyObject())).toThrow(new ConnectorException());        
        Mockito.stub(mockConnector.createIdentity(null)).toReturn(mockIdentity);
    	
        ConnectorWrapper wrapper = new ConnectorWrapper(mockConnector);
        
        wrapper.updateStatus();
        assertEquals(ConnectorStatus.DATA_SOURCE_UNAVAILABLE, wrapper.getStatus());
    }     
}