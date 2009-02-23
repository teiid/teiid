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
package com.metamatrix.dqp.internal.datamgr.impl;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.connector.internal.ConnectorPropertyNames;
import com.metamatrix.dqp.internal.datamgr.impl.TestConnectorWorkItem.QueueResultsReceiver;
import com.metamatrix.dqp.internal.pooling.connector.FakeSourceConnectionFactory;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.FakeMetadataService;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;

/**
 * JUnit test for TestConnectorManagerImpl
 */
public final class TestConnectorManagerImpl extends TestCase {
    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================
    /**
     * Constructor for TestConnectorManagerImpl.
     * @param name
     */
    public TestConnectorManagerImpl(final String name) {
        super(name);
    }

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

    public void testStartFailsWithNullRequiredProp() throws Exception {
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

    public void testReceive() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	startConnectorManager(cm, helpGetAppProps());
        
        AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        QueueResultsReceiver receiver = new QueueResultsReceiver();
        cm.executeRequest(receiver, request);
        assertNotNull(receiver.getResults().poll(1000, TimeUnit.MILLISECONDS));
        cm.stop();
    }
    
    public void testConnectorCapabilitiesOverride() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
    	startConnectorManager(cm, helpGetAppProps());

    	AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
    	
    	SourceCapabilities caps = cm.getCapabilities(request.getRequestID(), null, Mockito.mock(DQPWorkContext.class));
    	assertFalse(caps.supportsCapability(Capability.QUERY_WHERE_EXISTS));
    	assertFalse(caps.supportsCapability(Capability.QUERY_EXCEPT));

    	
    	ConnectorManager cmnew = new ConnectorManager();
    	Properties props = helpGetAppProps();
    	props.setProperty("supportsExistsCriteria", "true"); //$NON-NLS-1$ //$NON-NLS-2$
    	props.setProperty("supportsExcept", "true"); //$NON-NLS-1$ //$NON-NLS-2$
    	startConnectorManager(cmnew, props);

    	SourceCapabilities capsnew = cmnew.getCapabilities(request.getRequestID(), null, Mockito.mock(DQPWorkContext.class));
    	assertTrue(capsnew.supportsCapability(Capability.QUERY_WHERE_EXISTS));
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
    
    public void testIsXA() throws Exception {
    	ConnectorManager cm = new ConnectorManager();
        Properties props = new Properties();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, FakeConnector.class.getName());
        startConnectorManager(cm, props);
        assertTrue(cm.isXa());
        cm.stop();
        cm = new ConnectorManager();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, FakeSourceConnectionFactory.class.getName());
        startConnectorManager(cm, props);
        assertFalse(cm.isXa());
        cm.stop();
    }
    
    public void testDefect19049() throws Exception {
        ConnectorManager cm = new ConnectorManager();
        Properties props = new Properties();
        final String connectorName = FakeConnector.class.getName();
        props.setProperty(ConnectorPropertyNames.CONNECTOR_CLASS, connectorName);//$NON-NLS-1$
        URLClassLoader cl = new URLClassLoader(new URL[0]);
        startConnectorManager(cm, props);
        ((FakeConnector)cm.getConnector().getActualConnector()).setClassloader(cl);
        AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        QueueResultsReceiver receiver = new QueueResultsReceiver();
        cm.executeRequest(receiver, request);
        assertNotNull(receiver.getResults().poll(1000, TimeUnit.MILLISECONDS));
        cm.stop();
    }
    
}