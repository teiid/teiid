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
package org.teiid.connector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.basic.BasicConnectorCapabilities;
import org.teiid.connector.basic.WrappedConnector;


public class TestWrappedConnector {

    @Test public void testConnectorCapabilitiesOverride() throws Exception {

    	Connector c = Mockito.mock(Connector.class);
    	Mockito.stub(c.getCapabilities()).toReturn(new BasicConnectorCapabilities());
    	ConnectorEnvironment env = Mockito.mock(ConnectorEnvironment.class);
    	Mockito.stub(c.getConnectorEnvironment()).toReturn(env);
    	
    	WrappedConnector connector = new WrappedConnector(c, null, null);
    	
    	ConnectorCapabilities caps = connector.getCapabilities();
    	assertFalse(caps.supportsExistsCriteria());
    	assertFalse(caps.supportsExcept());

    	
    	c = Mockito.mock(Connector.class);
    	Mockito.stub(c.getCapabilities()).toReturn(new BasicConnectorCapabilities());
    	
    	connector = new WrappedConnector(c, null, null);
    	
    	Properties props = new Properties();
    	props.setProperty("supportsExistsCriteria", "true"); //$NON-NLS-1$ //$NON-NLS-2$
    	props.setProperty("supportsExcept", "true"); //$NON-NLS-1$ //$NON-NLS-2$
    	
    	env = Mockito.mock(ConnectorEnvironment.class);
    	Mockito.stub(env.getOverrideCapabilities()).toReturn(props);
    	Mockito.stub(c.getConnectorEnvironment()).toReturn(env);    	
    	
    	caps = connector.getCapabilities();
    	assertTrue(caps.supportsExistsCriteria());
    	assertTrue(caps.supportsExcept());
    }
}
