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

package com.metamatrix.connector.yahoo;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;

/**
 */
public class TestYahooConnection extends TestCase {

    /**
     * @param name
     */
    public TestYahooConnection(String name) {
        super(name);
    }

    public void testConstructor() throws ConnectorException {
    	System.setProperty(CoreConstants.NO_CONFIGURATION, "");//$NON-NLS-1$
        Properties props = new Properties();
          
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        YahooConnection conn = new YahooConnection(env);
        
        assertNotNull(conn);
        
        conn.release();
        
    }
    
    public void testCapabilities() {
        Properties props = new Properties();
          
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        YahooConnection conn = new YahooConnection(env);

        ConnectorCapabilities caps = conn.getCapabilities();
        assertNotNull("Capabilities should not be null", caps); //$NON-NLS-1$
        
        conn.release();
    }

}
