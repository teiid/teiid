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

package com.metamatrix.connector.text;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.data.api.ConnectorEnvironment;

/**
 */
public class TestTextConnector extends TestCase {
    private static final String DESC_FILE = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$

    public TestTextConnector(String name) {
        super(name);
    }

    public TextConnector helpSetUp() throws Exception {
        String descFile = DESC_FILE;
        Properties props = new Properties();
        props.put(TextPropertyNames.DESCRIPTOR_FILE, descFile);

        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        TextConnector connector = new TextConnector();
        // Init license checker with class, non-GUI notifier and don't exitOnFailure
        connector.initialize(env);
        return connector;
    }
    
    public void testInitialize() throws Exception {
        helpSetUp();
    }

    // descriptor and data file both are files
    public void testGetConnection() throws Exception{
        TextConnector connector = helpSetUp();
        TextConnection conn = (TextConnection) connector.getConnection(null);
        assertNotNull(conn);
    }

}
