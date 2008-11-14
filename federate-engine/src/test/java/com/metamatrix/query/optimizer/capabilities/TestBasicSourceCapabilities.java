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

package com.metamatrix.query.optimizer.capabilities;

import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;

import junit.framework.*;

/**
 */
public class TestBasicSourceCapabilities extends TestCase {

    public TestBasicSourceCapabilities(String name) {
        super(name);
    }

    public void testPutGet() {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        
        // Check initial state
        assertEquals("Default value for capability should be false", false, caps.supportsCapability(Capability.QUERY_FROM_JOIN)); //$NON-NLS-1$
        
        // Change state
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN, true);        
        
        // Check current state
        assertEquals("Changed value for capability should be true", true, caps.supportsCapability(Capability.QUERY_FROM_JOIN));         //$NON-NLS-1$
    }
    
    
}
