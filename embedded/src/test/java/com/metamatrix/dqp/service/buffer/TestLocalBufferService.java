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

package com.metamatrix.dqp.service.buffer;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.application.Application;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.buffer.impl.BufferConfig;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.embedded.EmbeddedTestUtil;
import com.metamatrix.dqp.embedded.services.EmbeddedBufferService;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;

public class TestLocalBufferService extends TestCase {

    public TestLocalBufferService(String name) {
        super(name);
    }

    public void testMissingRequiredProperties() throws Exception {        
        try {
            System.setProperty("mm.io.tmpdir", System.getProperty("java.io.tmpdir")+"/metamatrix/1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            
            Application r = new Application();
            ConfigurationService cs = new EmbeddedConfigurationService();
            Properties p = EmbeddedTestUtil.getProperties(UnitTestUtil.getTestDataPath() + "/admin/buffertest1.properties"); //$NON-NLS-1$
            p.setProperty("mm.io.tmpdir", System.getProperty("mm.io.tmpdir")); //$NON-NLS-1$ //$NON-NLS-2$
            cs.initialize(p);
            r.installService(DQPServiceNames.CONFIGURATION_SERVICE, cs);
            EmbeddedBufferService svc = new EmbeddedBufferService();
            svc.initialize(null);
            r.installService(DQPServiceNames.BUFFER_SERVICE, svc);

            // These are defaults if none of the properties are set.
            assertTrue("64".equals(cs.getBufferMemorySize())); //$NON-NLS-1$
            assertTrue(cs.getDiskBufferDirectory().isDirectory() && cs.getDiskBufferDirectory().exists());
            assertTrue(cs.useDiskBuffering());
            
        } catch(ApplicationInitializationException e) {
            // expected
        } 
    }
    
    public void testCheckMemPropertyGotSet() throws Exception {
        EmbeddedBufferService svc = null;
        ConfigurationService cs = null;
        Application r = new Application();
        cs = new EmbeddedConfigurationService();
        Properties p = EmbeddedTestUtil.getProperties(UnitTestUtil.getTestDataPath() + "/admin/buffertest2.properties"); //$NON-NLS-1$
        cs.initialize(p);
        r.installService(DQPServiceNames.CONFIGURATION_SERVICE, cs);
        svc = new EmbeddedBufferService();
        svc.initialize(null);
        r.installService(DQPServiceNames.BUFFER_SERVICE, svc);
        
        // all the properties are set
        assertTrue("96".equals(cs.getBufferMemorySize()));     //$NON-NLS-1$
        cs.getDiskBufferDirectory();
        assertTrue("Not Directory", cs.getDiskBufferDirectory().isDirectory()); //$NON-NLS-1$
        assertTrue("does not exist", cs.getDiskBufferDirectory().exists()); //$NON-NLS-1$
        assertTrue("does not end with one", cs.getDiskBufferDirectory().getName().endsWith("1")); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(cs.useDiskBuffering());
        
        BufferManagerImpl mgr = (BufferManagerImpl) svc.getBufferManager();
        BufferConfig config = mgr.getConfig();
        assertEquals("Did not get expected memory level", 96000000L, config.getTotalAvailableMemory()); //$NON-NLS-1$
        assertTrue(config.getBufferStorageDirectory().endsWith(cs.getDiskBufferDirectory().getName()));
    }

    public void testCheckMemPropertyGotSet2() throws Exception {
        System.setProperty("mm.io.tmpdir", System.getProperty("java.io.tmpdir")+"/metamatrix/1");         //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        EmbeddedBufferService svc = null;
        Application r = new Application();
        ConfigurationService cs = new EmbeddedConfigurationService();
        Properties p = EmbeddedTestUtil.getProperties(UnitTestUtil.getTestDataPath() + "/admin/buffertest3.properties"); //$NON-NLS-1$
        p.setProperty("mm.io.tmpdir", System.getProperty("mm.io.tmpdir")); //$NON-NLS-1$ //$NON-NLS-2$
        cs.initialize(p);            
        r.installService(DQPServiceNames.CONFIGURATION_SERVICE, cs);
        svc = new EmbeddedBufferService();
        svc.initialize(null);
        r.installService(DQPServiceNames.BUFFER_SERVICE, svc);
        
        // all the properties are set
        assertFalse(cs.useDiskBuffering());
        
        BufferManagerImpl mgr = (BufferManagerImpl) svc.getBufferManager();
        BufferConfig config = mgr.getConfig();
        assertEquals("Did not get expected memory level", 64000000L, config.getTotalAvailableMemory()); //$NON-NLS-1$
    }
    
}
