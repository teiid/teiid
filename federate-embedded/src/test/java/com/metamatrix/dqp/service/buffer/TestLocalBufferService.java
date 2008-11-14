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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.buffer.impl.BufferConfig;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.services.EmbeddedBufferService;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;
import com.metamatrix.dqp.embedded.services.EmbeddedDQPServiceRegistry;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DQPServiceRegistry;

public class TestLocalBufferService extends TestCase {

    public TestLocalBufferService(String name) {
        super(name);
    }

    private Properties helpLoadProperties(String file) throws IOException {
        Properties props = new Properties();        
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        try {
            File f = new File(file);
            fis = new FileInputStream(f);
            bis = new BufferedInputStream(fis); 
            props.load(bis);
            props.put(DQPEmbeddedProperties.DQP_BOOTSTRAP_PROPERTIES_FILE, f.toURL());
        } finally {
            if(bis != null) {   
                try {             
                    bis.close();
                } catch(IOException e) {
                    // ignore - rather have original exception if there is one
                }
            }
        }
        
        return props;       
    }
    
    public void testMissingRequiredProperties() throws Exception {        
        try {
            System.setProperty("mm.io.tmpdir", System.getProperty("java.io.tmpdir")+"/metamatrix/1");
            
            DQPServiceRegistry r = new EmbeddedDQPServiceRegistry();
            ConfigurationService cs = new EmbeddedConfigurationService(r);
            r.registerService(DQPServiceNames.CONFIGURATION_SERVICE, cs);
            Properties p = helpLoadProperties(UnitTestUtil.getTestDataPath() + "/admin/buffertest1.properties");
            p.setProperty("mm.io.tmpdir", System.getProperty("mm.io.tmpdir"));
            cs.initialize(p);            
            EmbeddedBufferService svc = new EmbeddedBufferService(r);
            svc.initialize(null); //$NON-NLS-1$

            // These are defaults if none of the properties are set.
            assertTrue("64".equals(cs.getBufferMemorySize()));
            assertTrue(cs.getDiskBufferDirectory().isDirectory() && cs.getDiskBufferDirectory().exists());
            assertTrue(cs.useDiskBuffering());
            
        } catch(ApplicationInitializationException e) {
            // expected
        } 
    }
    
    public void testCheckMemPropertyGotSet() throws Exception {
        EmbeddedBufferService svc = null;
        ConfigurationService cs = null;
        DQPServiceRegistry r = new EmbeddedDQPServiceRegistry();
        cs = new EmbeddedConfigurationService(r);
        Properties p = helpLoadProperties(UnitTestUtil.getTestDataPath() + "/admin/buffertest2.properties");
        cs.initialize(p);
        r.registerService(DQPServiceNames.CONFIGURATION_SERVICE, cs);
        svc = new EmbeddedBufferService(r);
        svc.initialize(null); //$NON-NLS-1$
        
        // all the properties are set
        assertTrue("96".equals(cs.getBufferMemorySize()));    
        cs.getDiskBufferDirectory();
        assertTrue("Not Directory", cs.getDiskBufferDirectory().isDirectory());
        assertTrue("does not exist", cs.getDiskBufferDirectory().exists());
        assertTrue("does not end with one", cs.getDiskBufferDirectory().getName().endsWith("1"));
        assertTrue(cs.useDiskBuffering());
        
        BufferManagerImpl mgr = (BufferManagerImpl) svc.getBufferManager();
        BufferConfig config = mgr.getConfig();
        assertEquals("Did not get expected memory level", 96000000L, config.getTotalAvailableMemory()); //$NON-NLS-1$
        assertTrue(config.getBufferStorageDirectory().endsWith(cs.getDiskBufferDirectory().getName()));
    }

    public void testCheckMemPropertyGotSet2() throws Exception {
        System.setProperty("mm.io.tmpdir", System.getProperty("java.io.tmpdir")+"/metamatrix/1");        
        EmbeddedBufferService svc = null;
        DQPServiceRegistry r = new EmbeddedDQPServiceRegistry();
        ConfigurationService cs = new EmbeddedConfigurationService(r);
        Properties p = helpLoadProperties(UnitTestUtil.getTestDataPath() + "/admin/buffertest3.properties");
        p.setProperty("mm.io.tmpdir", System.getProperty("mm.io.tmpdir"));
        cs.initialize(p);            
        r.registerService(DQPServiceNames.CONFIGURATION_SERVICE, cs);
        svc = new EmbeddedBufferService(r);
        svc.initialize(null); //$NON-NLS-1$
        
        // all the properties are set
        assertFalse(cs.useDiskBuffering());
        
        BufferManagerImpl mgr = (BufferManagerImpl) svc.getBufferManager();
        BufferConfig config = mgr.getConfig();
        assertEquals("Did not get expected memory level", 64000000L, config.getTotalAvailableMemory()); //$NON-NLS-1$
    }
    
}
