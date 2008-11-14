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

package com.metamatrix.dqp.config;

import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.core.log.LogListener;
import com.metamatrix.dqp.internal.process.DQPCore;
import com.metamatrix.dqp.service.AutoGenDataService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.FakeAbstractService;
import com.metamatrix.dqp.service.FakeBufferService;
import com.metamatrix.dqp.service.FakeMetadataService;
import com.metamatrix.internal.core.log.PlatformLog;

/**
 */
public class TestDQPLauncher extends TestCase {

    /**
     * Constructor for TestDQPLauncher.
     * @param name
     */
    public TestDQPLauncher(String name) {
        super(name);
    }
    
    public void setup() {
    }

    public void testLaunch() throws Exception {
        FakeConfigSource configSource = new FakeConfigSource();
        FakeAbstractService[] svcs = new FakeAbstractService[3];
        svcs[0] = new FakeBufferService();
        svcs[0].initialize(new Properties());
        svcs[1] = new FakeMetadataService();
        svcs[1].initialize(new Properties());
        svcs[2] = new AutoGenDataService();
        svcs[2].initialize(new Properties());
        configSource.addService(DQPServiceNames.BUFFER_SERVICE, svcs[0]);
        configSource.addService(DQPServiceNames.METADATA_SERVICE, svcs[1]);
        configSource.addService(DQPServiceNames.DATA_SERVICE, svcs[2]);
        
        DQPLauncher launcher = new DQPLauncher(configSource);
    	
        PlatformLog log = PlatformLog.getInstance();
    	List<LogListener> list = log.getLogListeners();
    	for(LogListener l: list) {
    		log.removeListener(l);
    	}
                
        DQPCore dqp = launcher.createDqp();
        assertNotNull("DQP should not be null", dqp); //$NON-NLS-1$
        
        // Check that bootstrapping occurred
        for(int i=0; i<svcs.length; i++) {
            FakeAbstractService svc = svcs[i];
            assertEquals("service " + svc.getClass().getName() + " not init'ed correct # of times ", 1, svc.getInitializeCount()); //$NON-NLS-1$ //$NON-NLS-2$
            assertEquals("service " + svc.getClass().getName() + " not bind'ed correct # of times ", 1, svc.getBindCount()); //$NON-NLS-1$ //$NON-NLS-2$
            assertEquals("service " + svc.getClass().getName() + " not start'ed correct # of times ", 1, svc.getStartCount()); //$NON-NLS-1$ //$NON-NLS-2$
            assertEquals("service " + svc.getClass().getName() + " not unbind'ed correct # of times ", 0, svc.getUnbindCount()); //$NON-NLS-1$ //$NON-NLS-2$
            assertEquals("service " + svc.getClass().getName() + " not stop'ed correct # of times ", 0, svc.getStopCount()); //$NON-NLS-1$ //$NON-NLS-2$
        }

    }
    
}
