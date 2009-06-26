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

package com.metamatrix.dqp.config;

import java.util.HashMap;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.dqp.internal.process.DQPCore;

import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.dqp.service.AutoGenDataService;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.FakeAbstractService;
import com.metamatrix.dqp.service.FakeBufferService;
import com.metamatrix.dqp.service.FakeMetadataService;
import com.metamatrix.dqp.service.MetadataService;

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
    
    public void testLaunch() throws Exception {
        DQPConfigSource configSource = Mockito.mock(DQPConfigSource.class);
        Mockito.stub(configSource.getProperties()).toReturn(new Properties());
        
        String[] services = new String[] {DQPServiceNames.BUFFER_SERVICE, DQPServiceNames.METADATA_SERVICE, DQPServiceNames.DATA_SERVICE};
        
        Mockito.stub(configSource.getServiceInstance(BufferService.class)).toReturn(new FakeBufferService());
        Mockito.stub(configSource.getServiceInstance(MetadataService.class)).toReturn(new FakeMetadataService());
        Mockito.stub(configSource.getServiceInstance(DataService.class)).toReturn(new AutoGenDataService());
        
        DQPCore dqpCore = new DQPCore();
        dqpCore.start(configSource);
    	
        assertNotNull("DQP should not be null", dqpCore); //$NON-NLS-1$
        
        // Check that bootstrapping occurred
        for(int i=0; i<services.length; i++) {
            FakeAbstractService svc = (FakeAbstractService)dqpCore.getEnvironment().findService(services[i]);
            assertEquals("service " + svc.getClass().getName() + " not init'ed correct # of times ", 1, svc.getInitializeCount()); //$NON-NLS-1$ //$NON-NLS-2$
            assertEquals("service " + svc.getClass().getName() + " not start'ed correct # of times ", 1, svc.getStartCount()); //$NON-NLS-1$ //$NON-NLS-2$
            assertEquals("service " + svc.getClass().getName() + " not stop'ed correct # of times ", 0, svc.getStopCount()); //$NON-NLS-1$ //$NON-NLS-2$
        }

    }
    
}
