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

package com.metamatrix.dqp.embedded.services;

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.EmbeddedTestUtil;
import com.metamatrix.dqp.service.DQPServiceNames;


/** 
 * @since 4.3
 */
public class TestEmbeddedDataService extends TestCase {
    EmbeddedConfigurationService configService =  null;
    EmbeddedDataService dataService = null;
    
    protected void setUp() throws Exception {
    	ApplicationEnvironment registry = new ApplicationEnvironment();
        configService = new EmbeddedConfigurationService();
        registry.bindService(DQPServiceNames.CONFIGURATION_SERVICE, configService);
        dataService = new EmbeddedDataService();
        registry.bindService(DQPServiceNames.DATA_SERVICE, dataService);
        configService.start(registry);
    }

    protected void tearDown() throws Exception {
        configService.stop();
    }

    public void testSelectConnector() throws Exception {
        Properties p = EmbeddedTestUtil.getProperties(UnitTestUtil.getTestDataPath()+"/dqp/dqp.properties"); //$NON-NLS-1$
        p.setProperty(DQPEmbeddedProperties.DQP_WORKDIR, System.getProperty("java.io.tmpdir")+"/teiid/1");         //$NON-NLS-1$ //$NON-NLS-2$
        p.setProperty(DQPEmbeddedProperties.DQP_DEPLOYDIR, System.getProperty("java.io.tmpdir")+"/teiid/deploy");         //$NON-NLS-1$ //$NON-NLS-2$
        configService.setUserPreferences(p);
        configService.initializeService(p);
        
    }
}
