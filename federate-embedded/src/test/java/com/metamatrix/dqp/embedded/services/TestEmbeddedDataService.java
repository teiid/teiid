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

package com.metamatrix.dqp.embedded.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.service.DQPServiceNames;


/** 
 * @since 4.3
 */
public class TestEmbeddedDataService extends TestCase {
    EmbeddedConfigurationService configService =  null;
    EmbeddedDataService dataService = null;
    
    protected void setUp() throws Exception {
    	System.setProperty(CoreConstants.NO_CONFIGURATION, "");//$NON-NLS-1$
        File[] files = new File(UnitTestUtil.getTestDataPath()+"/dqp/data").listFiles(); //$NON-NLS-1$
        for (int i = 0; i < files.length; i++) {
            if (!files[i].isDirectory()) {
                copy (files[i], new File(UnitTestUtil.getTestDataPath()+"/dqp/config/"+files[i].getName())); //$NON-NLS-1$
            }
        }
        ApplicationEnvironment registry = new ApplicationEnvironment();
        configService = new EmbeddedConfigurationService();
        registry.bindService(DQPServiceNames.CONFIGURATION_SERVICE, configService);
        dataService = new EmbeddedDataService();
        registry.bindService(DQPServiceNames.DATA_SERVICE, dataService);
        configService.start(registry);
    }

    protected void tearDown() throws Exception {
        configService.stop();
        File f = new File(UnitTestUtil.getTestDataPath()+"/dqp/config"); //$NON-NLS-1$
        if (!f.exists()) {
            f.mkdirs();
        }
        
        File[] files = f.listFiles(); 
        for (int i = 0; i < files.length; i++) {
            files[i].delete();
        }        
    }

    void copy(File src, File target) {
        try {
            FileInputStream in = new FileInputStream(src);
            FileOutputStream out = new FileOutputStream(target);
            
            byte[] buf = new byte[1024];
            int d = in.read(buf, 0, 1024);
            while (d != -1) {
                out.write(buf, 0, d);
                d = in.read(buf, 0, 1024);            
            }            
            in.close();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            // skip..
        } 
    }
    
    Properties getProperties() throws Exception{
        Properties p = new Properties();
        File f = new File(UnitTestUtil.getTestDataPath()+"/dqp/dqp.properties"); //$NON-NLS-1$
        p.load(new FileInputStream(f)); 
        p.put("dqp.propertiesFile", URLHelper.buildURL(UnitTestUtil.getTestDataPath()+"/dqp/dqp.properties")); //$NON-NLS-1$ //$NON-NLS-2$
        return p;
    }
    
    public void testSelectConnector() throws Exception {
        Properties p = getProperties();        
        configService.userPreferences = p;
        configService.initializeService(p);
        
    }
}
