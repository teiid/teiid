/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008-2009 Red Hat, Inc.
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

package com.metamatrix.dqp.embedded;

import java.net.MalformedURLException;
import java.net.URL;

import junit.framework.TestCase;

import com.metamatrix.common.application.Application;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.FakeAbstractService;
import com.metamatrix.dqp.service.FakeVDBService;

public class TestEmbeddedConfigSource extends TestCase {
    
    public TestEmbeddedConfigSource(String name) {
        super(name);
    }

	@Override
	protected void setUp() throws Exception {
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/configuration.xml", UnitTestUtil.getTestScratchPath()+"/configuration.xml"); //$NON-NLS-1$ //$NON-NLS-2$	
	}

    
    URL buildDQPUrl(String configFileName) throws MalformedURLException {
        return URLHelper.buildURL(configFileName);
    }    

    public void testServiceLoading() throws Exception {
        EmbeddedConfigSource source = new EmbeddedConfigSource(buildDQPUrl(UnitTestUtil.getTestDataPath() + "/bqt/fakebqt.properties"), null);//$NON-NLS-1$        
        Application application = new Application();
        application.start(source);
        assertTrue(application.getEnvironment().findService(DQPServiceNames.VDB_SERVICE) instanceof FakeVDBService);
        assertTrue(application.getEnvironment().findService(DQPServiceNames.CONFIGURATION_SERVICE) instanceof FakeAbstractService);
    }
    
}
