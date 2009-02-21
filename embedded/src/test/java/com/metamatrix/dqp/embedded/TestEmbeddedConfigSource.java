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

package com.metamatrix.dqp.embedded;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.application.Application;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.FakeAbstractService;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.jdbc.EmbeddedDataSource;

public class TestEmbeddedConfigSource extends TestCase {
    
    public TestEmbeddedConfigSource(String name) {
        super(name);
    }

    URL buildDQPUrl(String configFileName) throws MalformedURLException {
        return URLHelper.buildURL(configFileName);
    }    

    public void testServiceLoading() throws Exception {
    	Properties p = new Properties();
    	p.put(EmbeddedDataSource.DQP_BOOTSTRAP_FILE, buildDQPUrl(UnitTestUtil.getTestDataPath() + "/bqt/fakebqt.properties")); //$NON-NLS-1$
    	
        EmbeddedConfigSource source = new EmbeddedConfigSource(p);        
        Application application = new Application();
        application.start(source);
        assertTrue(application.getEnvironment().findService(DQPServiceNames.VDB_SERVICE) instanceof FakeVDBService);
        assertTrue(application.getEnvironment().findService(DQPServiceNames.CONFIGURATION_SERVICE) instanceof FakeAbstractService);
    }
    
}
