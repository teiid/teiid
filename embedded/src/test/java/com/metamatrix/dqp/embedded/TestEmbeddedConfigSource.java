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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.JMXUtil;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.FakeAbstractService;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.jdbc.EmbeddedGuiceModule;

public class TestEmbeddedConfigSource extends TestCase {
    
    public TestEmbeddedConfigSource(String name) {
        super(name);
    }

    URL buildDQPUrl(String configFileName) throws MalformedURLException {
        return URLHelper.buildURL(configFileName);
    }    

    public void testServiceLoading() throws Exception {
    	Properties p = new Properties();
    	URL url = buildDQPUrl(UnitTestUtil.getTestDataPath() + "/bqt/fakebqt.properties"); //$NON-NLS-1$
    	p.load(url.openStream());
    	
    	EmbeddedGuiceModule source = new EmbeddedGuiceModule(url, p, new JMXUtil("test")); //$NON-NLS-1$       
		Injector injector = Guice.createInjector(source);
		source.setInjector(injector);

		
        assertTrue(source.getServiceInstance(VDBService.class) instanceof FakeVDBService);
        assertTrue(source.getServiceInstance(ConfigurationService.class) instanceof FakeAbstractService);
    }
    
}
