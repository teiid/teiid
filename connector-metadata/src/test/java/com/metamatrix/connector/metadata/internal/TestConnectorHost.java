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

package com.metamatrix.connector.metadata.internal;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;

import junit.framework.TestCase;

import com.metamatrix.cdk.IConnectorHost;
import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.connector.metadata.IndexConnector;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.metadata.runtime.FakeMetadataService;
import com.metamatrix.metadata.runtime.FakeQueryMetadata;

public class TestConnectorHost extends TestCase {
    private FakeMetadataService fakeApplicationService;
    public static final URL TEST_FILE = TestConnectorHost.class.getResource("/PartsSupplier.vdb"); //$NON-NLS-1$

    public TestConnectorHost(String name) {
        super(name);
    }

    public void testWithIndexConnector() throws ConnectorException, IOException {
        IConnectorHost host = getConnectorHost(null);
        host.setSecurityContext("testName", "testVersion", null, null, null); //$NON-NLS-1$ //$NON-NLS-2$
        List results = host.executeCommand("select FullName from tables"); //$NON-NLS-1$

        List row = (List) results.get(0);
        assertEquals("PartsSupplier.PARTSSUPPLIER.PARTS", row.get(0).toString()); //$NON-NLS-1$
    }

    private IConnectorHost getConnectorHost(Properties properties) throws IOException {
        clearApplicationService();
        fakeApplicationService =  new FakeMetadataService(TEST_FILE);
        ConnectorHost host = new ConnectorHost(new IndexConnector(), properties, new TranslationUtility(FakeQueryMetadata.getQueryMetadata()));
        host.addResourceToConnectorEnvironment(DQPServiceNames.METADATA_SERVICE, fakeApplicationService);
        return host;
    }

    public void testWithIndexConnectorAndVdb() throws ConnectorException, IOException {
        ConnectorHost host = new ConnectorHost(new IndexConnector(), null, new TranslationUtility(TEST_FILE), false);
        host.setSecurityContext("testName", "testVersion", null, null, null); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSetProperties() throws ConnectorException, IOException {
        Properties properties = new Properties();
        properties.put("prop1", "value1"); //$NON-NLS-1$ //$NON-NLS-2$
        IConnectorHost host = getConnectorHost(properties);

        assertEquals("value1", (String)host.getConnectorEnvironmentProperties().get("prop1")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCannotChangePropertiesPassedIn() throws ConnectorException, IOException {
        Properties properties = new Properties();
        IConnectorHost host = getConnectorHost(properties);

        //Cannot directly manipulate properties after they are set.
        properties.put("prop2", "value2"); //$NON-NLS-1$ //$NON-NLS-2$
        assertNull(host.getConnectorEnvironmentProperties().get("prop2") ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCannotChangeProperties() throws ConnectorException, IOException {
        Properties properties = new Properties();
        IConnectorHost host = getConnectorHost(properties);

        //Cannot manipulate properties returned by connector host.
        host.getConnectorEnvironmentProperties().put("prop3", "value3"); //$NON-NLS-1$ //$NON-NLS-2$
        assertNull(host.getConnectorEnvironmentProperties().get("prop3") ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /*
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
        clearApplicationService();
    }

    private void clearApplicationService() {
        if (fakeApplicationService != null) {
            fakeApplicationService.clear();
            fakeApplicationService = null;
        }
    }
}
