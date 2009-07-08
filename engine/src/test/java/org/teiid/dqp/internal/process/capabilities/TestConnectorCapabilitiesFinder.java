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

package org.teiid.dqp.internal.process.capabilities;

import java.util.Arrays;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;

/**
 */
public class TestConnectorCapabilitiesFinder extends TestCase {

    /**
     * Constructor for TestConnectorCapabilitiesFinder.
     * @param name
     */
    public TestConnectorCapabilitiesFinder(String name) {
        super(name);
    }

    public void testFind() throws Exception {
        String vdbName = "myvdb"; //$NON-NLS-1$
        String vdbVersion = "1"; //$NON-NLS-1$
        String modelName = "model"; //$NON-NLS-1$
        String functionName = "fakeFunction"; //$NON-NLS-1$
        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setFunctionSupport("fakeFunction", true); //$NON-NLS-1$
        RequestMessage request = new RequestMessage(null);
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName(vdbName);
        workContext.setVdbVersion(vdbVersion);
        
        VDBService vdbService = Mockito.mock(VDBService.class); 
        Mockito.stub(vdbService.getConnectorBindingNames(vdbName, vdbVersion, modelName)).toReturn(Arrays.asList(modelName));
        DataService dataService = Mockito.mock(DataService.class);
        ConnectorID id = new ConnectorID("foo"); //$NON-NLS-1$
        Mockito.stub(dataService.selectConnector(modelName)).toReturn(id);
        BasicSourceCapabilities basicSourceCapabilities = new BasicSourceCapabilities();
        basicSourceCapabilities.setFunctionSupport(functionName, true);
        Mockito.stub(dataService.getCapabilities(request, workContext, id)).toReturn(basicSourceCapabilities);
        
        ConnectorCapabilitiesFinder finder = new ConnectorCapabilitiesFinder(vdbService, dataService, request, workContext);
        
        // Test
        SourceCapabilities actual = finder.findCapabilities(modelName);
        assertEquals("Did not get expected capabilities", true, actual.supportsFunction(functionName)); //$NON-NLS-1$
    }

}
