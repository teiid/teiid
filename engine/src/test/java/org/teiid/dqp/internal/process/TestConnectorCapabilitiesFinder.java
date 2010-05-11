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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;


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
        String modelName = "model"; //$NON-NLS-1$
        String functionName = "fakeFunction"; //$NON-NLS-1$
        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setFunctionSupport("fakeFunction", true); //$NON-NLS-1$
       
        ArrayList<String> bindings = new ArrayList<String>();
        bindings.add(modelName);
        
        VDBMetaData vdb = Mockito.mock(VDBMetaData.class); 
        ModelMetaData model = Mockito.mock(ModelMetaData.class); 
        Mockito.stub(vdb.getModel(modelName)).toReturn(model);
        Mockito.stub(model.getSourceNames()).toReturn(bindings);
        
        BasicSourceCapabilities basicSourceCapabilities = new BasicSourceCapabilities();
        basicSourceCapabilities.setFunctionSupport(functionName, true);

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        ConnectorManager cm = Mockito.mock(ConnectorManager.class);
        Mockito.stub(cm.getCapabilities()).toReturn(basicSourceCapabilities);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(cm);
        
        CachedFinder finder = new CachedFinder(repo, vdb);
        
        // Test
        SourceCapabilities actual = finder.findCapabilities(modelName);
        assertEquals("Did not get expected capabilities", true, actual.supportsFunction(functionName)); //$NON-NLS-1$
    }

}
