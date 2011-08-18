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

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;


/**
 */
@SuppressWarnings("nls")
public class TestConnectorCapabilitiesFinder {

    @Test public void testFind() throws Exception {
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

    
    @Test public void testPushdownFunctionSupport() throws Exception {
    	ExecutionFactory<Object, Object> ef  = new ExecutionFactory<Object, Object>(){
    		
    		@Override
    		public void start() throws TranslatorException {
    			super.start();
    			addPushDownFunction("ns", "func", DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING);
    		}
    	};
    	ef.start();
    	BasicSourceCapabilities bsc = CapabilitiesConverter.convertCapabilities(ef, "conn"); //$NON-NLS-1$
        assertTrue("Did not get expected capabilities", bsc.supportsFunction("ns.func")); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
