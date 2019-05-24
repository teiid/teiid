/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;


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
        assertTrue(finder.isValid(modelName));
    }

    @Test public void testFindRequiresSource() throws Exception {
        String modelName = "model"; //$NON-NLS-1$
        String functionName = "fakeFunction"; //$NON-NLS-1$

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
        Mockito.stub(cm.getCapabilities()).toThrow(new TranslatorException());
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(cm);

        CachedFinder finder = new CachedFinder(repo, vdb);

        // Test
        SourceCapabilities actual = finder.findCapabilities(modelName);
        assertNotNull(actual); //$NON-NLS-1$
        assertFalse(finder.isValid(modelName));
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

    @Test public void testConverts() throws Exception {
        ExecutionFactory<Object, Object> ef  = new ExecutionFactory<Object, Object>(){
            @Override
            public boolean supportsConvert(int fromType, int toType) {
                return false;
            }
            @Override
            public List<String> getSupportedFunctions() {
                return Arrays.asList("convert");
            }
        };
        ef.start();
        BasicSourceCapabilities bsc = CapabilitiesConverter.convertCapabilities(ef, "conn"); //$NON-NLS-1$
        assertTrue(bsc.supportsFunction("convert")); //$NON-NLS-1$
        assertFalse(bsc.supportsConvert(TypeFacility.RUNTIME_CODES.BIG_DECIMAL, TypeFacility.RUNTIME_CODES.BIG_INTEGER));
    }

    @Test public void testCTESupport() throws Exception {
        final AtomicBoolean bool = new AtomicBoolean(false);
        ExecutionFactory<Object, Object> ef  = new ExecutionFactory<Object, Object>(){
            @Override
            public boolean supportsCommonTableExpressions() {
                return bool.get();
            }
            @Override
            public boolean supportsRecursiveCommonTableExpressions() {
                return true;
            }
        };
        ef.start();
        BasicSourceCapabilities bsc = CapabilitiesConverter.convertCapabilities(ef, "conn"); //$NON-NLS-1$
        assertFalse(bsc.supportsCapability(Capability.RECURSIVE_COMMON_TABLE_EXPRESSIONS));

        bool.set(true);
        bsc = CapabilitiesConverter.convertCapabilities(ef, "conn"); //$NON-NLS-1$
        assertTrue(bsc.supportsCapability(Capability.RECURSIVE_COMMON_TABLE_EXPRESSIONS));
    }
}
