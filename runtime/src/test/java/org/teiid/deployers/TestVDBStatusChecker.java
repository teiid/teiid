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

package org.teiid.deployers;

import static org.junit.Assert.*;

import java.util.concurrent.Executor;

import org.junit.Test;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionFactory;

@SuppressWarnings("nls")
public class TestVDBStatusChecker {

    @Test public void testDataSourceReplaced() throws Exception {
        final VDBRepository repo = new VDBRepository();
        repo.setSystemFunctionManager(RealMetadataFactory.SFM);
        repo.start();

        VDBStatusChecker vsc = new VDBStatusChecker() {

            @Override
            public VDBRepository getVDBRepository() {
                return repo;
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        };
        VDBTranslatorMetaData factory = new VDBTranslatorMetaData();
        factory.setExecutionFactoryClass(ExecutionFactory.class);

        assertFalse(vsc.dataSourceReplaced("x", "1", "y", "z", "t", "dsName"));

        MetadataStore metadataStore = RealMetadataFactory.exampleBQTCached().getMetadataStore();
        VDBMetaData vdb = TestCompositeVDB.createVDBMetadata(metadataStore, "bqt");

        ConnectorManagerRepository cmr = new ConnectorManagerRepository();
        cmr.setProvider(new ExecutionFactoryProvider() {

            @Override
            public ExecutionFactory<Object, Object> getExecutionFactory(String name)
                    throws ConnectorManagerException {
                return new ExecutionFactory<Object, Object>();
            }
        });
        ExecutionFactory ef1 = new ExecutionFactory();
        ConnectorManager mgr = new ConnectorManager("oracle", "dsName", ef1);
        cmr.addConnectorManager("BQT1", mgr);
        repo.addVDB(vdb, metadataStore, null, null, cmr);

        assertTrue(vsc.dataSourceReplaced("bqt", "1", "BQT1", "BQT1", "oracle", "dsName1"));
        ExecutionFactory ef = cmr.getConnectorManager("BQT1").getExecutionFactory();
        assertSame(ef, ef1);
        assertFalse(vsc.dataSourceReplaced("bqt", "1", "BQT1", "BQT1", "sqlserver", "dsName1"));
        ExecutionFactory ef2 = cmr.getConnectorManager("BQT1").getExecutionFactory();
        assertNotNull(ef2);
        assertNotSame(ef, ef2);
        assertTrue(vsc.dataSourceReplaced("bqt", "1", "BQT1", "BQT1", "oracle", "dsName2"));
        ef = cmr.getConnectorManager("BQT1").getExecutionFactory();
        assertNotNull(ef);
        assertNotSame(ef, ef2);
    }

}
