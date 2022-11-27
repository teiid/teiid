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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.List;

import org.teiid.cache.DefaultCacheFactory;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TempTableTestHarness {

    protected TempMetadataAdapter metadata;
    protected TempTableDataManager dataManager;
    protected TempTableStore tempStore;

    protected TransactionContext tc;

    public ProcessorPlan execute(String sql, List<?>[] expectedResults, CapabilitiesFinder finder) throws Exception {
        CommandContext cc = TestProcessor.createCommandContext();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestProcessor.helpParse(sql), metadata, finder, cc);
        cc.setTransactionContext(tc);
        cc.setMetadata(metadata);
        cc.setTempTableStore(tempStore);
        TestProcessor.doProcess(plan, dataManager, expectedResults, cc);
        assertTrue(Determinism.SESSION_DETERMINISTIC.compareTo(cc.getDeterminismLevel()) <= 0);
        return plan;
    }

    public ProcessorPlan execute(String sql, List<?>[] expectedResults) throws Exception {
        return execute(sql, expectedResults, DefaultCapabilitiesFinder.INSTANCE);
    }

    public void setUp(QueryMetadataInterface qmi, ProcessorDataManager dm) {
        setUp(qmi, dm, BufferManagerFactory.getStandaloneBufferManager());
    }

    public void setUp(QueryMetadataInterface qmi, ProcessorDataManager dm, BufferManager bm) {
        tempStore = new TempTableStore("1", TransactionMode.ISOLATE_WRITES); //$NON-NLS-1$
        metadata = new TempMetadataAdapter(qmi, tempStore.getMetadataStore());
        metadata.setSession(true);

        SessionAwareCache<CachedResults> cache = new SessionAwareCache<CachedResults>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
        cache.setTupleBufferCache(bm);
        dataManager = new TempTableDataManager(dm, bm, cache);
    }

    public TempMetadataAdapter getMetadata() {
        return metadata;
    }

}
