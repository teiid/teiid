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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.metadata.Table;
import org.teiid.query.processor.FakeProcessorPlan;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"nls"})
public class TestCachedResults {

    @Test
    public void testCaching() throws Exception {
        FakeBufferService fbs = new FakeBufferService(true);

        ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
        x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List<ElementSymbol> schema = Arrays.asList(x);
        TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tb.setForwardOnly(false);

        tb.addTuple(Arrays.asList(1));
        tb.addTuple(Arrays.asList(2));
        tb.addTuple(Arrays.asList(3));
        tb.addTuple(Arrays.asList(4));
        tb.addTuple(Arrays.asList(5));
        tb.addTuple(Arrays.asList(6));
        tb.addTuple(Arrays.asList(7));
        tb.addTuple(Arrays.asList(8));
        tb.addTuple(Arrays.asList(9));
        tb.addTuple(Arrays.asList(10));

        tb.close();

        BufferManager bm = fbs.getBufferManager();
        CachedResults results = new CachedResults();
        ProcessorPlan plan = new FakeProcessorPlan(0);
        CommandContext cc = new CommandContext();
        Table t = RealMetadataFactory.exampleBQT().getGroupID("bqt1.smalla");
        cc.accessedDataObject(t);
        plan.setContext(cc);
        results.setResults(tb, plan);
        results.setCommand(new Query());
        //Cache cache = new DefaultCache("dummy"); //$NON-NLS-1$
        long ts = results.getAccessInfo().getCreationTime();
        // simulate the jboss-cache remote transport, where the batches are remotely looked up
        // in cache
        for (int row=1; row<=tb.getRowCount();row+=4) {
            //cache.put(results.getId()+","+row, tb.getBatch(row), null); //$NON-NLS-1$
        }

        results.prepare(bm);

        //simulate distribute
        TupleBuffer distributedTb = bm.getTupleBuffer(results.getId());

        CachedResults cachedResults = UnitTestUtil.helpSerialize(results);

        RealMetadataFactory.buildWorkContext(RealMetadataFactory.exampleBQT());

        BufferManager bm2 = fbs.getBufferManager();
        bm2.distributeTupleBuffer(results.getId(), distributedTb);

        assertTrue(cachedResults.restore(bm2));

        // since restored, simulate a async cache flush
        //cache.clear();

        TupleBuffer cachedTb = cachedResults.getResults();

        assertTrue(cachedTb.isFinal());
        assertEquals(tb.getRowCount(), cachedTb.getRowCount());
        assertEquals(tb.getBatchSize(), cachedTb.getBatchSize());

        assertArrayEquals(tb.getBatch(1).getAllTuples(), cachedTb.getBatch(1).getAllTuples());
        assertArrayEquals(tb.getBatch(9).getAllTuples(), cachedTb.getBatch(9).getAllTuples());
        assertTrue(ts - cachedResults.getAccessInfo().getCreationTime() <= 5000);

        //ensure that an incomplete load fails ( is this still valid use case?)
//        bm2.getTupleBuffer(results.getId()).remove();
//        cachedResults = UnitTestUtil.helpSerialize(results);
//        assertFalse(cachedResults.restore(cache, bm2));
    }
}
