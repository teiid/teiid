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

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.RequestMessage.StatementType;
import org.teiid.client.ResultsMessage;
import org.teiid.client.lob.LobChunk;
import org.teiid.client.util.ResultsFuture;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobType;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.internal.process.AbstractWorkItem.ThreadState;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestDQPCore {

    private final class LobThread extends Thread {
        BlobType bt;
        private final RequestMessage reqMsg;
        volatile ResultsFuture<LobChunk> chunkFuture;
        protected DQPWorkContext workContext;

        private LobThread(RequestMessage reqMsg) {
            this.reqMsg = reqMsg;
        }

        @Override
        public void run() {
            synchronized (this) {
                while (workContext == null) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            workContext.runInContext(new Runnable() {

                @Override
                public void run() {
                    try {
                        chunkFuture = core.requestNextLobChunk(1, reqMsg.getExecutionId(), bt.getReferenceStreamId());
                    } catch (TeiidProcessingException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private DQPCore core;
    private DQPConfiguration config;
    private AutoGenDataService agds;

    @Before public void setUp() throws Exception {
        agds = new AutoGenDataService();
        DQPWorkContext context = RealMetadataFactory.buildWorkContext(RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.exampleBQTCached().getMetadataStore(), "bqt"));
        context.getVDB().getModel("BQT3").setVisible(false); //$NON-NLS-1$
        context.getVDB().getModel("VQT").setVisible(true); //$NON-NLS-1$
        HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
        policies.put("foo", new DataPolicyMetadata());
        context.setPolicies(policies);

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        context.getVDB().addAttachment(ConnectorManagerRepository.class, repo);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(agds);
        BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
        bm.setInlineLobs(false);
        FakeBufferService bs = new FakeBufferService(bm, bm);
        core = new DQPCore();
        core.setBufferManager(bs.getBufferManager());
        core.setResultsetCache(new SessionAwareCache<CachedResults>("resultset", new DefaultCacheFactory(new CacheConfiguration()), SessionAwareCache.Type.RESULTSET, 0));
        core.setPreparedPlanCache(new SessionAwareCache<PreparedPlan>("preparedplan", new DefaultCacheFactory(new CacheConfiguration()), SessionAwareCache.Type.PREPAREDPLAN, 0));
        core.setTransactionService(new FakeTransactionService());

        config = new DQPConfiguration();
        config.setMaxActivePlans(1);
        config.setUserRequestSourceConcurrency(2);
        DefaultAuthorizationValidator daa = new DefaultAuthorizationValidator();
        daa.setPolicyDecider(new DataRolePolicyDecider());
        config.setAuthorizationValidator(daa);
        core.start(config);
        core.getPrepPlanCache().setModTime(1);
        core.getRsCache().setTupleBufferCache(bs.getBufferManager());
    }

    @After public void tearDown() throws Exception {
        DQPWorkContext.setWorkContext(new DQPWorkContext());
        core.stop();
    }

    int id = 0;

    public RequestMessage exampleRequestMessage(String sql) {
        RequestMessage msg = new RequestMessage(sql);
        msg.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        msg.setFetchSize(10);
        msg.setPartialResults(false);
        msg.setExecutionId(100);
        msg.setExecutionId(id++);
        return msg;
    }

    @Test public void testConfigurationSets() {
        assertEquals(1, core.getMaxActivePlans());
        assertEquals(2, core.getUserRequestSourceConcurrency());
    }

    @Test public void testRequest1() throws Exception {
        helpExecute("SELECT IntKey FROM BQT1.SmallA", "a"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRequestMaxActive() throws Exception {
        agds.latch = new CountDownLatch(3);
        int toRun = 2;
        CountDownLatch submitted = new CountDownLatch(toRun);
        ExecutorService es = Executors.newCachedThreadPool();
        final DQPWorkContext context = DQPWorkContext.getWorkContext();
        final AtomicInteger counter = new AtomicInteger();
        es.invokeAll(Collections.nCopies(toRun, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                DQPWorkContext.setWorkContext(context);
                RequestMessage reqMsg = exampleRequestMessage("select * FROM BQT1.SmallA");
                DQPWorkContext.getWorkContext().getSession().setSessionId("1");
                DQPWorkContext.getWorkContext().getSession().setUserName("a");

                Future<ResultsMessage> message = null;
                try {
                    message = core.executeRequest(counter.getAndIncrement(), reqMsg);
                } finally {
                    submitted.countDown();
                }
                assertNotNull(core.getClientState("1", false));
                submitted.await(); //after this, both will be submitted
                agds.latch.countDown(); //allow the execution to proceed
                message.get(500000, TimeUnit.MILLISECONDS);
                return null;
            }
        }));
        assertEquals(1, this.core.getMaxWaitingPlanWatermark());
    }

    @Test public void testHasRole() throws Exception {
        String sql = "SELECT hasRole('foo')"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        ResultsMessage rm = helpExecute(sql, userName);
        assertTrue((Boolean)rm.getResultsList().get(0).get(0));
    }

    @Test public void testNotHasRole() throws Exception {
        String sql = "SELECT hasRole('bar')"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        ResultsMessage rm = helpExecute(sql, userName);
        assertFalse((Boolean)rm.getResultsList().get(0).get(0));
    }

    @Test public void testUser1() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = 'logon'"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser2() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() LIKE 'logon'"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser3() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() IN ('logon3') AND StringKey LIKE '1'"; //$NON-NLS-1$
        String userName = "logon3"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser4() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE 'logon4' = user() AND StringKey = '1'"; //$NON-NLS-1$
        String userName = "logon4"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser5() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() IS NULL "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser6() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = 'logon33' "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser7() throws Exception {
        String sql = "UPDATE BQT1.SmallA SET IntKey = 2 WHERE user() = 'logon' AND StringKey = '1' "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser8() throws Exception {
        String sql = "SELECT user(), StringKey FROM BQT1.SmallA WHERE IntKey = 1 "; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testUser9() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE user() = StringKey AND StringKey = '1' "; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    @Test public void testTxnAutoWrap() throws Exception {
        String sql = "SELECT * FROM BQT1.SmallA"; //$NON-NLS-1$
        helpExecute(sql, "a", 1, true); //$NON-NLS-1$
    }

    /**
     * Ensures that VQT visibility does not affect the view query
     */
    @Test public void testViewVisibility() throws Exception {
        String sql = "SELECT * FROM VQT.SmallA_2589g"; //$NON-NLS-1$
        helpExecute(sql, "a"); //$NON-NLS-1$
    }

    @Test public void testLimitCompensation() throws Exception {
        String sql = "SELECT * FROM VQT.SmallA_2589g LIMIT 1, 1"; //$NON-NLS-1$
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        agds.setCaps(caps);
        ResultsMessage rm = helpExecute(sql, "a"); //$NON-NLS-1$
        //we test for > 0 here because the autogen service doesn't obey the limit
        assertTrue(rm.getResultsList().size() > 0);
    }

    @Test public void testLimitCompensation1() throws Exception {
        String sql = "SELECT * FROM VQT.SmallA_2589g LIMIT 1, 1"; //$NON-NLS-1$
        ResultsMessage rm = helpExecute(sql, "a"); //$NON-NLS-1$
        assertEquals(1, rm.getResultsList().size());
    }


    /**
     * Tests whether an exception result is sent when an exception occurs
     * @since 4.3
     */
    @Test public void testPlanningException() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.BadIdea "; //$NON-NLS-1$

        RequestMessage reqMsg = exampleRequestMessage(sql);

        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        try {
            message.get(5000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof QueryResolverException);
        }
    }

    @Ignore("visibility no longer ristricts access")
    @Test public void testLookupVisibility() throws Exception {
        helpTestVisibilityFails("select lookup('bqt3.smalla', 'intkey', 'stringkey', '?')"); //$NON-NLS-1$
    }

    @Test public void testCancel() throws Exception {
        assertFalse(this.core.cancelRequest(1L));
    }

    @Test public void testBufferLimit() throws Exception {
        //the sql should return 400 rows
        String sql = "SELECT A.IntKey FROM BQT1.SmallA as A, BQT1.SmallA as B, (select intkey from BQT1.SmallA limit 4) as C"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        String sessionid = "1"; //$NON-NLS-1$

        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        DQPWorkContext.getWorkContext().getSession().setSessionId(sessionid);
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        ((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(1);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());

        int rowsPerBatch = 8;
        assertEquals(rowsPerBatch, rm.getResultsList().size());
        RequestWorkItem item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));

        message = core.processCursorRequest(reqMsg.getExecutionId(), 9, rowsPerBatch);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(rowsPerBatch, rm.getResultsList().size());
        //ensure that we are idle
        for (int i = 0; i < 10 && item.getThreadState() != ThreadState.IDLE; i++) {
            Thread.sleep(100);
        }
        assertEquals(ThreadState.IDLE, item.getThreadState());
        assertTrue(item.resultsBuffer.getManagedRowCount() <= rowsPerBatch*23);
        //pull the rest of the results
        int start = 17;
        while (true) {
            item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));

            message = core.processCursorRequest(reqMsg.getExecutionId(), start, rowsPerBatch);
            rm = message.get(5000, TimeUnit.MILLISECONDS);
            assertNull(rm.getException());
            assertTrue(rowsPerBatch >= rm.getResultsList().size());
            start += rm.getResultsList().size();
            if (rm.getFinalRow() == rm.getLastRow()) {
                break;
            }
        }

        //insensitive should not block
        reqMsg.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        reqMsg.setExecutionId(id++);

        message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());

        assertEquals(rowsPerBatch, rm.getResultsList().size());
        item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));

        message = core.processCursorRequest(reqMsg.getExecutionId(), 9, rowsPerBatch);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(rowsPerBatch, rm.getResultsList().size());
        //ensure that we are idle
        for (int i = 0; i < 50 && item.getThreadState() != ThreadState.IDLE; i++) {
            Thread.sleep(100);
        }
        assertEquals(ThreadState.IDLE, item.getThreadState());
        //should buffer the same as forward only until a further batch is requested
        assertTrue(item.resultsBuffer.getManagedRowCount() <= rowsPerBatch*23);

        //local should not buffer
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        reqMsg.setSync(true);

        message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        rm = message.get(0, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(rowsPerBatch, rm.getResultsList().size());

        item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));
        assertEquals(0, item.resultsBuffer.getManagedRowCount());
        assertEquals(8, item.resultsBuffer.getRowCount());
    }

    @Test public void testBufferReuse() throws Exception {
        //the sql should return 100 rows
        String sql = "SELECT A.IntKey FROM BQT1.SmallA as A, BQT1.SmallA as B ORDER BY A.IntKey"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        String sessionid = "1"; //$NON-NLS-1$

        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        DQPWorkContext.getWorkContext().getSession().setSessionId(sessionid);
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        ((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(1);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(8, rm.getResultsList().size());
        RequestWorkItem item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));
        assertEquals(100, item.resultsBuffer.getRowCount());
    }

    @Test public void testFinalRow() throws Exception {
        String sql = "SELECT A.IntKey FROM BQT1.SmallA as A"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        String sessionid = "1"; //$NON-NLS-1$

        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        DQPWorkContext.getWorkContext().getSession().setSessionId(sessionid);
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        ((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(10);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(10, rm.getResultsList().size());
        RequestWorkItem item = core.getRequestWorkItem(DQPWorkContext.getWorkContext().getRequestID(reqMsg.getExecutionId()));
        while(item.isProcessing());
        synchronized (item) {
            for (int i = 0; i < 100; i++) {
                Thread.sleep(10);
            }
        }
        assertEquals(10, item.resultsBuffer.getRowCount());
    }

    @Test public void testBufferReuse1() throws Exception {
        //the sql should return 100 rows
        String sql = "SELECT IntKey FROM texttable('1112131415' columns intkey integer width 2 no row delimiter) t " +
                "union " +
                "SELECT IntKey FROM bqt1.smalla order by intkey"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        String sessionid = "1"; //$NON-NLS-1$
        agds.sleep = 50;
        agds.setUseIntCounter(true);
        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setRowLimit(11);
        reqMsg.setCursorType(ResultSet.TYPE_FORWARD_ONLY);
        DQPWorkContext.getWorkContext().getSession().setSessionId(sessionid);
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);
        BufferManagerImpl bufferManager = (BufferManagerImpl)core.getBufferManager();
        bufferManager.setProcessorBatchSize(20);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(10, rm.getResultsList().size());

        message = core.processCursorRequest(reqMsg.getExecutionId(), 6, 5);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(5, rm.getResultsList().size());

        message = core.processCursorRequest(reqMsg.getExecutionId(), 11, 5);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(1, rm.getResultsList().size());
        assertEquals(11, rm.getFirstRow());
        assertEquals(11, rm.getFinalRow());
    }

    @Test public void testSourceConcurrency() throws Exception {
        //setup default of 2
        agds.setSleep(100);
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        agds.setCaps(bsc);
        StringBuffer sql = new StringBuffer();
        int branches = 20;
        for (int i = 0; i < branches; i++) {
            if (i > 0) {
                sql.append(" union all ");
            }
            sql.append("select stringkey || " + i + " from bqt1.smalla");
        }
        //sql.append(" limit 2");
        helpExecute(sql.toString(), "a", 1, false);
        //there's isn't a hard guarantee that only two requests will get started
        assertTrue(agds.getExecuteCount().get() <= 6);

        //20 concurrent
        core.setUserRequestSourceConcurrency(20);
        agds.getExecuteCount().set(0);
        helpExecute(sql.toString(), "a", 2, false);
        assertTrue(agds.getExecuteCount().get() > 10 && agds.getExecuteCount().get() <= 20);

        //serial
        core.setUserRequestSourceConcurrency(1);
        agds.getExecuteCount().set(0);
        helpExecute(sql.toString(), "a", 3, false);
        //there's two since 1 is smaller than the expected batch
        assertTrue(agds.getExecuteCount().get() <= 2);
    }

    @Test public void testSourceConcurrencyWithLimitedUnion() throws Exception {
        agds.setSleep(100);
        helpTestSourceConcurrencyWithLimitedUnion();
    }

    @Test public void testSourceConcurrencyWithLimitedUnionThreadBound() throws Exception {
        agds.setSleep(100);
        agds.threadBound = true;
        helpTestSourceConcurrencyWithLimitedUnion();
    }

    @Test public void testSerialThreadBoundClose() throws Exception {
        agds.setSleep(100);
        agds.threadBound = true;
        core.setUserRequestSourceConcurrency(1);
        String sql = "SELECT IntKey FROM BQT1.SmallA"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        ((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(1);

        RequestMessage reqMsg = exampleRequestMessage(sql);
        //execute by don't finish the work
        execute(userName, 1, reqMsg);
        //make sure the source request is still closed
        assertEquals(1, agds.getCloseCount().get());
    }

    @Test(expected=TeiidProcessingException.class) public void testThreadBoundException() throws Exception {
        agds.threadBound = true;
        agds.throwExceptionOnExecute = true;
        String sql = "SELECT IntKey FROM BQT1.SmallA"; //$NON-NLS-1$
        String userName = "logon"; //$NON-NLS-1$
        helpExecute(sql, userName);
    }

    private void helpTestSourceConcurrencyWithLimitedUnion() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        agds.setCaps(bsc);
        StringBuffer sql = new StringBuffer();
        int branches = 20;
        for (int i = 0; i < branches; i++) {
            if (i > 0) {
                sql.append(" union all ");
            }
            sql.append("select stringkey || " + i + " from bqt1.smalla");
        }
        sql.append(" limit 11");

        helpExecute(sql.toString(), "a", 1, false);
        assertTrue(String.valueOf(agds.getExecuteCount()), agds.getExecuteCount().get() <= 2);

        //20 concurrent max, but we'll use 6
        core.setUserRequestSourceConcurrency(20);
        agds.getExecuteCount().set(0);
        helpExecute(sql.toString(), "a", 2, false);
        assertTrue(String.valueOf(agds.getExecuteCount()), agds.getExecuteCount().get() <= 6);

        //serial
        core.setUserRequestSourceConcurrency(1);
        agds.getExecuteCount().set(0);
        helpExecute(sql.toString(), "a", 3, false);
        assertTrue(agds.getExecuteCount().get() <= 2);

        //ensure that we'll still consult all sources even if the limit is not met
        core.setUserRequestSourceConcurrency(4);
        agds.getExecuteCount().set(0);
        agds.setRows(0);
        helpExecute(sql.toString(), "a", 4, false);
        assertEquals(20, agds.getExecuteCount().get());
    }

    @Test public void testUsingFinalBuffer() throws Exception {
        String sql = "select intkey from bqt1.smalla order by intkey";
        ((BufferManagerImpl)core.getBufferManager()).setProcessorBatchSize(2);
        agds.sleep = 50;
        RequestMessage reqMsg = exampleRequestMessage(sql);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(10, rm.getResultsList().size());

        message = core.processCursorRequest(reqMsg.getExecutionId(), 3, 2);
        rm = message.get(500000, TimeUnit.MILLISECONDS);
        assertNull(rm.getException());
        assertEquals(2, rm.getResultsList().size());
    }

    @Test public void testPreparedPlanInvalidation() throws Exception {
        helpTestPlanInvalidation("select * from #temp a, #temp b limit 10");

        assertEquals(2, this.core.getPrepPlanCache().getCacheHitCount());
    }

    @Test public void testPreparedPlanSimpleNoInvalidation() throws Exception {
        helpTestPlanInvalidation("select * from #temp");

        assertEquals(3, this.core.getPrepPlanCache().getCacheHitCount());
    }

    private void helpTestPlanInvalidation(String query) throws InterruptedException,
            ExecutionException, TimeoutException, TeiidProcessingException {
        String sql = "insert into #temp select * FROM vqt.SmallB"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        int sessionid = 1; //$NON-NLS-1$
        RequestMessage reqMsg = exampleRequestMessage(sql);
        ResultsMessage rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResultsList().size()); //$NON-NLS-1$

        sql = query;
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResultsList().size()); //$NON-NLS-1$

        sql = query;
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResultsList().size()); //$NON-NLS-1$

        assertEquals(1, this.core.getPrepPlanCache().getCacheHitCount());

        Thread.sleep(100);

        //perform a minor update, we should still use the cache
        sql = "delete from #temp where a12345 = '11'"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResultsList().size()); //$NON-NLS-1$

        sql = query;
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResultsList().size()); //$NON-NLS-1$

        assertEquals(2, this.core.getPrepPlanCache().getCacheHitCount());

        //perform a major update, it might purge the plan
        sql = "delete from #temp"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResultsList().size()); //$NON-NLS-1$

        sql = query;
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setStatementType(StatementType.PREPARED);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(0, rm.getResultsList().size()); //$NON-NLS-1$
    }

    @Test public void testRsCacheInvalidation() throws Exception {
        String sql = "select * FROM vqt.SmallB"; //$NON-NLS-1$
        String userName = "1"; //$NON-NLS-1$
        int sessionid = 1; //$NON-NLS-1$
        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setUseResultSetCache(true);
        ResultsMessage rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResultsList().size()); //$NON-NLS-1$
        assertEquals(0, this.core.getRsCache().getCacheHitCount());

        sql = "select * FROM vqt.SmallB"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setUseResultSetCache(true);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResultsList().size()); //$NON-NLS-1$

        assertEquals(1, this.core.getRsCache().getCacheHitCount());

        Thread.sleep(100);

        sql = "delete from bqt1.smalla"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(1, rm.getResultsList().size()); //$NON-NLS-1$

        sql = "select * FROM vqt.SmallB"; //$NON-NLS-1$
        reqMsg = exampleRequestMessage(sql);
        reqMsg.setUseResultSetCache(true);
        rm = execute(userName, sessionid, reqMsg);
        assertEquals(10, rm.getResultsList().size()); //$NON-NLS-1$

        assertEquals(1, this.core.getRsCache().getCacheHitCount());
    }

    @Test public void testLobConcurrency() throws Exception {
        RequestMessage reqMsg = exampleRequestMessage("select to_bytes(stringkey, 'utf-8') FROM BQT1.SmallA");
        reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_OFF);
        agds.setSleep(100);
        ResultsFuture<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        final LobThread t = new LobThread(reqMsg);
        t.start();
        message.addCompletionListener(new ResultsFuture.CompletionListener<ResultsMessage>() {
            @Override
            public void onCompletion(ResultsFuture<ResultsMessage> future) {
                try {
                    final BlobType bt = (BlobType)future.get().getResultsList().get(0).get(0);
                    synchronized (t) {
                        t.bt = bt;
                        t.workContext = DQPWorkContext.getWorkContext();
                        t.notify();
                    }
                    Thread.sleep(100); //give the Thread a chance to run
                } catch (Exception e) {
                    t.interrupt();
                    throw new RuntimeException(e);
                }
            }
        });
        message.get();
        t.join();
        assertNotNull(t.chunkFuture.get().getBytes());
    }

    @Test public void testServerTimeout() throws Exception {
        RequestMessage reqMsg = exampleRequestMessage("select to_bytes(stringkey, 'utf-8') FROM BQT1.SmallA");
        reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_OFF);
        agds.setSleep(100);
        this.config.setQueryTimeout(1);
        ResultsMessage rm = execute("A", 1, reqMsg);
        assertNotNull(rm.getException());
        assertEquals("57014 TEIID30563 The request 1.0 has been cancelled: TEIID31096 Query has exceeded the VDB/engine timeout of 1 milliseconds.", rm.getException().getMessage());
    }

    @Test public void testLongRunningQuery() throws Exception {
        RequestMessage reqMsg = exampleRequestMessage("select * FROM BQT1.SmallA");
        execute("A", 1, reqMsg);
        this.config.setQueryThresholdInMilli(5000);
        assertEquals(1, this.core.getRequests().size());
        assertEquals(0, this.core.getLongRunningRequests().size());
        this.config.setQueryThresholdInMilli(10);
        Thread.sleep(20);
        assertEquals(1, this.core.getLongRunningRequests().size());
    }

    @Test public void testDataAvailable() throws Exception {
        agds.dataNotAvailable = -1;
        agds.dataAvailable = true;
        RequestMessage reqMsg = exampleRequestMessage("select * FROM BQT1.SmallA");
        ResultsMessage results = execute("A", 1, reqMsg);
        if (results.getException() != null) {
            throw results.getException();
        }
    }

    /**
     * Ensure that the row limit is not misapplied.
     * Note that it still could be applied in this example, but the
     * resultset only returns a single row
     */
    @Test public void testProcedureMaxRows() throws Exception {
        String sql = "{? = call TEIIDSP9(1, ?)}"; //$NON-NLS-1$
        RequestMessage request = exampleRequestMessage(sql);
        request.setRowLimit(1);
        request.setStatementType(StatementType.CALLABLE);
        ResultsMessage rm = execute("A", 1, request);

        assertNull(rm.getException());
        assertEquals(2, rm.getResultsList().size());
    }

    @Test public void testProcedureUpdateCount() throws Exception {
        String sql = "{? = call TEIIDSP8(1)}"; //$NON-NLS-1$
        RequestMessage request = exampleRequestMessage(sql);
        request.setResultsMode(ResultsMode.UPDATECOUNT);
        request.setStatementType(StatementType.CALLABLE);
        ResultsMessage rm = execute("A", 1, request);
        assertNull(rm.getException());
        assertEquals(1, rm.getResultsList().size());
    }

    public void helpTestVisibilityFails(String sql) throws Exception {
        RequestMessage reqMsg = exampleRequestMessage(sql);
        reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_OFF);
        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        ResultsMessage results = message.get(5000, TimeUnit.MILLISECONDS);
        assertEquals("[QueryValidatorException]Group does not exist: BQT3.SmallA", results.getException().toString()); //$NON-NLS-1$
    }

    ///////////////////////////Helper method///////////////////////////////////
    private ResultsMessage helpExecute(String sql, String userName) throws Exception {
        return helpExecute(sql, userName, 1, false);
    }

    private ResultsMessage helpExecute(String sql, String userName, int sessionid, boolean txnAutoWrap) throws Exception {
        RequestMessage reqMsg = exampleRequestMessage(sql);
        if (txnAutoWrap) {
            reqMsg.setTxnAutoWrapMode(RequestMessage.TXN_WRAP_ON);
        }
        ResultsMessage results = execute(userName, sessionid, reqMsg);
        core.terminateSession(String.valueOf(sessionid));
        assertNull(core.getClientState(String.valueOf(sessionid), false));
        if (results.getException() != null) {
            throw results.getException();
        }
        return results;
    }

    public ResultsMessage execute(String userName, int sessionid, RequestMessage reqMsg)
            throws InterruptedException, ExecutionException, TimeoutException, TeiidProcessingException {
        DQPWorkContext.getWorkContext().getSession().setSessionId(String.valueOf(sessionid));
        DQPWorkContext.getWorkContext().getSession().setUserName(userName);

        Future<ResultsMessage> message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        assertNotNull(core.getClientState(String.valueOf(sessionid), false));
        ResultsMessage results = message.get(500000, TimeUnit.MILLISECONDS);
        return results;
    }
}
