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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.StatementType;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings({"nls", "unchecked"})
public class TestPreparedStatement {

    private static final int SESSION_ID = 6;

    public static void helpTestProcessing(String preparedSql, List<?> values, List<?>[] expected, ProcessorDataManager dataManager, QueryMetadataInterface metadata, boolean callableStatement, VDBMetaData vdb) throws Exception {
        helpTestProcessing(preparedSql, values, expected, dataManager, metadata, callableStatement, false, vdb);
    }

    static void helpTestProcessing(String preparedSql, List<?> values, List<?>[] expected, ProcessorDataManager dataManager, QueryMetadataInterface metadata, boolean callableStatement, boolean isSessionSpecific, VDBMetaData vdb) throws Exception {
        helpTestProcessing(preparedSql, values, expected, dataManager, (CapabilitiesFinder)null, metadata, null, callableStatement, isSessionSpecific, /* isAlreadyCached */false, vdb);
    }

    static public void helpTestProcessing(String preparedSql, List<?> values, List<?>[] expected, ProcessorDataManager dataManager, CapabilitiesFinder capFinder, QueryMetadataInterface metadata, SessionAwareCache<PreparedPlan> prepPlanCache, boolean callableStatement, boolean isSessionSpecific, boolean isAlreadyCached, VDBMetaData vdb) throws Exception {
        if ( dataManager == null ) {
            // Construct data manager with data
            dataManager = new FakeDataManager();
            TestProcessor.sampleData1((FakeDataManager)dataManager);
        }

        if ( capFinder == null ) {
            capFinder = new DefaultCapabilitiesFinder();
        }

        if ( prepPlanCache == null ) {
            prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0); //$NON-NLS-1$
        }

        // expected cache hit count
        int exHitCount = -1;

        /*
         * If the plan is already cached we want our expected hit
         * count of the cache to be at least 2 because we will
         * get the plan twice.  Otherwise, we want it to be 1.
         */
        if ( isAlreadyCached ) {
            exHitCount = prepPlanCache.getCacheHitCount() + 2;
        } else {
            exHitCount = prepPlanCache.getCacheHitCount() + 1;
        }

        //Create plan or used cache plan if isPlanCached
        PreparedStatementRequest plan = TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, capFinder, metadata, prepPlanCache, SESSION_ID, callableStatement, false, vdb);

        // Run query
        TestProcessor.doProcess(plan.processPlan, dataManager, expected, plan.context);

        //test cached plan
        plan = TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, capFinder, metadata, prepPlanCache, SESSION_ID, callableStatement, false,vdb);

        //make sure the plan is only created once
        assertEquals("should reuse the plan", exHitCount, prepPlanCache.getCacheHitCount()); //$NON-NLS-1$

        // If we are using FakeDataManager, stop command recording to prevent
        // duplicate commands
        boolean dmir = false;
        if (dataManager instanceof FakeDataManager && ((FakeDataManager) dataManager).isRecordingCommands()) {
            dmir = true;
            ((FakeDataManager) dataManager).setRecordingCommands(false);
        }
        // Run query again
        TestProcessor.doProcess(plan.processPlan, dataManager, expected, plan.context);

        // If we are using FakeDataManager and we stopped it from recording,
        // start it back up again
        if (dmir == true) {
            ((FakeDataManager) dataManager).setRecordingCommands(true);
        }

        //get the plan again with a new connection
        assertNotNull(TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, capFinder, metadata, prepPlanCache, 7, callableStatement, false, vdb));

        /*
         * If the command is not specific to a session we expect
         * another hit against the cache because we will use the
         * cached plan, otherwise, a new plan would have been
         * created and the hit count will be unchanged.
         */
        if ( !isSessionSpecific ) exHitCount++;
        assertEquals(exHitCount, prepPlanCache.getCacheHitCount());
    }

    @Test public void testWhere() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE e2=?"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List<?>[] {
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        List<?> values = Arrays.asList((short)0);
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        helpTestProcessing(preparedSql, values, expected, dataManager, RealMetadataFactory.example1Cached(), false, RealMetadataFactory.example1VDB());
    }

    @Test public void testCopiedWhere() throws Exception {
        String preparedSql = "SELECT mediuma.bigdecimalvalue as a FROM bqt1.smalla inner join  bqt1.mediuma "
                + "on (smalla.bigdecimalvalue = mediuma.bigdecimalvalue) "
                + "WHERE smalla.bigdecimalvalue in (?,?) and mediuma.bigdecimalvalue in (1,2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        List<?> values = Arrays.asList(0, 1);

        PreparedStatementRequest plan = helpGetProcessorPlan(preparedSql, values, capFinder, metadata, new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0), SESSION_ID, false, false,RealMetadataFactory.exampleBQTVDB());
        TestOptimizer.checkNodeTypes(plan.processPlan, TestOptimizer.FULL_PUSHDOWN);

        TestOptimizer.checkAtomicQueries(new String[] {"SELECT g_1.BigDecimalValue FROM BQT1.SmallA AS g_0, BQT1.MediumA AS g_1 WHERE (g_0.BigDecimalValue = g_1.BigDecimalValue) AND (g_0.BigDecimalValue IN (?, ?)) AND (g_0.BigDecimalValue IN (1, 2)) AND (g_1.BigDecimalValue IN (1, 2)) AND (g_1.BigDecimalValue IN (?, ?))"}, plan.processPlan);
    }

    @Test public void testObjectCast() throws Exception {
        // Create query
        String preparedSql = "SELECT array_length(cast(? as object))"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List<?>[] {
            Arrays.asList(1), //$NON-NLS-1$
        };

        List<?> values = Arrays.asList(new double[] {1.0});
        FakeDataManager dataManager = new FakeDataManager();
        helpTestProcessing(preparedSql, values, expected, dataManager, RealMetadataFactory.example1Cached(), false, RealMetadataFactory.example1VDB());

        values = Arrays.asList(new Object[] {new Double[] {1.0}});
        helpTestProcessing(preparedSql, values, expected, dataManager, RealMetadataFactory.example1Cached(), false, RealMetadataFactory.example1VDB());
    }

    @Test public void testSessionSpecificFunction() throws Exception {
        // Create query
        String preparedSql = "SELECT session_id(), e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE e2=?"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List<?>[] {
            Arrays.asList(new Object[] { "6",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "6",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }) //$NON-NLS-1$
        };

        List<?> values = Arrays.asList((short)0);
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        helpTestProcessing(preparedSql, values, expected, dataManager, RealMetadataFactory.example1Cached(), false, true, RealMetadataFactory.example1VDB());
    }

    @Test public void testFunctionWithReferencePushDown() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1 and pm1.g1.e2+2=?"; //$NON-NLS-1$

        //Create plan
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("+", false); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        List<?> values = Arrays.asList(0);

        PreparedStatementRequest plan = helpGetProcessorPlan(preparedSql, values, capFinder, metadata, new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0), SESSION_ID, false, false,RealMetadataFactory.example1VDB());

        TestOptimizer.checkNodeTypes(plan.processPlan, TestOptimizer.FULL_PUSHDOWN);
    }

    static public PreparedStatementRequest helpGetProcessorPlan(String preparedSql, List<?> values, SessionAwareCache<PreparedPlan> prepPlanCache)
            throws TeiidComponentException, TeiidProcessingException {
        return helpGetProcessorPlan(preparedSql, values, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), prepPlanCache, SESSION_ID, false, false, RealMetadataFactory.example1VDB());
    }

    static public PreparedStatementRequest helpGetProcessorPlan(String preparedSql, List<?> values,
            SessionAwareCache<PreparedPlan> prepPlanCache, int conn)
            throws TeiidComponentException, TeiidProcessingException {
        return helpGetProcessorPlan(preparedSql, values,
                new DefaultCapabilitiesFinder(), RealMetadataFactory
                        .example1Cached(), prepPlanCache, conn, false, false, RealMetadataFactory.example1VDB());
    }

    static PreparedStatementRequest helpGetProcessorPlan(String preparedSql, List<?> values,
            CapabilitiesFinder capFinder, QueryMetadataInterface metadata, SessionAwareCache<PreparedPlan> prepPlanCache, int conn, boolean callableStatement, boolean limitResults, VDBMetaData vdb)
            throws TeiidComponentException, TeiidProcessingException {

        //Create Request
        RequestMessage request = new RequestMessage(preparedSql);
        if (callableStatement) {
            request.setStatementType(StatementType.CALLABLE);
        } else {
            request.setStatementType(StatementType.PREPARED);
        }
        request.setParameterValues(values);
        if (values != null && values.size() > 0 && values.get(0) instanceof List) {
            request.setBatchedUpdate(true);
        }
        if (limitResults) {
            request.setRowLimit(1);
        }

        if (!(metadata instanceof TempMetadataAdapter)) {
            metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
        }
        DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(metadata, vdb);
        workContext.getSession().setSessionId(String.valueOf(conn));

        PreparedStatementRequest serverRequest = new PreparedStatementRequest(prepPlanCache);

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(new AutoGenDataService());

        serverRequest.initialize(request, BufferManagerFactory.getStandaloneBufferManager(), null,
                new FakeTransactionService(), null, workContext, prepPlanCache);
        serverRequest.setMetadata(capFinder, metadata);
        DefaultAuthorizationValidator drav = new DefaultAuthorizationValidator();
        serverRequest.setAuthorizationValidator(drav);
        serverRequest.processRequest();

        assertNotNull(serverRequest.processPlan);
        return serverRequest;
    }

    @Test public void testValidateCorrectValues() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e1=?"; //$NON-NLS-1$

        List<?> values = Arrays.asList("a"); //$NON-NLS-1$

        //Create plan
        helpGetProcessorPlan(preparedSql, values, new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0));
    }

    /** SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)*/
    @Test public void testWithSubquery() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)"; //$NON-NLS-1$

        List<?> values = Arrays.asList("a"); //$NON-NLS-1$

        //Create plan
        helpGetProcessorPlan(preparedSql, values, new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0));
    }

    /** SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e1 = ? AND pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?) */
    @Test public void testWithSubquery2() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e1 = ? AND pm1.g1.e2 IN (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)"; //$NON-NLS-1$

        List<?> values = Arrays.asList("d", "c"); //$NON-NLS-1$ //$NON-NLS-2$

        //Create plan
        helpGetProcessorPlan(preparedSql, values, new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0));
    }

    /** SELECT X.e1 FROM (SELECT pm1.g2.e1 FROM pm1.g2 WHERE pm1.g2.e1 = ?) as X */
    @Test public void testWithSubquery3() throws Exception {
        // Create query
        String preparedSql = "SELECT X.e1 FROM (SELECT pm1.g2.e1 FROM pm1.g2 WHERE pm1.g2.e1 = ?) as X"; //$NON-NLS-1$

        //Create Request
        List<?> values = Arrays.asList("d"); //$NON-NLS-1$

        //Create plan
        helpGetProcessorPlan(preparedSql, values, new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0));
    }

    @Test public void testValidateWrongValues() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        SessionAwareCache<PreparedPlan> prepCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0); //$NON-NLS-1$

        //wrong type
        try{
            List<?> values = Arrays.asList("x"); //$NON-NLS-1$

            //Create plan
            helpGetProcessorPlan(preparedSql, values, prepCache, SESSION_ID);
            fail();
        }catch(QueryResolverException qe){
            assertEquals("TEIID30558 Error converting parameter number 1 with value \"x\" of class java.lang.String to expected type integer.", qe.getMessage()); //$NON-NLS-1$
        }
        assertEquals(0, prepCache.getCacheHitCount());

        //test cached plan
        try{
            List<Object> values = new ArrayList<Object>();
            values.add("a"); //$NON-NLS-1$
            values.add("b"); //$NON-NLS-1$
            helpGetProcessorPlan(preparedSql, values, prepCache, SESSION_ID);
            fail();
           }catch(QueryResolverException qe){
               assertEquals("TEIID30556 The number of bound values '2' does not match the number of parameters '1' in the prepared statement.", qe.getMessage()); //$NON-NLS-1$
        }

        assertEquals(1, prepCache.getCacheHitCount());

        //wrong number of values
        try{
            List<Object> values = new ArrayList<Object>();
            values.add("a"); //$NON-NLS-1$
            values.add(new Integer(0));
            helpGetProcessorPlan(preparedSql, values, prepCache);
            fail();
        }catch(QueryResolverException qe){
            assertEquals("TEIID30556 The number of bound values '2' does not match the number of parameters '1' in the prepared statement.", qe.getMessage()); //$NON-NLS-1$
        }

    }

    @Test public void testResolveParameterValues() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e2=?"; //$NON-NLS-1$

        List<?> values = Arrays.asList("0"); //$NON-NLS-1$

        helpGetProcessorPlan(preparedSql, values, new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0));
    }

    @Test public void testLimit() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1 WHERE pm1.g1.e2=?"; //$NON-NLS-1$

        List<?> values = Arrays.asList("0"); //$NON-NLS-1$

        SessionAwareCache<PreparedPlan> planCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0); //$NON-NLS-1$

        helpGetProcessorPlan(preparedSql, values, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), planCache, SESSION_ID, false, true, RealMetadataFactory.example1VDB());

        helpGetProcessorPlan(preparedSql, values, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), planCache, SESSION_ID, false, true, RealMetadataFactory.example1VDB());
        //make sure the plan wasn't reused
        assertEquals(1, planCache.getCacheHitCount());
    }

    @Test public void testUpdateProcedureCriteria() throws Exception {
        String preparedSql = "delete from vm1.g37 where e1=?"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList(1),
        };

        List<String> values = Arrays.asList("aa "); //$NON-NLS-1$
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData2b(dataManager, RealMetadataFactory.example1Cached());
        helpTestProcessing(preparedSql, values, expected, dataManager, TestOptimizer.getGenericFinder(), RealMetadataFactory.example1Cached(), null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test(expected=QueryValidatorException.class) public void testLimitValidation() throws Exception {
        String preparedSql = "select pm1.g1.e1 from pm1.g1 limit ?"; //$NON-NLS-1$

        List<?> values = Arrays.asList(-1);
        FakeDataManager dataManager = new FakeDataManager();
        helpTestProcessing(preparedSql, values, null, dataManager, RealMetadataFactory.example1Cached(), false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testExecParam() throws Exception {
        String preparedSql = "exec pm1.sq2(?)"; //$NON-NLS-1$

        List<String> values = Arrays.asList("c"); //$NON-NLS-1$
        List<?>[] expected = new List<?>[] {
                Arrays.asList("c", 1),
            };

        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        helpTestProcessing(preparedSql, values, expected, dataManager, RealMetadataFactory.example1Cached(), false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testLimitParam() throws Exception {
        String preparedSql = "select e1 from pm1.g1 order by e1 desc limit ?"; //$NON-NLS-1$

        List<?> values = Arrays.asList(1);
        List<?>[] expected = new List<?>[] {
                Arrays.asList("c"), //$NON-NLS-1$s
            };

        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        helpTestProcessing(preparedSql, values, expected, dataManager, RealMetadataFactory.example1Cached(), false, false,RealMetadataFactory.example1VDB());
    }

    @Test public void testLimitOffsetParams() throws Exception {
        String preparedSql = "select e1 from pm1.g1 order by e1 desc limit ? offset ?"; //$NON-NLS-1$

        List<?> values = Arrays.asList(1, 2);
        List<?>[] expected = new List<?>[] {
                Arrays.asList("a"), //$NON-NLS-1$s
            };

        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        helpTestProcessing(preparedSql, values, expected, dataManager, RealMetadataFactory.example1Cached(), false, false,RealMetadataFactory.example1VDB());
    }

    @Test public void testWithSubqueryPushdown() throws Exception {
        String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e2 IN /*+ no_unnest */ (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList("a"),
        };

        List<?> values = Arrays.asList("a"); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e2 IN (SELECT g_1.e2 FROM g2 AS g_1 WHERE g_1.e1 = 'a')", new List<?>[] {Arrays.asList("a")});
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);

        helpTestProcessing(preparedSql, values, expected, dataManager, new DefaultCapabilitiesFinder(caps), metadata, null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testInherentlyUpdatableViewCompensation() throws Exception {
        String preparedSql = "SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e2 IN /*+ no_unnest */ (SELECT pm1.g2.e2 FROM pm1.g2 WHERE pm1.g2.e1 = ?)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList("a"),
        };

        List<?> values = Arrays.asList("a"); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e2 IN (SELECT g_1.e2 FROM g2 AS g_1 WHERE g_1.e1 = 'a')", new List<?>[] {Arrays.asList("a")});
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);

        helpTestProcessing(preparedSql, values, expected, dataManager, new DefaultCapabilitiesFinder(caps), metadata, null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testInsertWithSimpleSelect() throws Exception {
        String preparedSql = "insert into pm1.g1 (e1, e2) select ?, ?"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList(1),
        };

        List<?> values = Arrays.asList("a", "1"); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("INSERT INTO g1 (e1, e2) VALUES ('a', 1)", new List<?>[] {Arrays.asList(1)});
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        helpTestProcessing(preparedSql, values, expected, dataManager, new DefaultCapabilitiesFinder(caps), metadata, null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testInsertWithMultipleValues() throws Exception {
        String preparedSql = "insert into pm1.g1 (e1, e2) values (?, ?), (?, ?)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList(2),
        };

        List<?> values = Arrays.asList("a", "1", "b", "2"); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("INSERT INTO g1 (e1, e2) VALUES ('a', 1)", new List<?>[] {Arrays.asList(1)});
        dataManager.addData("INSERT INTO g1 (e1, e2) VALUES ('b', 2)", new List<?>[] {Arrays.asList(1)});
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        helpTestProcessing(preparedSql, values, expected, dataManager, new DefaultCapabilitiesFinder(caps), metadata, null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testAnonBlockIn() throws Exception {
        String preparedSql = "begin insert into pm1.g1 (e1, e2) select ?, ?; select rowcount; end;"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList(1),
        };

        List<?> values = Arrays.asList("a", "1"); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("INSERT INTO g1 (e1, e2) VALUES ('a', 1)", new List<?>[] {Arrays.asList(1)});
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        helpTestProcessing(preparedSql, values, expected, dataManager, new DefaultCapabilitiesFinder(caps), metadata, null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testBranchPruningPrepared() throws Exception {
        String preparedSql = "select * from (select 'a' as branch, e1 from pm1.g1 union all select 'b', pm1.g2.e1 as branch from pm1.g2, pm1.g1) as x where branch = ?"; //$NON-NLS-1$

        List<?> values = Arrays.asList("a"); //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList("a", "b"),
        };

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0", new List<?>[] {Arrays.asList("b")});
        CapabilitiesFinder caps = TestOptimizer.getGenericFinder(false);

        helpTestProcessing(preparedSql, values, expected, dataManager, caps, metadata, null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testParallelIn() throws Exception {
        String preparedSql = "select e1 from pm1.g1 where pm1.g1.e2 in (?,?,?,?,?,?,?,?)"; //$NON-NLS-1$

        List<?> values = Arrays.asList(1,2,3,4,5,6,7,8); //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Arrays.asList("a"),
            Arrays.asList("b"),
        };

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        int maxInSize = 4;
        dataManager.getLanguageBridgeFactory().setMaxInPredicateSize(maxInSize); //normally set in the connectorworkitem
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e2 IN (1, 2, 3, 4) OR g_0.e2 IN (5, 6, 7, 8)", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, maxInSize);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, null, false, false, false, RealMetadataFactory.example1VDB());

        dataManager.clearData();
        //should cause two parallel queries
        bsc.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e2 IN (1, 2, 3, 4)", new List<?>[] {Arrays.asList("a")});
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e2 IN (5, 6, 7, 8)", new List<?>[] {Arrays.asList("b")});
        helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, null, false, false, false, RealMetadataFactory.example1VDB());

        dataManager.clearData();
        preparedSql = "select e1 from pm1.g1 where pm1.g1.e2 in (1,2,3,4,5,6,7,8) and e1 < ?"; //$NON-NLS-1$
        values = Arrays.asList("c"); //$NON-NLS-1$
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e1 < 'c' AND g_0.e2 IN (1, 2, 3, 4)", new List<?>[] {Arrays.asList("a")});
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e1 < 'c' AND g_0.e2 IN (5, 6, 7, 8)", new List<?>[] {Arrays.asList("b")});
        helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, null, false, false, false, RealMetadataFactory.example1VDB());
    }

    @Test public void testDependentRewrite() throws Exception {
        // Create query
        String preparedSql = "SELECT pm1.g1.e1, pm1.g1.e3 FROM pm1.g1 left outer join /*+ makedep */ pm2.g1 on pm1.g1.e1 = pm2.g1.e1 and pm1.g1.e2 in (?, ?) where pm2.g1.e1 in (?, ?)"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List<?>[] {
            Arrays.asList("b", Boolean.FALSE), //$NON-NLS-1$
            Arrays.asList("b", Boolean.FALSE), //$NON-NLS-1$
        };

        List<?> values = Arrays.asList(1, 2, "a", "b");
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);

        helpTestProcessing(preparedSql, values, expected, dataManager, new DefaultCapabilitiesFinder(caps), RealMetadataFactory.example1Cached(), null, false, false, false, RealMetadataFactory.example1VDB());
    }

}

