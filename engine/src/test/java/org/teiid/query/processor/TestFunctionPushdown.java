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

import static org.junit.Assert.fail;
import static org.teiid.query.optimizer.TestOptimizer.checkNodeTypes;
import static org.teiid.query.optimizer.TestOptimizer.getTypicalCapabilities;
import static org.teiid.query.optimizer.TestOptimizer.helpPlan;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.RealMetadataFactory.DDLHolder;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings({"nls", "unchecked"})
public class TestFunctionPushdown {

    @Test public void testMustPushdownOverMultipleSourcesWithoutSupport() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "select func(x.e1) from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2"; //$NON-NLS-1$

        helpPlan(sql, metadata, null, capFinder,
                                      new String[] {}, ComparisonMode.FAILED_PLANNING); //$NON-NLS-1$
    }

    @Test public void testMustPushdownOverMultipleSources() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        String sql = "select func(x.e1) from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder,
                                      new String[] {"SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1, "a")});
        dataManager.addData("SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1), Arrays.asList(2)});
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList("a")});
    }

    @Test public void testSimpleFunctionPushdown() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign function func (param integer) returns integer; create foreign table g1 (e1 integer)", "x", "y");
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {

            @Override
            public PreparedPlan getPreparedPlan(String query, String recursionGroup,
                    CommandContext commandContext, QueryMetadataInterface metadata)
                    throws TeiidProcessingException, TeiidComponentException {
                return null;
            }

            @Override
            public CapabilitiesFinder getCapabiltiesFinder() {
                return capFinder;
            }

            @Override
            public QueryProcessor createQueryProcessor(String query,
                    String recursionGroup, CommandContext commandContext,
                    Object... params) throws TeiidProcessingException,
                    TeiidComponentException {
                // TODO Auto-generated method stub
                return null;
            }
        });
        cc.setMetadata(tm);

        String sql = "select func(1)"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(2)});

        //ensure that pseudo-correlation works
        sql = "select func(0) from g1 where func(e1) = 2"; //$NON-NLS-1$

        plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT y.g1.e1 FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT y.g1.e1 FROM y.g1", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT func(0)", new List[] {Arrays.asList(1)});
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});
        dataManager.addData("SELECT func(2)", new List[] {Arrays.asList(3)});

        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(1)});

        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);

        //ensure that pseudo-correlation works
        sql = "select case when hasrole('x') then func(0) else 2 end from g1"; //$NON-NLS-1$

        plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT func(0) FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT func(0) FROM g1", new List[] {Arrays.asList(1), Arrays.asList(1)});

        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(1), Arrays.asList(1)});

        //correlated case
        sql = "select (select func(e1)) from g1"; //$NON-NLS-1$

        plan = helpPlan(sql, tm, null, capFinder,
                new String[] {"SELECT y.g1.e1 FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT g1.e1 FROM g1", new List[] {Arrays.asList(1), Arrays.asList(1)});
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});

        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(2), Arrays.asList(2)});
    }

    @Test public void testSimpleFunctionPushdown1() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));

        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        bsc.setFunctionSupport("parseDate_", true);
        final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {

            @Override
            public PreparedPlan getPreparedPlan(String query, String recursionGroup,
                    CommandContext commandContext, QueryMetadataInterface metadata)
                    throws TeiidProcessingException, TeiidComponentException {
                return null;
            }

            @Override
            public CapabilitiesFinder getCapabiltiesFinder() {
                return capFinder;
            }

            @Override
            public QueryProcessor createQueryProcessor(String query,
                    String recursionGroup, CommandContext commandContext,
                    Object... params) throws TeiidProcessingException,
                    TeiidComponentException {
                // TODO Auto-generated method stub
                return null;
            }
        });
        cc.setMetadata(tm);

        String sql = "select parseDate_('2011-11-11')"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT parsedate_('2011-11-11')", new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});
        cc.setDQPWorkContext(RealMetadataFactory.buildWorkContext(tm));
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});

        sql = "select misc.namespace.func('2011-11-11')"; //$NON-NLS-1$

        plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT parseDate_('2011-11-11')", new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});
        try {
            TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});
            fail();
        } catch (TeiidProcessingException e) {
            //not supported by any source
        }
    }

    @Test public void testSimpleFunctionPushdown2() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("x", new DDLHolder("y", "CREATE FOREIGN FUNCTION func(a object, b object) RETURNS string;"),
                new DDLHolder("z", "CREATE FOREIGN FUNCTION func1(a object, b object) RETURNS string; create foreign table g1 (e1 object)"));
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {

            @Override
            public PreparedPlan getPreparedPlan(String query, String recursionGroup,
                    CommandContext commandContext, QueryMetadataInterface metadata)
                    throws TeiidProcessingException, TeiidComponentException {
                return null;
            }

            @Override
            public CapabilitiesFinder getCapabiltiesFinder() {
                return capFinder;
            }

            @Override
            public QueryProcessor createQueryProcessor(String query,
                    String recursionGroup, CommandContext commandContext,
                    Object... params) throws TeiidProcessingException,
                    TeiidComponentException {
                // TODO Auto-generated method stub
                return null;
            }
        });
        cc.setMetadata(tm);

        String sql = "select e1 from g1 where func(1, 1) = '2'"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT z.g1.e1 FROM z.g1 WHERE func(1, 1) = '2'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT func(1, 1)", new List[] {Arrays.asList("hello world")});
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {});

        sql = "select e1 from g1 where func1(1, 1) = '2'"; //$NON-NLS-1$

        plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT z.g1.e1 FROM z.g1 WHERE func1(1, 1) = '2'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT g1.e1 FROM g1 WHERE func1(1, 1) = '2'", new List[] {Arrays.asList("hello world")});
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList("hello world")});
    }

    @Test public void testMustPushdownOverMultipleSourcesWithView() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "select func(x.e1) from (select x.* from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2 order by e1 limit 10) as x"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder,
                                      new String[] {"SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1)});
        dataManager.addData("SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1, "aa", "a"), Arrays.asList(2, "bb", "b")});
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList("aa")});
    }

    @Test public void testMustPushdownOverMultipleSourcesWithViewDupRemoval() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "select func(x.e1) from (select distinct x.* from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2 order by e1 limit 10) as x"; //$NON-NLS-1$

        helpPlan(sql, metadata, null, capFinder,
                                      new String[] {}, ComparisonMode.FAILED_PLANNING); //$NON-NLS-1$
    }

    @Test public void testDDLMetadata() throws Exception {
        String ddl = "CREATE VIRTUAL FUNCTION SourceFunc(msg varchar) RETURNS varchar " +
                "OPTIONS(CATEGORY 'misc', DETERMINISM 'DETERMINISTIC', " +
                "\"NULL-ON-NULL\" 'true', JAVA_CLASS '"+TestFunctionPushdown.class.getName()+"', JAVA_METHOD 'sourceFunc');" +
                "CREATE VIEW X (Y varchar) as SELECT e1 from pm1.g1;";

        MetadataFactory mf = TestDDLParser.helpParse(ddl, "model");
        mf.getSchema().setPhysical(false);
        MetadataStore ms = mf.asMetadataStore();
        ms.merge(RealMetadataFactory.example1Cached().getMetadataStore());

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(ms, "example1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("model.SourceFunc", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpPlan("select sourceFunc(y) from x", metadata, null, capFinder,
                new String[] {"SELECT sourceFunc(g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        caps.setFunctionSupport("model.SourceFunc", false);

        helpPlan("select sourceFunc(y) from x", metadata, null, capFinder,
                new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testDDLMetadata1() throws Exception {
        String ddl = "CREATE foreign FUNCTION sourceFunc(msg varchar) RETURNS varchar options (nameinsource 'a.sourcefunc'); " +
                      "CREATE foreign FUNCTION \"b.sourceFunc\"(msg varchar) RETURNS varchar; " +
                "CREATE foreign table X (Y varchar);";

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("phy", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT sourceFunc(g_0.Y), phy.b.sourceFunc(g_0.Y) FROM phy.X AS g_0", metadata, null, capFinder,
                new String[] {"SELECT sourceFunc(g_0.Y), phy.b.sourceFunc(g_0.Y) FROM phy.X AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        //ensure that the source query contains the function schemas
        HardcodedDataManager dm = new HardcodedDataManager(metadata);
        dm.addData("SELECT a.sourcefunc(g_0.Y), b.sourceFunc(g_0.Y) FROM X AS g_0", new List[0]);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        TestProcessor.helpProcess(plan, cc, dm, new List[0]);
    }

    @Test public void testDDLMetadataNameConflict() throws Exception {
        String ddl = "CREATE foreign FUNCTION \"convert\"(msg integer, type varchar) RETURNS varchar; " +
                "CREATE foreign table X (Y integer);";

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("phy", caps); //$NON-NLS-1$

        helpPlan("select phy.convert(y, 'z') from x", metadata, null, capFinder,
                new String[] {"SELECT phy.convert(g_0.Y, 'z') FROM phy.X AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testConcat2() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT2, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "select concat2(x.e1, x.e1) from pm1.g1 as x"; //$NON-NLS-1$

        helpPlan(sql, metadata, null, capFinder,
                                      new String[] {"SELECT concat2(g_0.e1, g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        //cannot pushdown
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT2, false);

        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder,
                new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("a"), Arrays.asList((String)null)});
        TestProcessor.helpProcess(plan, dataManager, new List[] {Arrays.asList("aa"), Arrays.asList((String)null)});

        caps.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        caps.setFunctionSupport(SourceSystemFunctions.IFNULL, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);

        //will get replaced in the LanguageBridgeFactory
        helpPlan(sql, metadata, null, capFinder,
                new String[] {"SELECT concat2(g_0.e1, g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test
    public void testFromUnitTime() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT from_unixtime(x.e2) from pm1.g1 as x"; //$NON-NLS-1$

        // can pushdown
        String expected = "SELECT from_unixtime(convert(g_0.e2, long)) FROM pm1.g1 AS g_0"; //$NON-NLS-1$
        caps.setFunctionSupport(SourceSystemFunctions.FROM_UNIXTIME, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        helpPlan(sql, metadata, null, capFinder, new String[] {expected}, ComparisonMode.EXACT_COMMAND_STRING);

        // can not pushdown
        expected = "SELECT g_0.e2 FROM pm1.g1 AS g_0"; //$NON-NLS-1$
        caps.setFunctionSupport(SourceSystemFunctions.FROM_UNIXTIME, false);
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, new String[] {expected}, ComparisonMode.EXACT_COMMAND_STRING);
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData(expected, new List[] {Arrays.asList(1500000000)});
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
        try {
            TestProcessor.helpProcess(plan, dataManager, new List[] {Arrays.asList("2017-07-13 20:40:00")}); //$NON-NLS-1$
        } finally {
            TimestampWithTimezone.resetCalendar(null);
        }
    }

    @Test public void testPartialProjectPushdown() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);

        ProcessorPlan plan = helpPlan("select case when e1 = 1 then 1 else 0 end, e2 + e4 from pm1.g1", metadata, null, new DefaultCapabilitiesFinder(caps),
                new String[] {"SELECT CASE WHEN g_0.e1 = '1' THEN 1 ELSE 0 END, g_0.e2, g_0.e4 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        HardcodedDataManager dm = new HardcodedDataManager(metadata);
        dm.addData("SELECT CASE WHEN g_0.e1 = '1' THEN 1 ELSE 0 END, g_0.e2, g_0.e4 FROM g1 AS g_0", new List[] {Arrays.asList(1, 2, 3.1)});
        TestProcessor.helpProcess(plan, dm, new List[] {Arrays.asList(1, 5.1)});
    }

    @Test public void testMustPushdownOverGrouping() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign function func (param integer) returns integer; create foreign table g1 (e1 integer)", "x", "y");
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {

            @Override
            public PreparedPlan getPreparedPlan(String query, String recursionGroup,
                    CommandContext commandContext, QueryMetadataInterface metadata)
                    throws TeiidProcessingException, TeiidComponentException {
                return null;
            }

            @Override
            public CapabilitiesFinder getCapabiltiesFinder() {
                return capFinder;
            }

            @Override
            public QueryProcessor createQueryProcessor(String query,
                    String recursionGroup, CommandContext commandContext,
                    Object... params) throws TeiidProcessingException,
                    TeiidComponentException {
                // TODO Auto-generated method stub
                return null;
            }
        });
        cc.setMetadata(tm);

        String sql = "select func(e1) from g1 group by e1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT y.g1.e1 FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT y.g1.e1 FROM y.g1", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});
        dataManager.addData("SELECT func(2)", new List[] {Arrays.asList(3)});

        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(2), Arrays.asList(3)});
    }

    @Test public void testMustPushdownSubexpressionOverGrouping() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign function func (param integer) returns integer; create foreign table g1 (e1 integer, e2 integer)", "x", "y");
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {

            @Override
            public PreparedPlan getPreparedPlan(String query, String recursionGroup,
                    CommandContext commandContext, QueryMetadataInterface metadata)
                    throws TeiidProcessingException, TeiidComponentException {
                return null;
            }

            @Override
            public CapabilitiesFinder getCapabiltiesFinder() {
                return capFinder;
            }

            @Override
            public QueryProcessor createQueryProcessor(String query,
                    String recursionGroup, CommandContext commandContext,
                    Object... params) throws TeiidProcessingException,
                    TeiidComponentException {
                // TODO Auto-generated method stub
                return null;
            }
        });
        cc.setMetadata(tm);

        String sql = "select max(func(e2)) from g1 group by e1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT y.g1.e1, func(y.g1.e2) FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT y.g1.e1, func(y.g1.e2) FROM y.g1", new List[] {Arrays.asList(1, 2), Arrays.asList(2, 3)});

        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(2), Arrays.asList(3)});
    }

    public static String sourceFunc(String msg) {
        return msg;
    }

    @Test public void testPartialProjectPushdownCorrelatedSubquery() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
        ProcessorPlan plan = helpPlan("select x.c, case when e1 = 1 then 1 else 0 end, (select e1 from pm1.g1 where pm1.g1.e1 = pm1.g2.e1) from pm1.g2, (select max(e1) as c from pm1.g2) x where x.c = pm1.g2.e1", metadata, null, new DefaultCapabilitiesFinder(caps),
                new String[] {"SELECT v_0.c_0, g_0.e1, (SELECT g_2.e1 FROM pm1.g1 AS g_2 WHERE g_2.e1 = g_0.e1) FROM pm1.g2 AS g_0, (SELECT MAX(g_1.e1) AS c_0 FROM pm1.g2 AS g_1) AS v_0 WHERE v_0.c_0 = g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        HardcodedDataManager dm = new HardcodedDataManager(metadata);
        dm.addData("SELECT v_0.c_0, g_0.e1, (SELECT g_2.e1 FROM g1 AS g_2 WHERE g_2.e1 = g_0.e1) FROM g2 AS g_0, (SELECT MAX(g_1.e1) AS c_0 FROM g2 AS g_1) AS v_0 WHERE v_0.c_0 = g_0.e1", new List[] {Arrays.asList("a", "a", "a")});
        TestProcessor.helpProcess(plan, dm, new List[] {Arrays.asList("a", 0, "a")});
    }

    @Test public void testMustPushdownSubexpression() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign function func (param integer) returns integer; create foreign table g1 (e1 integer)", "x", "y");
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(tm);

        String sql = "select concat('x', func(1) + e1) from g1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT func(1), y.g1.e1 FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT func(1), g1.e1 FROM g1", new List[] {Arrays.asList(2, 0)});
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList("x2")});

    }

    @Test public void testParseFormatNameCase() throws Exception {
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ONLY_FORMAT_LITERALS, true);
        caps.setFunctionSupport(SourceSystemFunctions.FORMATTIMESTAMP, true);
        caps.setTranslator(new ExecutionFactory<Object, Object> () {
            @Override
            public boolean supportsFormatLiteral(String literal,
                    org.teiid.translator.ExecutionFactory.Format format) {
                return literal.equals("yyyy");
            }
        });
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT stringkey from bqt1.smalla where formatTimestamp(timestampvalue, 'yyyy') = '1921' and parsebigdecimal(stringkey, 'yyyy') = 1 and formatTimestamp(timestampvalue, stringkey) = '19'", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.TimestampValue, g_0.StringKey FROM BQT1.SmallA AS g_0 WHERE formatTimestamp(g_0.TimestampValue, 'yyyy') = '1921'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                1,      // Select
                0,      // Sort
                0       // UnionAll
            });
    }

    @Test public void testSystemNameConflict() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t (x char); create foreign function chr(x char) returns string", "x", "y");

        HardcodedDataManager dataMgr = new HardcodedDataManager();

        String sql = "SELECT chr(x) from t";

        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);

        dataMgr.addData("SELECT y.chr(y.t.x) FROM y.t", Arrays.asList("a"));

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, tm, new DefaultCapabilitiesFinder(bsc));
        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                dataMgr, new List<?>[] {Arrays.asList("a")});
    }

    @Test public void testTimestampAddLong() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table tbl (long_col long, ts_col timestamp);", "x", "y");
        HardcodedDataManager dataMgr = new HardcodedDataManager();

        String sql = "SELECT timestampadd(sql_tsi_second, long_col, ts_col) from tbl";

        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        bsc.setFunctionSupport(SourceSystemFunctions.TIMESTAMPADD, true);

        dataMgr.addData("SELECT y.tbl.long_col, y.tbl.ts_col FROM y.tbl", Arrays.asList(1L, null));

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, tm, new DefaultCapabilitiesFinder(bsc));
        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                dataMgr, new List<?>[] {Collections.singletonList(null)});

        dataMgr.clearData();
        bsc.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        dataMgr.addData("SELECT timestampadd(sql_tsi_second, y.tbl.long_col, y.tbl.ts_col) FROM y.tbl", Collections.singletonList(null));

        plan = TestProcessor.helpGetPlan(sql, tm, new DefaultCapabilitiesFinder(bsc));
        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                dataMgr, new List<?>[] {Collections.singletonList(null)});

    }

    @Test public void testSimpleFunctionPushdownCrossSource() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("x", new DDLHolder("a",
                "create foreign function func (param integer) returns integer; create foreign table tbl2 (col integer);"), new DDLHolder("b", "create foreign table tbl (col integer);"));
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
        final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {

            @Override
            public PreparedPlan getPreparedPlan(String query, String recursionGroup,
                    CommandContext commandContext, QueryMetadataInterface metadata)
                    throws TeiidProcessingException, TeiidComponentException {
                return null;
            }

            @Override
            public CapabilitiesFinder getCapabiltiesFinder() {
                return capFinder;
            }

            @Override
            public QueryProcessor createQueryProcessor(String query,
                    String recursionGroup, CommandContext commandContext,
                    Object... params) throws TeiidProcessingException,
                    TeiidComponentException {
                return null;
            }
        });
        cc.setMetadata(tm);

        String sql = "select func(tbl.col) from tbl"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder,
                                      new String[] {"SELECT b.tbl.col FROM b.tbl"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT b.tbl.col FROM b.tbl", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});
        dataManager.addData("SELECT func(2)", new List[] {Arrays.asList(3)});

        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(2), Arrays.asList(3)});
    }

}
