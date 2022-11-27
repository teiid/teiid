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
import static org.teiid.query.processor.TestProcessor.*;
import static org.teiid.query.processor.TestProcessor.helpParse;
import static org.teiid.query.resolver.TestResolver.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.TestParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestArrayProcessing {

    @Test public void testArrayCast() throws Exception {
        String sql = "select cast(cast((1,2) as object) as integer[])"; //$NON-NLS-1$

        helpResolve(sql, RealMetadataFactory.example1Cached());

        //should succeed
        sql = "select cast(cast((1,2) as object) as integer[])"; //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached());

        helpProcess(plan, dataManager, null);

        //should succeed
        sql = "select cast(cast((1,2) as object) as string[])"; //$NON-NLS-1$

        plan = helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached(), DefaultCapabilitiesFinder.INSTANCE, createCommandContext());
        helpProcess(plan, dataManager, null);

        //should fail
        sql = "select cast(cast((1,2) as object) as time[])"; //$NON-NLS-1$

        try {
            helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached(), DefaultCapabilitiesFinder.INSTANCE, createCommandContext());
            fail();
        } catch (TeiidProcessingException e) {

        }
    }

    @Test public void testArrayComparison() {
        String sql = "select count(e1) from pm1.g1 where (e1, e2) = ('a', 1)";

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", Arrays.asList("a", 2), Arrays.asList("a", 1));
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});

        dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE (g_0.e1, g_0.e2) = ('a', 1)", Arrays.asList("a"));
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ARRAY_TYPE, true);
        plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc));

        helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});
    }

    @Test public void testArraySort() {
        String sql = "select (e1, e2) from pm1.g1 order by (e1, e2), e3";

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3 FROM pm1.g1", Arrays.asList("b", 4, true), Arrays.asList("a", 2, true), Arrays.asList("a", 1, false));
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(new ArrayImpl("a", 1)),
                Arrays.asList(new ArrayImpl("a", 2)),
                Arrays.asList(new ArrayImpl("b", 4))});
    }

    @Test public void testArrayGetTyping() {
        String sql = "select array_agg(e1)[1], array_agg(e2)[3] from pm1.g1"; //$NON-NLS-1$

        Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, command.getProjectedSymbols().get(0).getType());
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, command.getProjectedSymbols().get(1).getType());
    }

    @Test(expected=QueryResolverException.class) public void testArrayGetTypingFails() throws QueryResolverException, TeiidComponentException {
        String sql = "select array_agg(e1)[1][2] from pm1.g1"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
    }

    @Test public void testArrayParsing() throws Exception {
        TestParser.helpTestExpression("()", "()", new Array(new ArrayList<Expression>()));
        TestParser.helpTestExpression("(,)", "()", new Array(new ArrayList<Expression>()));
        TestParser.helpTestExpression("(1,)", "(1,)", new Array(Arrays.asList((Expression)new Constant(1))));
        TestParser.helpTestExpression("(1,2)", "(1, 2)", new Array(Arrays.asList((Expression)new Constant(1), (Expression)new Constant(2))));
        TestParser.helpTestExpression("(1,2,)", "(1, 2)", new Array(Arrays.asList((Expression)new Constant(1), (Expression)new Constant(2))));
    }

    @Test public void testArrayEquivalence() throws Exception {
        Array a1 = new Array(new ArrayList<Expression>());

        UnitTestUtil.helpTestEquivalence(0, a1, a1);

        Array a2 = new Array(Arrays.asList((Expression)new Constant(1)));

        UnitTestUtil.helpTestEquivalence(1, a1, a2);
    }

    @Test public void testArrayTable() throws Exception {
        String sql = "select x.* from arraytable(('a', 2-1, {d'2001-01-01'}) COLUMNS x string, y integer) x"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList("a", 1),
        };

        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, new HardcodedDataManager(), expected);
    }

    @Test public void testMultiDimensionalGet() throws Exception {
        String sql = "select -((e2, e2), (e2, e2))[1][1] from pm1.g1"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, command.getProjectedSymbols().get(0).getType());
    }

    @Test public void testMultiDimensionalCast() throws Exception {
        String sql = "select cast( ((e2, e2), (e2, e2)) as object[])  from pm1.g1"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(Object[].class, command.getProjectedSymbols().get(0).getType());

        ProcessorPlan pp = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList(1), Arrays.asList(2));
        TestProcessor.helpProcess(pp, dataManager, new List[] {
                Arrays.asList(new ArrayImpl((Object[])new Integer[][] {new Integer[] {1,1}, new Integer[] {1,1}})),
                Arrays.asList(new ArrayImpl((Object[])new Integer[][] {new Integer[] {2,2}, new Integer[] {2,2}}))});

        sql = "select cast(cast( ((e2, e2), (e2, e2)) as object[]) as integer[][])  from pm1.g1"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(Integer[][].class, command.getProjectedSymbols().get(0).getType());
    }

    @Test public void testMultiDimensionalArrayRewrite() throws Exception {
        String sql = "select (('a', 'b'),('c','d'))"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(String[][].class, command.getProjectedSymbols().get(0).getType());

        command = QueryRewriter.rewrite(command, RealMetadataFactory.example1Cached(), null);
        Expression ex = SymbolMap.getExpression(command.getProjectedSymbols().get(0));
        Constant c = (Constant)ex;
        assertTrue(c.getValue() instanceof ArrayImpl);
    }

    @Test public void testArrayResolvingNull() throws Exception {
        String sql = "select (null, 'a')"; //$NON-NLS-1$

        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(String[].class, command.getProjectedSymbols().get(0).getType());

        sql = "select ((null,), ('a',))"; //$NON-NLS-1$

        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(String[][].class, command.getProjectedSymbols().get(0).getType());
    }

    @Test public void testQuantifiedCompareRewrite() throws Exception {
        String sql = "select 'a' < ALL (('b','c'))"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals("SELECT 'a' < ALL (('b', 'c'))", command.toString());
        command = QueryRewriter.rewrite(command, RealMetadataFactory.example1Cached(), null);
        assertEquals("SELECT TRUE", command.toString());

        sql = "select 'a' < ALL ((null,'c'))"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
        command = helpResolve(sql, RealMetadataFactory.example1Cached());
        command = QueryRewriter.rewrite(command, RealMetadataFactory.example1Cached(), null);
        assertEquals("SELECT UNKNOWN", command.toString());
    }

    @Test(expected=QueryResolverException.class) public void testQuantifiedCompareResolving() throws Exception {
        String sql = "select 'a' < ALL ('b')"; //$NON-NLS-1$
        QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
    }

    @Test(expected=QueryResolverException.class) public void testQuantifiedCompareResolving1() throws Exception {
        String sql = "select 1 < ALL (('1', '2'))"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1Cached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        QueryResolver.resolveCommand(helpParse(sql), tm);
    }

    @Test public void testQuantifiedCompareResolving2() throws Exception {
        String sql = "select '1' < ALL (cast(? as integer[]))"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.example1Cached().getDesignTimeMetadata();
        tm.setWidenComparisonToString(false);
        Command command = helpParse(sql);
        QueryResolver.resolveCommand(command, tm);
        command = QueryRewriter.rewrite(command, tm, null);
        assertEquals("SELECT 1 < ALL (?)", command.toString());
    }

    @Test public void testQuantifiedCompareProcessing() throws Exception {
        String sql = "select e2 from pm1.g1 where e1 = some (('a', 'b'))"; //$NON-NLS-1$
        Command command = helpParse(sql);
        ProcessorPlan pp = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList("a", 1), Arrays.asList("c", 2));
        TestProcessor.helpProcess(pp, dataManager, new List[] {Arrays.asList(1)});

    }

    @Test public void testQuantifiedCompareProcessingNull() throws Exception {
        String sql = "select e2 from g1, g2 where g1.e1 = some (g2.x)"; //$NON-NLS-1$
        Command command = helpParse(sql);
        ProcessorPlan pp = TestProcessor.helpGetPlan(command, RealMetadataFactory.fromDDL(""
                + "create foreign table g1 (e1 string, e2 integer);"
                + "create foreign table g2 (x string[]);"
                , "x", "y"), TestOptimizer.getGenericFinder());
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e1, g_0.e2 FROM y.g1 AS g_0", Arrays.asList("a", 1), Arrays.asList("c", 2));
        dataManager.addData("SELECT g_0.x FROM y.g2 AS g_0", Collections.singletonList(null), Arrays.asList(new ArrayImpl("a", "b")));
        TestProcessor.helpProcess(pp, dataManager, new List[] {Arrays.asList(1)});
    }

    @Test public void testNestedArrayAgg() throws Exception {
        String sql = "select array_agg((e1, e2)) from pm1.g1"; //$NON-NLS-1$
        Command command = helpParse(sql);
        ProcessorPlan pp = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList("a", 1), Arrays.asList("c", 2));
        TestProcessor.helpProcess(pp, dataManager, new List[] {Arrays.asList(new ArrayImpl(new Object[] {"a", 1}, new Object[] {"c", 2}))});
    }

    @Test(expected=TeiidProcessingException.class) public void testLargeArrays() throws Exception {
        String sql = "WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 70000 ) SELECT array_agg((n, n, n, n, n)) FROM t;"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList(2080L),
        };

        FakeDataManager dataManager = new FakeDataManager();
        dataManager.setBlockOnce();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        CommandContext cc = createCommandContext();
        cc.setSession(new SessionMetadata());
        cc.setSessionVariable(TempTableStore.TEIID_MAX_RECURSION, 100000);
        helpProcess(plan, cc, dataManager, expected);
    }

    @Test public void testArrayProjection() throws Exception {
        String sql = "SELECT e1, (e2, e3) FROM pm1.g1";
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        bsc.setCapabilitySupport(Capability.ARRAY_TYPE, true);
        ProcessorPlan pp = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc));
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3 FROM pm1.g1", Arrays.asList("a", 1, false));
        TestProcessor.helpProcess(pp, dataManager, new List[] {Arrays.asList("a", new ArrayImpl(1, false))});

        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION_ARRAY_TYPE, true);
        pp = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc));
        dataManager.addData("SELECT pm1.g1.e1, (pm1.g1.e2, pm1.g1.e3) FROM pm1.g1", Arrays.asList("a", new ArrayImpl(1, false)));
        TestProcessor.helpProcess(pp, dataManager, new List[] {Arrays.asList("a", new ArrayImpl(1, false))});
    }


    /**
     * TODO
     @Test public void testArrayLobs() {
        //ensure that we introspect arrays for lob references
    }
     */

}
