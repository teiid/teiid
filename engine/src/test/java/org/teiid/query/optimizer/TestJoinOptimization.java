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

package org.teiid.query.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.teiid.query.processor.TestProcessor.createCommandContext;
import static org.teiid.query.processor.TestProcessor.helpProcess;
import static org.teiid.query.processor.TestProcessor.sampleData1;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord.Type;
import org.teiid.metadata.Table;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.JoinUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestJoinOptimization {

    /**
     * Single group criteria should get pushed and be eligible for copy criteria
     */
    @Test public void testInnerJoinPushAndCopyNonJoinCriteria() {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey from bqt1.smalla inner join bqt2.smalla on (bqt1.smalla.intkey = bqt2.smalla.intkey and bqt2.smalla.intkey = 1)"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT bqt1.smalla.intkey FROM bqt1.smalla WHERE bqt1.smalla.intkey = 1", "SELECT bqt2.smalla.intkey FROM bqt2.smalla WHERE bqt2.smalla.intkey = 1"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Single group criteria should get pushed when it is on the inner side
     */
    @Test public void testOuterJoinPushNonJoinCriteria() {
        String sql = "select bqt1.smalla.intkey from bqt1.smalla left outer join bqt2.smalla on (bqt1.smalla.intkey = bqt2.smalla.intkey and bqt2.smalla.stringkey = 1)"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.intkey AS c_0 FROM bqt1.smalla AS g_0 ORDER BY c_0", "SELECT g_0.intkey AS c_0 FROM bqt2.smalla AS g_0 WHERE g_0.stringkey = '1' ORDER BY c_0"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Single group criteria should not be pushed when it is on the outer side
     */
    @Test public void testOuterJoinPushNonJoinCriteriaA() {
        String sql = "select bqt1.smalla.intkey from bqt1.smalla left outer join bqt2.smalla on (bqt1.smalla.intkey = bqt2.smalla.intkey and bqt1.smalla.stringkey = 1)"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.intkey AS c_0 FROM bqt2.smalla AS g_0 ORDER BY c_0", "SELECT g_0.intkey AS c_0, g_0.stringkey AS c_1 FROM bqt1.smalla AS g_0 ORDER BY c_0"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testOuterJoinPushNonJoinCriteria_Case5547() {
        String sql = "select bqt1.smalla.intkey from bqt1.smalla left outer join bqt2.smalla on (1=1)"; //$NON-NLS-1$
        String BQT1 = "BQT1";   //$NON-NLS-1$
        String BQT2 = "BQT2";   //$NON-NLS-1$
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        // ===  Must set the ORDER BY prop on the capabilities object to TRUE
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities(BQT1, caps);
        capFinder.addCapabilities(BQT2, caps);

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                                                    metadata,
                                                    null,
                                                    capFinder,
                                                    new String[] {"SELECT bqt1.smalla.intkey FROM bqt1.smalla", "SELECT 1 FROM bqt2.smalla"}, true ); //$NON-NLS-1$  //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

    }


    /**
     * Single group criteria should not be pushed when it is used in a full outer join
     * Note that the join has also degraded into a cross join rather than an outer join
     */
    @Test public void testFullOuterJoinPushNonJoinCriteria() {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey from bqt1.smalla full outer join bqt2.smalla on (bqt1.smalla.intkey = bqt2.smalla.intkey and bqt1.smalla.stringkey = 1 and bqt2.smalla.stringkey = 1)"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.intkey AS c_0, g_0.stringkey AS c_1 FROM bqt2.smalla AS g_0 ORDER BY c_0", "SELECT g_0.intkey AS c_0, g_0.stringkey AS c_1 FROM bqt1.smalla AS g_0 ORDER BY c_0"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testFullOuterJoinPredicatePlacement() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select b1.intkey, b2.intkey from (select * from bqt1.smalla where bqt1.smalla.stringkey = 'a') b1 full outer join (select * from bqt1.smallb where bqt1.smallb.stringkey = 'b') b2 on (b1.intkey = b2.intkey)"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, false);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.StringKey = 'a'", "SELECT g_0.IntKey FROM BQT1.SmallB AS g_0 WHERE g_0.StringKey = 'b'"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFullOuterJoinPushCriteria() throws TeiidComponentException, TeiidProcessingException {
        String sql = "SELECT 1\n" +
                "from bqt1.smalla a full outer join bqt2.smallb b on a.intkey=b.intkey\n" +
                "WHERE\n" +
                "     a.intkey=1 ;"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(
                sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT 1 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey = 1", "SELECT 1 FROM BQT2.SmallB AS g_0 WHERE g_0.IntKey = 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testEvaluatableSubqueryInOn() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select b1.intkey, b2.intkey from bqt1.smalla b1 left outer join bqt1.smallb b2 on (b1.intkey = b2.intkey and b2.stringkey = (select stringkey from bqt1.mediuma))"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, false);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey, g_1.IntKey FROM BQT1.SmallA AS g_0 LEFT OUTER JOIN BQT1.SmallB AS g_1 ON g_0.IntKey = g_1.IntKey AND g_1.StringKey = (SELECT g_0.StringKey FROM BQT1.MediumA AS g_0)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey FROM BQT1.MediumA AS g_0", Arrays.asList("b"));
        hdm.addData("SELECT g_0.IntKey, g_1.IntKey FROM BQT1.SmallA AS g_0 LEFT OUTER JOIN BQT1.SmallB AS g_1 ON g_0.IntKey = g_1.IntKey AND g_1.StringKey = 'b'", Arrays.asList(1, 1));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, 1)});
    }

    /**
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    @Test public void testEvaluatableSubqueryInOn1() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select b1.intkey, b2.intkey from bqt1.smalla b1 left outer join bqt1.smallb b2 on (b1.intkey = b2.intkey and b2.stringkey in (select stringkey from bqt1.mediuma))"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, false);
        bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 ORDER BY c_0",
            "SELECT g_0.IntKey AS c_0 FROM BQT1.SmallB AS g_0 WHERE g_0.StringKey IN (SELECT g_1.StringKey FROM BQT1.MediumA AS g_1) ORDER BY c_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 ORDER BY c_0", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT1.SmallB AS g_0 WHERE g_0.StringKey IN (SELECT g_1.StringKey FROM BQT1.MediumA AS g_1) ORDER BY c_0", Arrays.asList(1));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, 1)});
    }

    /**
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    @Test public void testEvaluatableSubqueryInOn1a() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select b1.intkey, b2.intkey from bqt1.smalla b1 left outer join bqt1.smallb b2 on (b1.intkey = b2.intkey and exists (select stringkey from bqt1.mediuma where stringkey = b2.stringkey))"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, false);
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 ORDER BY c_0",
                "SELECT g_0.IntKey AS c_0 FROM BQT1.SmallB AS g_0 WHERE EXISTS (SELECT g_1.StringKey FROM BQT1.MediumA AS g_1 WHERE g_1.StringKey = g_0.StringKey) ORDER BY c_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 ORDER BY c_0", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT1.SmallB AS g_0 WHERE EXISTS (SELECT g_1.StringKey FROM BQT1.MediumA AS g_1 WHERE g_1.StringKey = g_0.StringKey) ORDER BY c_0", Arrays.asList(1));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, 1)});
    }

    @Test public void testEvaluatableSubqueryInOn1b() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select b1.intkey, b2.intkey from bqt1.smalla b1 left outer join bqt1.smallb b2 on (b1.intkey = b2.intkey and exists (select stringkey from bqt1.mediuma))"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, false);
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, false);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey, g_1.IntKey FROM BQT1.SmallA AS g_0 LEFT OUTER JOIN BQT1.SmallB AS g_1 ON g_0.IntKey = g_1.IntKey AND EXISTS (SELECT g_0.StringKey FROM BQT1.MediumA AS g_0)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey FROM BQT1.MediumA AS g_0");
        hdm.addData("SELECT g_0.IntKey, g_1.IntKey FROM BQT1.SmallA AS g_0 LEFT OUTER JOIN BQT1.SmallB AS g_1 ON 1 = 0", Arrays.asList(1, null));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, null)});

        //make sure even if exists is supported, this doesn't inhibit pushdown
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);

        plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey, g_1.IntKey FROM BQT1.SmallA AS g_0 LEFT OUTER JOIN BQT1.SmallB AS g_1 ON g_0.IntKey = g_1.IntKey AND EXISTS (SELECT g_0.StringKey FROM BQT1.MediumA AS g_0)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, null)});
    }

    /**
     * Copy criteria should still work here even though the join criteria has an implicit type conversion because
     * the equality operation on the select criteria can be used.
     */
    @Test public void testCopyCriteriaWithFunction1() {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey from bqt1.smalla, bqt2.smalla where bqt1.smalla.stringkey = bqt2.smalla.intkey and bqt2.smalla.intkey = 1"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT bqt1.smalla.intkey FROM bqt1.smalla WHERE bqt1.smalla.stringkey = '1'", "SELECT bqt2.smalla.intkey FROM bqt2.smalla WHERE bqt2.smalla.intkey = 1"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Copy criteria should not work here as the join criteria has an implicit convert and the where criteria is a non-equality predicate
     */
    @Test public void testCopyCriteriaWithFunction2() {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey from bqt1.smalla, bqt2.smalla where bqt1.smalla.stringkey = bqt2.smalla.intkey and bqt2.smalla.intkey <> 1"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.intkey FROM bqt2.smalla AS g_0 WHERE g_0.intkey <> 1", "SELECT g_0.stringkey AS c_0, g_0.intkey AS c_1 FROM bqt1.smalla AS g_0 ORDER BY c_0"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testCopyCriteriaWithFunction3() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select bqt1.smalla.intkey, bqt1.smallb.intkey from bqt1.smalla, bqt1.smallb where bqt1.smalla.stringkey = bqt1.smallb.intkey and bqt1.smallb.intkey = 1"; //$NON-NLS-1$

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.IntKey, g_1.IntKey FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1 WHERE (g_0.StringKey = '1') AND (g_1.IntKey = 1)"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.StringKey = '1'", "SELECT g_0.IntKey FROM BQT1.SmallB AS g_0 WHERE g_0.IntKey = 1"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * The intkey criteria should not be copied above to bqt1.smalla since the criteria is coming from the inner side in the join below
     */
    @Test public void testInvalidCopyCriteria() {
        String sql = "select bqt1.smalla.intkey from bqt1.smalla inner join (select bqt3.smalla.intkey from bqt2.smalla left outer join bqt3.smalla on bqt2.smalla.intkey = bqt3.smalla.intkey and bqt3.smalla.intkey = 1) foo on bqt1.smalla.intkey = foo.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT 1 FROM BQT2.SmallA AS g_0 WHERE g_0.IntKey = 1", "SELECT 1 FROM BQT3.SmallA AS g_0 WHERE g_0.IntKey = 1", "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey = 1"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            2,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /*
     * Note that the criteria does not get copied to the outer side.
     */
    @Test public void testCopyCriteriaFromInnerSide() throws Exception {
        String sql = "select bqt1.smalla.intkey from bqt1.smalla left outer join (select bqt3.smalla.intkey from bqt3.smalla where bqt3.smalla.intkey = 1) foo on bqt1.smalla.intkey = foo.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.IntKey FROM BQT1.SmallA AS g_0", "SELECT 1 FROM BQT3.SmallA AS g_0 WHERE g_0.IntKey = 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Check to ensure that the full outer join does not get merged since the where criteria cannot be moved
     */
    @Test public void testFullOuterJoinPreservation() {
        String sql = "select bqt2.mediumb.intkey from bqt2.mediumb full outer join (select bqt2.smallb.intkey from bqt2.smalla left outer join bqt2.smallb on bqt2.smalla.intkey = bqt2.smallb.intkey where bqt2.smalla.stringkey = 1) foo on bqt2.mediumb.intkey = foo.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                                    new String[] {"SELECT g_1.intkey FROM bqt2.smalla AS g_0 LEFT OUTER JOIN bqt2.smallb AS g_1 ON g_0.intkey = g_1.intkey WHERE g_0.stringkey = '1'", "SELECT g_0.intkey AS c_0 FROM bqt2.mediumb AS g_0 ORDER BY c_0"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Same as above but with a 0 group criteria
     */
    @Test public void testFullOuterJoinPreservation1() {
        String sql = "select bqt2.mediumb.intkey from bqt2.mediumb full outer join (select bqt2.smallb.intkey from bqt2.smalla inner join bqt2.smallb on bqt2.smalla.intkey = bqt2.smallb.intkey where ? = 1) foo on bqt2.mediumb.intkey = foo.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.intkey AS c_0 FROM bqt2.mediumb AS g_0 ORDER BY c_0", "SELECT g_1.intkey FROM bqt2.smalla AS g_0, bqt2.smallb AS g_1 WHERE (g_0.intkey = g_1.intkey) AND (? = 1)"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Same as above but with a left outer join
     */
    @Test public void testOuterJoinPreservation() {
        String sql = "select bqt2.mediumb.intkey from bqt2.mediumb left outer join (select bqt2.smallb.intkey from bqt2.smalla inner join bqt2.smallb on bqt2.smalla.intkey = bqt2.smallb.intkey where ? = 1) foo on bqt2.mediumb.intkey = foo.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.intkey AS c_0 FROM bqt2.mediumb AS g_0 ORDER BY c_0", "SELECT g_1.intkey FROM bqt2.smalla AS g_0, bqt2.smallb AS g_1 WHERE (g_0.intkey = g_1.intkey) AND (? = 1)"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testCopyCriteriaCreatesFalseCriteria() {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey from bqt1.smalla, bqt2.smalla where bqt1.smalla.stringkey = bqt2.smalla.intkey and bqt2.smalla.intkey = 1 and bqt1.smalla.stringkey = '2'"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {});

        TestOptimizer.checkNodeTypes(plan, TestRuleRaiseNull.FULLY_NULL);
    }

    @Test public void testPushNonJoinCriteriaWithFalse() {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey from bqt1.smalla left outer join bqt2.smalla on (bqt1.smalla.stringkey = bqt2.smalla.intkey and bqt2.smalla.intkey = null)"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT bqt1.smalla.intkey FROM bqt1.smalla"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testPushMultiGroupJoinCriteria() throws Exception {
        String sql = "select bqt1.smalla.intkey, bqt1.smallb.intkey from bqt1.smalla right outer join (bqt1.smallb cross join (bqt1.mediuma cross join bqt1.mediumb)) on bqt1.smalla.stringkey = bqt1.smallb.stringkey" //$NON-NLS-1$
            +" where bqt1.smallb.intkey + bqt1.mediuma.intkey + bqt1.mediumb.intkey = 1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                                    new String[] {"SELECT g_3.IntKey, g_2.IntKey FROM (BQT1.MediumA AS g_0 CROSS JOIN BQT1.MediumB AS g_1) INNER JOIN (BQT1.SmallB AS g_2 LEFT OUTER JOIN BQT1.SmallA AS g_3 ON g_3.StringKey = g_2.StringKey) ON ((g_2.IntKey + g_0.IntKey) + g_1.IntKey) = 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Since the multigroup criteria spans the inner side, it should not be pushed.
     */
    @Test public void testPushMultiGroupJoinCriteria1() {
        String sql = "select bqt1.smalla.intkey, bqt1.smallb.intkey from bqt1.smalla right outer join (bqt1.smallb cross join (bqt1.mediuma cross join bqt1.mediumb)) on bqt1.smalla.stringkey = bqt1.smallb.stringkey" //$NON-NLS-1$
            +" where bqt1.smalla.intkey + bqt1.mediuma.intkey + bqt1.mediumb.intkey is null"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                                    new String[] {"SELECT g_3.intkey, g_0.intkey FROM ((bqt1.smallb AS g_0 CROSS JOIN bqt1.mediuma AS g_1) CROSS JOIN bqt1.mediumb AS g_2) LEFT OUTER JOIN bqt1.smalla AS g_3 ON g_3.stringkey = g_0.stringkey WHERE ((g_3.intkey + g_1.intkey) + g_2.intkey) IS NULL"}, true); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Since the multigroup criteria is not null dependent, it should get pushed.
     */
    @Test public void testPushMultiGroupJoinCriteria2() {
        String sql = "select bqt1.smalla.intkey, bqt1.smallb.intkey from bqt1.smalla right outer join (bqt1.smallb cross join (bqt1.mediuma cross join bqt1.mediumb)) on bqt1.smalla.stringkey = bqt1.smallb.stringkey" //$NON-NLS-1$
            +" where bqt1.smalla.intkey + bqt1.mediuma.intkey + bqt1.mediumb.intkey = 1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                                    new String[] {"SELECT g_3.intkey, g_2.intkey FROM bqt1.mediuma AS g_0, bqt1.mediumb AS g_1, bqt1.smallb AS g_2, bqt1.smalla AS g_3 WHERE (g_3.stringkey = g_2.stringkey) AND (((g_3.intkey + g_0.intkey) + g_1.intkey) = 1)"}, true); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }


    /**
     * Having criteria should not be considered as regular criteria (unless it contains no aggregate expressions).
     */
    @Test public void testHavingCriteriaNotUsedAsJoinCriteria() {
        String sql = "select bqt1.smalla.intkey, max(bqt1.smallb.intkey) from bqt1.smalla, bqt1.smallb where bqt1.smalla.intkey = bqt1.smallb.intnum group by bqt1.smallb.intkey, bqt1.smalla.intkey having max(bqt1.smallb.intkey) = bqt1.smalla.intkey"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_HAVING, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                                    new String[] {"SELECT bqt1.smalla.intkey, MAX(bqt1.smallb.intkey) FROM bqt1.smalla, bqt1.smallb WHERE bqt1.smalla.intkey = bqt1.smallb.intnum GROUP BY bqt1.smallb.intkey, bqt1.smalla.intkey HAVING MAX(bqt1.smallb.intkey) = bqt1.smalla.intkey"}, true); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Ensure that subqueries not initially pushable to the source still get replaced
     */
    @Test public void testSubqueryReplacement() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan("select Y.e1, Y.e2 FROM vm1.g1 X left outer join vm1.g1 Y on Y.e1 = X.e1 where Y.e3 in (select e3 FROM vm1.g1) or Y.e3 IS NULL", metadata, null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 AS g1__1 LEFT OUTER JOIN pm1.g1 ON pm1.g1.e1 = g1__1.e1 WHERE (pm1.g1.e3 IN (SELECT pm1.g1.e3 FROM pm1.g1)) OR (pm1.g1.e3 IS NULL)"}, true); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testRulePushNonJoinCriteriaPreservesOuterJoin() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        String sql = "select b.intkey from (select intkey from bqt1.smalla) a left outer join (select intkey from bqt1.smallb) b on (1 = 1)"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
                                                    new String[]{"SELECT g_1.IntKey FROM BQT1.SmallA AS g_0 LEFT OUTER JOIN BQT1.SmallB AS g_1 ON 1 = 1"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOuterToInnerJoinConversion() {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        String sql = "select bqt1.smalla.intkey from bqt1.smalla left outer join bqt1.smallb on (bqt1.smalla.intkey = bqt1.smallb.intkey) where bqt1.smallb.intnum = 1"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, metadata, new String[]{"SELECT bqt1.smalla.intkey FROM bqt1.smalla, bqt1.smallb WHERE (bqt1.smalla.intkey = bqt1.smallb.intkey) AND (bqt1.smallb.intnum = 1)"}); //$NON-NLS-1$
    }

    //same as above, but with a right outer join
    @Test public void testOuterToInnerJoinConversion1() {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        String sql = "select bqt1.smalla.intkey from bqt1.smalla right outer join bqt1.smallb on (bqt1.smalla.intkey = bqt1.smallb.intkey) where bqt1.smalla.intnum = 1"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, metadata, new String[]{"SELECT bqt1.smalla.intkey FROM bqt1.smallb, bqt1.smalla WHERE (bqt1.smalla.intkey = bqt1.smallb.intkey) AND (bqt1.smalla.intnum = 1)"}); //$NON-NLS-1$
    }

    @Test public void testOuterToInnerJoinConversion2() {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        String sql = "select bqt1.smalla.intkey from bqt1.smalla full outer join bqt1.smallb on (bqt1.smalla.intkey = bqt1.smallb.intkey) where bqt1.smallb.intnum = 1"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, metadata, new String[]{"SELECT bqt1.smalla.intkey FROM bqt1.smallb LEFT OUTER JOIN bqt1.smalla ON bqt1.smalla.intkey = bqt1.smallb.intkey WHERE bqt1.smallb.intnum = 1"}); //$NON-NLS-1$
    }

    @Test public void testOuterToInnerJoinConversion3() {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        String sql = "select bqt1.smalla.intkey from bqt1.smalla full outer join bqt1.smallb on (bqt1.smalla.intkey = bqt1.smallb.intkey) where bqt1.smalla.intnum = 1"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, metadata, new String[]{"SELECT bqt1.smalla.intkey FROM bqt1.smalla LEFT OUTER JOIN bqt1.smallb ON bqt1.smalla.intkey = bqt1.smallb.intkey WHERE bqt1.smalla.intnum = 1"}); //$NON-NLS-1$
    }

    /**
     * non-dependent criteria on each side of a full outer creates an inner join
     */
    @Test public void testOuterToInnerJoinConversion4() {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        String sql = "select bqt1.smalla.intkey from bqt1.smalla full outer join bqt1.smallb on (bqt1.smalla.intkey = bqt1.smallb.intkey) where bqt1.smalla.intnum = bqt1.smallb.intnum"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, metadata, new String[]{"SELECT bqt1.smalla.intkey FROM bqt1.smalla, bqt1.smallb WHERE (bqt1.smalla.intkey = bqt1.smallb.intkey) AND (bqt1.smalla.intnum = bqt1.smallb.intnum)"}); //$NON-NLS-1$
    }

    /**
     * Since concat2 is null dependent the join will not be changed
     */
    @Test public void testOuterToInnerJoinConversionNullDependent() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setFunctionSupport("concat2", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        String sql = "select bqt1.smalla.intkey from bqt1.smalla left outer join bqt1.smallb on (bqt1.smalla.intkey = bqt1.smallb.intkey) where concat2(bqt1.smallb.intnum, '1') = 1"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, metadata, null, capFinder, new String[]{"SELECT bqt1.smallb.intnum, bqt1.smalla.intkey FROM bqt1.smalla LEFT OUTER JOIN bqt1.smallb ON bqt1.smalla.intkey = bqt1.smallb.intkey"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    }

    @Test public void testInlineViewToHaving() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_HAVING, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sql = "select x.y, x.intkey from (select max(intnum) y, intkey from bqt1.smalla group by intkey) x where x.y = 1"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                               new String[]{"SELECT MAX(intnum), intkey FROM bqt1.smalla GROUP BY intkey HAVING MAX(intnum) = 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * <p>In RuleBreakMultiJoin terminology:
     * If any of the regions contains a group with any unsatisfied access patterns, one
     * of those access patterns could be satisfied by arranging for a dependent join,
     * provided that group has join criteria which covers the column(s) in an access
     * pattern, and which joins the group to group(s) in other region(s).  The task, then,
     * is to ensure that an ordering isn't picked which makes such a dependent join
     * impossible.
     *
     * <p>A physical group can have zero or more access patterns; each access pattern can have one
     * or more columns.  So a group could implicitly be dependent on one or more other physical
     * groups in one or more other regions.  A table can be used to illustrate the potential
     * complexity of access patterns:
     * <pre>
     * Region with    | Target
     * Access Patterns| Regions
     * -------------------------
     * Reg3           | Reg1, Reg2
     * Reg3           | Reg4
     * Reg1           | Reg2
     * Reg4           | Reg3
     * </pre>
     *
     * This tests now passes with RulePlanJoins
     */
    @Test public void testPathologicalAccessPatternCaseCase2976Defect19018() throws Exception{
        TransformationMetadata metadata = RealMetadataFactory.example1();

        // add single access pattern to pm1.g4 containing elements e1, e2, and e3
        Table pm4g1 = metadata.getGroupID("pm4.g1");
        List<Column> cols = new ArrayList<Column>(pm4g1.getColumns());
        cols.remove(2);
        RealMetadataFactory.createKey(Type.AccessPattern, "pm4.g1.ap1", pm4g1, cols);

        String sql = "SELECT pm1.g1.e1, pm2.g1.e1, pm4.g1.e1 " +//$NON-NLS-1$
        "FROM pm1.g1, pm2.g1, pm4.g1 WHERE " +//$NON-NLS-1$
        "pm1.g1.e1 = pm4.g1.e1 AND pm2.g1.e2 = pm4.g1.e2 AND pm1.g1.e4 = pm2.g1.e4 " +//$NON-NLS-1$
        "AND pm4.g1.e4 = 3.2";//$NON-NLS-1$

        String[] expected = new String[] {"SELECT g_0.e4 AS c_0, g_0.e1 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", //$NON-NLS-1$
                                          "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm4.g1 AS g_0 WHERE (g_0.e4 = 3.2) AND (g_0.e1 IN (<dependent values>)) AND (g_0.e2 IN (<dependent values>)) ORDER BY c_0, c_1", //$NON-NLS-1$
                                          "SELECT g_0.e4 AS c_0, g_0.e2 AS c_1, g_0.e1 AS c_2 FROM pm2.g1 AS g_0 ORDER BY c_0",//$NON-NLS-1$
                                          };

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, expected, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        1,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        2,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });
    }

    /**
     * non-null dependent criteria should get pushed down
     */
    @Test public void testPushMultiGroupCriteriaOuterJoin() {
        String sql = "select m.intkey, m.intnum, s.intkey, s.intnum from BQT2.mediuma m left outer join BQT2.smalla s on m.intkey = s.intkey where not (m.intkey + s.intnum = 26)"; //$NON-NLS-1$

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
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {
                                          "SELECT m.intkey, m.intnum, s.intkey, s.intnum FROM BQT2.mediuma AS m, BQT2.smalla AS s WHERE (m.intkey = s.intkey) AND (NOT ((m.intkey + s.intnum) = 26))" }, //$NON-NLS-1$
                                          TestOptimizer.SHOULD_SUCCEED);

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

    }

    /**
     * Assumes that pm1.g1 is the only inner group
     */
    private void helpTestNullDependentVisitor(String critSQL,
                                              boolean dependent) throws QueryParserException,
                                                                QueryResolverException,
                                                                QueryMetadataException,
                                                                TeiidComponentException {
        List<GroupSymbol> innerGroups = new ArrayList<GroupSymbol>();
        innerGroups.add(new GroupSymbol("pm1.g1")); //$NON-NLS-1$

        Criteria crit = QueryParser.getQueryParser().parseCriteria(critSQL);

        QueryResolver.resolveCriteria(crit, RealMetadataFactory.example1Cached());

        assertEquals(dependent, JoinUtil.isNullDependent(RealMetadataFactory.example1Cached(), innerGroups, crit));
    }

    private void helpTestNullDependent(String expressionSQL,
                                              boolean dependent) throws QueryParserException,
                                                                QueryResolverException,
                                                                QueryMetadataException,
                                                                TeiidComponentException {
        List<GroupSymbol> innerGroups = new ArrayList<GroupSymbol>();
        innerGroups.add(new GroupSymbol("pm1.g1")); //$NON-NLS-1$

        Expression expr = QueryParser.getQueryParser().parseExpression(expressionSQL);

        ResolverVisitor.resolveLanguageObject(expr, RealMetadataFactory.example1Cached());

        assertEquals(dependent, JoinUtil.isNullDependent(RealMetadataFactory.example1Cached(), innerGroups, expr));
    }

    @Test public void testNullDependentVisitor() throws Exception {
        helpTestNullDependentVisitor("nvl(pm1.g1.e1, 1) = 1", true); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor1() throws Exception {
        helpTestNullDependentVisitor("ifnull(pm1.g1.e1, 1) = 1", true); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor2() throws Exception {
        helpTestNullDependentVisitor("rand(pm1.g1.e2) = 1", true); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor3() throws Exception {
        helpTestNullDependentVisitor("concat2(pm1.g1.e1, pm1.g1.e2) = '1'", false); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor4() throws Exception {
        helpTestNullDependentVisitor("nvl(pm1.g2.e1, 1) = 1", true); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor5() throws Exception {
        helpTestNullDependentVisitor("pm1.g1.e1 is null", true); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor6() throws Exception {
        helpTestNullDependentVisitor("pm1.g1.e1 is not null", false); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor7() throws Exception {
        helpTestNullDependentVisitor("pm1.g2.e1 is not null", true); //$NON-NLS-1$
    }

    //this is an important test, the or causes this criteria to be null dependent
    @Test public void testNullDependentVisitor8() throws Exception {
        helpTestNullDependentVisitor("pm1.g1.e1 = 1 or pm1.g2.e1 = 1", true); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor9() throws Exception {
        helpTestNullDependentVisitor("pm1.g1.e1 = 1 or pm1.g1.e2 = 2", false); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor10() throws Exception {
        helpTestNullDependentVisitor("pm1.g1.e1 in (1, pm1.g2.e1)", false); //$NON-NLS-1$
    }

    @Test public void testNullDependentVisitor11() throws Exception {
        helpTestNullDependentVisitor("pm1.g2.e1 in (1, pm1.g1.e1)", true); //$NON-NLS-1$
    }

    @Test public void testIsNullDependent() throws Exception {
        helpTestNullDependent("pm1.g1.e2 + 1", false); //$NON-NLS-1$
    }

    @Test public void testIsNullDependent1() throws Exception {
        helpTestNullDependent("pm1.g2.e2 + 1", true); //$NON-NLS-1$
    }

    /**
     *  The criteria will still get pushed to appropriate location, and
     *  the other side of the join will be removed
     */
    @Test public void testCriteriaPushedWithUnionJoin() throws Exception {
        String sql = "select * from pm1.g1 union join pm1.g2 where g1.e1 = 1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                      null, capFinder,
                                      new String[] {
                                          "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE pm1.g1.e1 = '1'" }, //$NON-NLS-1$
                                          TestOptimizer.SHOULD_SUCCEED);

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
        });

    }

    /**
     * union joins allow RuleRemoveVirtual to still take effect
     */
    @Test public void testCriteriaPushedWithUnionJoin1() throws Exception {
        String sql = "select vm1.g1.e1 from vm1.g1 union join vm1.g2 where g2.e1 = 1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
                                      null, capFinder,
                                      new String[] {
                                          "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = '1')" }, //$NON-NLS-1$
                                          ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
        });

    }

    /**
     * null-dependent expressions should prevent merging of virtual layers
     */
    @Test public void testNullDependentPreventsMerge() throws Exception {
        String sql = "select x from pm1.g1 left outer join (select nvl(e2, 1) x from pm1.g2) y on e2 = x"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setFunctionSupport(SourceSystemFunctions.IFNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
                                      null, capFinder,
                                      new String[] {
                                          "SELECT v_0.c_0 FROM pm1.g1 AS g_0 LEFT OUTER JOIN (SELECT ifnull(g_1.e2, 1) AS c_0 FROM pm1.g2 AS g_1) AS v_0 ON g_0.e2 = v_0.c_0" }, //$NON-NLS-1$
                                          TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

    }

    @Test public void testPreserveHint() throws Exception {
        String sql = "select pm1.g1.e1 from /*+ preserve */ (pm1.g1 left outer join pm1.g2 on g1.e2 = g2.e2) where pm1.g2.e1 = 'a'"; //$NON-NLS-1$
        assertEquals("SELECT pm1.g1.e1 FROM /*+ PRESERVE */ (pm1.g1 LEFT OUTER JOIN pm1.g2 ON g1.e2 = g2.e2) WHERE pm1.g2.e1 = 'a'", QueryParser.getQueryParser().parseCommand(sql).toString());
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
                                      null, TestOptimizer.getGenericFinder(true),
                                      new String[] {
                                          "SELECT g_0.e1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e2 = g_1.e2 WHERE g_1.e1 = 'a'" }, //$NON-NLS-1$
                                          TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * RuleCopyCriteria will remove the first join criteria and the source doesn't support the * function.  However we still
     * want the join to be pushed since it originally contained proper criteria.
     */
    @Test public void testCopyCriteriaJoinPushed() throws Exception {
        String sql = "select pm1.g1.e1 from pm1.g1, pm1.g2 where pm1.g1.e1 = pm1.g2.e1 and pm1.g1.e1 = 5 and pm1.g1.e2 * 5 = pm1.g2.e2"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,metadata,
                new String[] { "SELECT g_0.e2, g_1.e2, g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = '5') AND (g_1.e1 = '5')" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
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


    /**
     * Test for Case 836073:
     */
    @Test public void testForCase836073_1() {
        String sql = "select bqt1.smalla.intkey, bqt1.smallb.intkey from bqt1.smalla, bqt1.smallb WHERE formatdate(bqt1.smalla.DateValue,'yyyyMM') = '200309' AND bqt1.smalla.intkey = bqt1.smallb.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT bqt1.smalla.DateValue, bqt1.smalla.intkey, bqt1.smallb.intkey FROM bqt1.smalla, bqt1.smallb WHERE bqt1.smalla.intkey = bqt1.smallb.intkey"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testForCase836073_2() {
        String sql = "select bqt1.smalla.intkey, bqt1.smallb.intkey from bqt1.smalla left outer join bqt1.smallb on bqt1.smalla.intkey = bqt1.smallb.intkey WHERE formatdate(bqt1.smalla.DateValue,'yyyyMM') = '200309'"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT bqt1.smalla.DateValue, bqt1.smalla.intkey, bqt1.smallb.intkey FROM bqt1.smalla LEFT OUTER JOIN bqt1.smallb ON bqt1.smalla.intkey = bqt1.smallb.intkey"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Note that we don't allow pushdown here because the criteria placement matters
     */
    @Test public void testForCase836073_3() {
        String sql = "select bqt1.smalla.intkey, b.intkey from bqt1.smalla left outer join (select * from bqt1.smallb where formatdate(bqt1.smallb.DateValue,'yyyyMM') = '200309') b on bqt1.smalla.intkey = b.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.intkey AS c_0 FROM bqt1.smalla AS g_0 ORDER BY c_0", "SELECT g_0.DateValue AS c_0, g_0.IntKey AS c_1 FROM bqt1.smallb AS g_0 ORDER BY c_1"}); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testTransitiveJoinCondition() {
        String sql = "select b.intkey from bqt1.smalla a, bqt2.smallb b, bqt2.smalla b1 where a.intkey = b.intkey and a.intkey = b1.intkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {
            "SELECT g_1.intkey AS c_0, g_0.intkey AS c_1 FROM bqt2.smallb AS g_0, bqt2.smalla AS g_1 WHERE g_1.intkey = g_0.intkey ORDER BY c_0, c_1",
            "SELECT g_0.intkey AS c_0 FROM bqt1.smalla AS g_0 ORDER BY c_0"});

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testCrossJoinAvoidance() throws Exception {

        CapabilitiesFinder capFinder = TestOptimizer.getGenericFinder();

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("bqt1.smallb", 1800, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt1.smalla", -1, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt2.smallb", 15662, metadata); //$NON-NLS-1$

        TestOptimizer.helpPlan(
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallB, BQT1.Smalla, bqt2.smallb where bqt2.smallb.intkey = bqt1.smallb.intkey and bqt2.smallb.stringkey = bqt1.smalla.stringkey",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT g_0.IntKey, g_0.StringKey FROM BQT2.SmallB AS g_0 WHERE g_0.IntKey IN (<dependent values>)",
                    "SELECT g_0.StringKey AS c_0, g_0.IntKey AS c_1 FROM BQT1.SmallA AS g_0 ORDER BY c_0",
                    "SELECT g_0.IntKey AS c_0 FROM BQT1.SmallB AS g_0 ORDER BY c_0"},
            ComparisonMode.EXACT_COMMAND_STRING );

    }

    @Test public void testOuterJoinRemoval() throws Exception {
           BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
           ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * from pm1.g1 inner join (pm1.g2 left outer join pm1.g3 on pm1.g2.e1=pm1.g3.e1) on pm1.g1.e1=pm1.g3.e1", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g2 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g3 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        RelationalNode node = ((RelationalPlan)plan).getRootNode().getChildren()[0];
        assertTrue(node instanceof JoinNode);
        node = node.getChildren()[0];
        assertTrue(node instanceof JoinNode);
        assertEquals(JoinType.JOIN_INNER, ((JoinNode)node).getJoinType());
     }

    //doesn't modify the plan
    @Test public void testLeftOuterAssocitivtyNullDependent() throws Exception {
           BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
           TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 or pm2.g2.e1 is null left outer join pm2.g3 on pm2.g2.e2 = pm2.g3.e2", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_0.e2 AS c_0 FROM pm2.g3 AS g_0 ORDER BY c_0",
                       "SELECT g_0.e1, g_0.e2 FROM pm2.g2 AS g_0",
                       "SELECT g_0.e1, g_0.e3 FROM pm1.g1 AS g_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

    @Test public void testLeftOuterAssocitivtyLeftLinear() throws Exception {
           BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
           TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 left outer join pm2.g3 on pm2.g2.e2 = pm2.g3.e2", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0",
                       "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 LEFT OUTER JOIN pm2.g3 AS g_1 ON g_0.e2 = g_1.e2 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

    @Test public void testLeftOuterAssocitivtyLeftLinearWithPreserve() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        TestOptimizer.helpPlan("SELECT pm1.g1.e3 from /*+ preserve */ (pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 left outer join pm2.g3 on pm2.g2.e2 = pm2.g3.e2)", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0",
                    "SELECT g_0.e2 AS c_0 FROM pm2.g3 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

    @Test public void testLeftOuterAssocitivtyLeftLinearSwap() throws Exception {
           BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
           TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 left outer join pm1.g3 on pm1.g1.e2 = pm1.g3.e2", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0",
                       "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g3 AS g_1 ON g_0.e2 = g_1.e2 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

    @Test public void testLeftOuterAssocitivtyLeftLinearSwapNested() throws Exception {
           BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
           ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e3 from (pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 left outer join pm1.g3 on pm1.g1.e2 = pm1.g3.e2) inner join pm3.g1 on (pm1.g1.e1 = pm3.g1.e1)", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0",
                       "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g3 AS g_1 ON g_0.e2 = g_1.e2 ORDER BY c_0",
                       "SELECT g_0.e1 AS c_0 FROM pm3.g1 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

           HardcodedDataManager hdm = new HardcodedDataManager();
           hdm.addData("SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0", Arrays.asList("a"));
           hdm.addData("SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g3 AS g_1 ON g_0.e2 = g_1.e2 ORDER BY c_0", Arrays.asList("a", true));
           hdm.addData("SELECT g_0.e1 AS c_0 FROM pm3.g1 AS g_0 ORDER BY c_0", Arrays.asList("a"));
           TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(true)});
     }

    //doesn't modify the plan
    @Test public void testLeftOuterAssocitivtyLeftLinearInvalid() throws Exception {
           BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
           TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 left outer join pm2.g3 on pm1.g1.e2 = pm2.g3.e2", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0",
                       "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0",
                       "SELECT g_0.e2 AS c_0 FROM pm2.g3 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testLeftOuterAssocitivtyRightLinear() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
           TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join (pm1.g2 left outer join pm2.g3 on pm1.g2.e2 = pm2.g3.e2) on pm1.g1.e1 = pm1.g2.e1", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_1.e2 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e1 = g_1.e1 ORDER BY c_0",
                       "SELECT g_0.e2 AS c_0 FROM pm2.g3 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testLeftOuterAssocitivtyRightLinearMakeDep() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join (pm1.g2 left outer join /*+ MAKEDEP */ pm2.g3 on pm1.g2.e2 = pm2.g3.e2) on pm1.g1.e1 = pm1.g2.e1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e2 AS c_0 FROM pm2.g3 AS g_0 WHERE g_0.e2 IN (<dependent values>) ORDER BY c_0",
                    "SELECT g_1.e2 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e1 = g_1.e1 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testLeftOuterAssocitivtyRightLinearSwap() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
           caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
           TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join (pm2.g2 left outer join pm1.g3 on pm2.g2.e2 = pm1.g3.e2) on pm1.g1.e1 = pm1.g3.e1", //$NON-NLS-1$
                   RealMetadataFactory.example1Cached(),
                new String[] {
                       "SELECT g_0.e2 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0",
                       "SELECT g_1.e2 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g3 AS g_1 ON g_0.e1 = g_1.e1 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

    @Test public void testLeftOuterAssocitivtyRightLinearSwapMakeDep() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 left outer join (/*+ MAKEDEP */ pm2.g2 left outer join pm1.g3 on pm2.g2.e2 = pm1.g3.e2) on pm1.g1.e1 = pm1.g3.e1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e2 AS c_0 FROM pm2.g2 AS g_0 WHERE g_0.e2 IN (<dependent values>) ORDER BY c_0",
                    "SELECT g_1.e2 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g3 AS g_1 ON g_0.e1 = g_1.e1 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

    @Test public void testMergeJoinOrderNotPushed() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey "
                + "from bqt1.smalla inner join bqt2.smalla on (bqt2.smalla.stringkey = bqt1.smalla.stringkey)"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        bsc.setSourceProperty(Capability.COLLATION_LOCALE, "nowhere");
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0",
            "SELECT g_0.StringKey, g_0.IntKey FROM BQT2.SmallA AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0", Arrays.asList("b", 1), Arrays.asList("a", 3));
        hdm.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT2.SmallA AS g_0", Arrays.asList("c", 1), Arrays.asList("a", 2));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(3, 2)});
    }

    /**
     * Same as above but using the system/option property
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    @Test public void testMergeJoinOrderNotPushed1() throws Exception {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey "
                + "from bqt1.smalla inner join bqt2.smalla on (bqt2.smalla.stringkey = bqt1.smalla.stringkey)"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.getOptions().setAssumeMatchingCollation(false);

        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, RealMetadataFactory.exampleBQTCached()), RealMetadataFactory.exampleBQTCached(), new DefaultCapabilitiesFinder(bsc), cc);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0", Arrays.asList("b", 1), Arrays.asList("a", 3));
        hdm.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT2.SmallA AS g_0", Arrays.asList("c", 1), Arrays.asList("a", 2));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(3, 2)});
    }

    @Test public void testOutputColumnsWithMergeJoinAndNonPushedSelect() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey "
                + "from bqt1.smalla inner join bqt2.smalla on (bqt2.smalla.intkey = case when bqt1.smalla.intkey = 1 then 2 else 3 end) where right(bqt1.smalla.stringkey, 1) = 'a'"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.StringKey AS c_0, g_0.IntKey AS c_1, CASE WHEN g_0.IntKey = 1 THEN 2 ELSE 3 END AS c_2 FROM BQT1.SmallA AS g_0 ORDER BY c_2",
            "SELECT g_0.IntKey AS c_0 FROM BQT2.SmallA AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey AS c_0, g_0.IntKey AS c_1, CASE WHEN g_0.IntKey = 1 THEN 2 ELSE 3 END AS c_2 FROM BQT1.SmallA AS g_0 ORDER BY c_2", Arrays.asList("aa", 1, 2));
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT2.SmallA AS g_0 ORDER BY c_0", Arrays.asList(1));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {});
    }

    @Test public void testOutputColumnsWithMergeJoinAndNonPushedSelect1() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey "
                + "from bqt1.smalla inner join bqt2.smalla on (bqt2.smalla.intkey = case when bqt1.smalla.intkey = 1 then 2 else 3 end) where right(bqt1.smalla.stringkey, 1) = 'a'"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey FROM BQT2.SmallA AS g_0", "SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0", Arrays.asList("aa", 1));
        hdm.addData("SELECT g_0.IntKey FROM BQT2.SmallA AS g_0", Arrays.asList(1));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {});
    }

    @Test public void testLateralPushdown() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select bqt1.smallb.intkey, x.stringkey, x.intkey "
                + "from bqt1.smallb left outer join lateral (select bqt1.smalla.intkey, bqt1.smalla.stringkey from bqt1.smalla where bqt1.smalla.intnum = bqt1.smallb.intnum order by bqt1.smalla.intkey limit 1) as x on true"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_LATERAL, true);
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey, v_0.c_0, v_0.c_1 FROM BQT1.SmallB AS g_0 LEFT OUTER JOIN LATERAL(SELECT g_1.StringKey AS c_0, g_1.IntKey AS c_1 FROM BQT1.SmallA AS g_1 WHERE g_1.IntNum = g_0.IntNum ORDER BY c_1 LIMIT 1) AS v_0 ON 1 = 1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.exampleBQTCached());
        hdm.addData("SELECT g_0.IntKey, v_0.c_0, v_0.c_1 FROM SmallB AS g_0 LEFT OUTER JOIN LATERAL (SELECT g_1.StringKey AS c_0, g_1.IntKey AS c_1 FROM SmallA AS g_1 WHERE g_1.IntNum = g_0.IntNum ORDER BY c_1 LIMIT 1) AS v_0 ON 1 = 1");

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {});
    }

    @Test public void testLateralPushdownCondition() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select bqt1.smallb.intkey, x.stringkey, x.intkey "
                + "from bqt1.smallb left outer join lateral (select bqt1.smalla.intkey, bqt1.smalla.stringkey from bqt1.smalla where bqt1.smalla.intnum = bqt1.smallb.intnum order by bqt1.smalla.intkey limit 1) as x on (bqt1.smallb.intkey = 1)"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_LATERAL, true);
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.StringKey AS c_0, g_0.IntKey AS c_1 FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = BQT1.SmallB.IntNum ORDER BY c_1 LIMIT 1", "SELECT g_0.IntKey, g_0.IntNum FROM BQT1.SmallB AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.exampleBQTCached());
        hdm.addData("SELECT g_0.IntKey, g_0.IntNum FROM SmallB AS g_0", Arrays.asList(1, 2));
        hdm.addData("SELECT g_0.StringKey AS c_0, g_0.IntKey AS c_1 FROM SmallA AS g_0 WHERE g_0.IntNum = 2 ORDER BY c_1 LIMIT 1", Arrays.asList("a", 2));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, "a", 2)});

        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_LATERAL_CONDITION, true);

        plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                new String[] {"SELECT g_0.IntKey, v_0.c_0, v_0.c_1 FROM BQT1.SmallB AS g_0 LEFT OUTER JOIN LATERAL(SELECT g_1.StringKey AS c_0, g_1.IntKey AS c_1 FROM BQT1.SmallA AS g_1 WHERE g_1.IntNum = g_0.IntNum ORDER BY c_1 LIMIT 1) AS v_0 ON g_0.IntKey = 1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        hdm.clearData();
        hdm.addData("SELECT g_0.IntKey, v_0.c_0, v_0.c_1 FROM SmallB AS g_0 LEFT OUTER JOIN LATERAL (SELECT g_1.StringKey AS c_0, g_1.IntKey AS c_1 FROM SmallA AS g_1 WHERE g_1.IntNum = g_0.IntNum ORDER BY c_1 LIMIT 1) AS v_0 ON g_0.IntKey = 1");

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {});
    }

    @Test public void testLateralProcedurePushdown() throws Exception {
        String sql = "select smallb.intkey, x.stringkey, x.intkey "
                + "from smallb left outer join lateral (exec spTest5(smallb.intkey)) as x on (true)"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_LATERAL, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_PROCEDURE_TABLE, true);
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        bsc.setCapabilitySupport(Capability.QUERY_ONLY_FROM_JOIN_LATERAL_PROCEDURE, true);

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table smallb (intkey integer, stringkey string); "
                + "create foreign procedure spTest5 (param integer) returns table(stringkey string, intkey integer)", "x", "y");
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
                new String[] {"SELECT g_0.intkey, v_0.stringkey, v_0.intkey FROM y.smallb AS g_0 LEFT OUTER JOIN LATERAL(EXEC spTest5(g_0.intkey)) AS v_0 ON 1 = 1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        HardcodedDataManager hdm = new HardcodedDataManager(metadata);
        hdm.addData("SELECT g_0.intkey, v_0.stringkey, v_0.intkey FROM smallb AS g_0 LEFT OUTER JOIN LATERAL (EXEC spTest5(g_0.intkey)) AS v_0 ON 1 = 1", Arrays.asList(1, "2", 1));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] { Arrays.asList(1, "2", 1)});

        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);

        //with an extra inline view, should result in same plan - but is not currently as we can't remove the intermediate view without
        //a lot of work
        sql = "SELECT g_0.intkey, v_1.c_0, v_1.c_1 FROM y.smallb AS g_0 LEFT OUTER JOIN LATERAL(SELECT v_0.stringkey AS c_0, v_0.intkey AS c_1 FROM (EXEC spTest5(g_0.intkey)) AS v_0 limit 1) AS v_1 ON 1 = 1";

        plan = TestOptimizer.helpPlan(sql, metadata,
                new String[] {"SELECT g_0.intkey FROM y.smallb AS g_0", "EXEC spTest5(g_0.intkey)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        hdm.addData("SELECT g_0.intkey FROM smallb AS g_0", Arrays.asList(1));
        hdm.addData("EXEC spTest5(1)", Arrays.asList("2", 1));

        TestProcessor.helpProcess(plan, hdm, new List<?>[] { Arrays.asList(1, "2", 1)});
    }

    @Test public void testLateralOnlyProcedurePushdown() throws Exception {
        String sql = "select smallb.intkey, x.stringkey, x.intkey "
                + "from smallb left outer join lateral (select * from smalla where intkey = smallb.intkey) as x on (true)"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_LATERAL, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_PROCEDURE_TABLE, true);
        bsc.setCapabilitySupport(Capability.QUERY_ONLY_FROM_JOIN_LATERAL_PROCEDURE, true);

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table smallb (intkey integer, stringkey string); "
                + "create foreign table smalla (intkey integer, stringkey string);", "x", "y");
        TestOptimizer.helpPlan(sql, metadata,
                new String[] {"SELECT g_0.stringkey, g_0.intkey FROM y.smalla AS g_0 WHERE g_0.intkey = y.smallb.intkey", "SELECT g_0.intkey FROM y.smallb AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        bsc.setCapabilitySupport(Capability.QUERY_ONLY_FROM_JOIN_LATERAL_PROCEDURE, false);

        TestOptimizer.helpPlan(sql, metadata,
                new String[] {"SELECT g_0.intkey, v_0.c_0, v_0.c_1 FROM y.smallb AS g_0 LEFT OUTER JOIN LATERAL(SELECT g_1.stringkey AS c_0, g_1.intkey AS c_1 FROM y.smalla AS g_1 WHERE g_1.intkey = g_0.intkey) AS v_0 ON 1 = 1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDistinctDetectionWithUnionAll() throws Exception {
        String sql = "select avg(t1.a) from (select 3 as a, 3 as b union all "
                + "select 1 as a, 1 as b union all select 3 as a, 3 as b) as t1 "
                + "join (select 1 as a, 1 as b union all select 1 as a, 1 as b union all "
                + "select 2 as a, 2 as b union all select 2 as a, 2 as b union all "
                + "select 3 as a, 3 as b union all select 3 as a, 3 as b) as t2 on t1.a=t2.a";

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager hdm = new HardcodedDataManager();
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, new List<?>[] { Arrays.asList(FunctionMethods.divide(BigDecimal.valueOf(14), BigDecimal.valueOf(6))) });
    }

    @Test public void testDistinctDetectionWithUnion() throws Exception {
        String sql = "select avg(t1.a) from (select 3 as a, 3 as b union "
                + "select 1 as a, 1 as b union select 3 as a, 3 as b) as t1 "
                + "join (select 1 as a, 1 as b union all select 1 as a, 1 as b union all "
                + "select 2 as a, 2 as b union all select 2 as a, 2 as b union all "
                + "select 3 as a, 3 as b union all select 3 as a, 3 as b) as t2 on t1.a=t2.a";

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager hdm = new HardcodedDataManager();
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, new List<?>[] { Arrays.asList(BigDecimal.valueOf(2)) });
    }

    @Test public void testEnhancedJoinWithLeftDuplicates() throws Exception {
        String sql = "select t2.*, t1.* from (select 3 as a, 3 as b union all select 1 as a, 1 as b union all select 3 as a, 3 as b) as t1 join test_a t2 on t1.a=t2.a limit 10";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE foreign TABLE test_a (  a integer,  b integer );", "x", "y");
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.a AS c_0, g_0.b AS c_1 FROM y.test_a AS g_0 WHERE g_0.a IN (1, 3) ORDER BY c_0", Arrays.asList(1, 1), Arrays.asList(1, 2), Arrays.asList(3, 2), Arrays.asList(3, 10));
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, TestOptimizer.getGenericFinder());

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, new List<?>[] { Arrays.asList(1, 1, 1, 1), Arrays.asList(1, 2, 1, 1),
            Arrays.asList(3, 2, 3, 3), Arrays.asList(3, 10, 3, 3),
            Arrays.asList(3, 2, 3, 3), Arrays.asList(3, 10, 3, 3) });
    }

    @Test(expected=TeiidComponentException.class) public void testDetectInvalidSort() throws Exception {
        String sql = "select * from (with a (x, y, z) as /*+ no_inline */ (select e1, e2, e3 from pm1.g1) SELECT pm1.g2.e2, a.x, z from pm1.g2, a where e1 = x order by x) as x where z = 1"; //$NON-NLS-1$

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        //we're allowing the sort to be pushed, but it's not honored by FakeDataManager
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(TestOptimizer.getTypicalCapabilities()),
                new String[] {"SELECT a.x, a.z FROM a WHERE a.z = TRUE", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);

        helpProcess(plan, createCommandContext(), dataManager, null);
    }

    @Test public void testCopyCriteriaMultiway() throws Exception {
        String sql = "select bqt1.smalla.intkey, bqt2.smalla.intkey from bqt1.smalla, bqt2.smalla, bqt1.smallb where bqt1.smalla.intnum = bqt2.smalla.intnum and cast(bqt1.smalla.stringkey as integer) = coalesce(bqt2.smalla.intkey, bqt1.smallb.intkey) and bqt2.smalla.intkey = 1"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.IntKey FROM BQT1.SmallB AS g_0", "SELECT g_0.IntNum AS c_0, g_0.IntKey AS c_1 FROM BQT2.SmallA AS g_0 WHERE g_0.IntKey = 1 ORDER BY c_0", "SELECT g_0.IntNum AS c_0, g_0.StringKey AS c_1, g_0.IntKey AS c_2 FROM BQT1.SmallA AS g_0 ORDER BY c_0"});

        RelationalPlan relationalPlan = (RelationalPlan)plan;
        JoinNode joinNode = (JoinNode) relationalPlan.getRootNode().getChildren()[0];
        assertNotNull(joinNode.getJoinCriteria());
    }

    @Test public void testCrossSourceOuterWithOffset() throws Exception {
        String sql = "SELECT pm1.g1.e1, pm2.g1.e2 from pm1.g1 left outer join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 ORDER BY pm1.g1.e1 OFFSET 1 ROWS";

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        bsc.setCapabilitySupport(Capability.ROW_OFFSET, true);

        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);

        HardcodedDataManager hdm = new HardcodedDataManager();

        hdm.addData("SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", Arrays.asList("a"), Arrays.asList("b"));
        hdm.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_0", Arrays.asList("a", 1), Arrays.asList("b", 2));

        helpProcess(plan, createCommandContext(), hdm, new List[] {Arrays.asList("b", 2)});
    }

    @Test public void testInnerOuterOptimization() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select g4.*, g3.*, g2.*, g1.* from (pm2.g1 inner join pm1.g2 on (pm2.g1.e1 = pm1.g2.e1) inner join pm2.g3 on (pm1.g2.e2 = pm2.g3.e2)) left outer join pm1.g4 on (pm1.g2.e4 = pm1.g4.e4)"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();

        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e2 AS c_0, g_0.e1 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm2.g3 AS g_0 ORDER BY c_0",
            "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_1.e1 AS c_2, g_1.e3 AS c_3, g_0.e3 AS c_4, g_0.e4 AS c_5 FROM pm1.g2 AS g_0 LEFT OUTER JOIN pm1.g4 AS g_1 ON g_0.e4 = g_1.e4 ORDER BY c_0",
            "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm2.g1 AS g_0 ORDER BY c_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            2,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testInnerOuterOptimizationCrossJoin() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select pm1.g2.e2, pm1.g3.e2, pm1.g4.e1, pm1.g4.e3 from ((pm1.g2 cross join pm1.g3) inner join pm1.g1 on pm1.g1.e1 = pm1.g2.e1) left outer join pm1.g4 on (pm1.g2.e4 = pm1.g4.e4)"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();

        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        TransformationMetadata tm = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", 1, tm);
        RealMetadataFactory.setCardinality("pm1.g2", 1, tm);
        RealMetadataFactory.setCardinality("pm1.g3", 1, tm);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, new String[] {"SELECT g_0.e2, g_1.e2, g_3.e1, g_3.e3 FROM (pm1.g2 AS g_0 INNER JOIN (pm1.g3 AS g_1 CROSS JOIN pm1.g1 AS g_2) ON g_2.e1 = g_0.e1) LEFT OUTER JOIN pm1.g4 AS g_3 ON g_0.e4 = g_3.e4"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testKeyJoinOverConstrained() throws Exception {
        String sql = "select col1 from t1, t2 where t1.col = t2.col and t1.col = 1"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);

        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t1 (col integer primary key, col1 string); create foreign table t2 (col integer, foreign key (col) references t1 (col));", "x", "y");

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, new String[] {"SELECT g_0.col1 FROM y.t1 AS g_0, y.t2 AS g_1 WHERE (g_0.col = g_1.col) AND (g_0.col = 1)"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        sql = "select col1 from t1 left outer join t2 on t1.col = t2.col where t1.col = 1"; //$NON-NLS-1$

        plan = TestOptimizer.helpPlan(sql, tm, new String[] {"SELECT g_0.col1 FROM y.t1 AS g_0 LEFT OUTER JOIN y.t2 AS g_1 ON g_0.col = g_1.col WHERE g_0.col = 1"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLeftOuterAssocitivtyWithStarJoin() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        TestOptimizer.helpPlan("SELECT pm1.g1.e3 from pm1.g1 "
                + "left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 "
                + "inner join pm1.g3 on pm1.g1.e1 = pm1.g3.e1 "
                + "left outer join pm1.g2 on pm1.g1.e1 = pm1.g2.e1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM (pm1.g1 AS g_0 INNER JOIN pm1.g3 AS g_1 ON g_0.e1 = g_1.e1) LEFT OUTER JOIN pm1.g2 AS g_2 ON g_0.e1 = g_2.e1 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

    @Test public void testLeftOuterAssocitivtyWithStarJoinProjection() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);

        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g3.e1 from pm1.g1 "
                + "left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 "
                + "left outer join pm1.g3 on pm1.g1.e1 = pm1.g3.e1 "
                + "left outer join pm1.g2 on pm1.g1.e1 = pm1.g2.e1 "
                + "left outer join pm2.g1 on pm1.g1.e1 = pm2.g1.e1",
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e1 AS c_0, g_1.e1 AS c_1 FROM (pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g3 AS g_1 ON g_0.e1 = g_1.e1) LEFT OUTER JOIN pm1.g2 AS g_2 ON g_0.e1 = g_2.e1 ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager(false);

        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Collections.singletonList(null)});
     }

    @Test public void testLeftOuterAssocitivtyWithStarJoinWithPreserve() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        TestOptimizer.helpPlan("SELECT pm1.g1.e3 from /*+ preserve */ (pm1.g1 "
                + "left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 "
                + "inner join pm1.g3 on pm1.g1.e1 = pm1.g3.e1 "
                + "left outer join pm1.g2 on pm1.g1.e1 = pm1.g2.e1)", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0 FROM pm1.g3 AS g_0 ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

     @Test public void testLeftOuterAssocitivtyWithMakedep() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        TestOptimizer.helpPlan("SELECT pm1.g1.e3 from (pm1.g1 "
                + "left outer join /*+ MAKEDEP */ pm2.g2 on pm1.g1.e1 = pm2.g2.e1) "
                + "left outer join pm1.g2 on pm1.g1.e1 = pm1.g2.e1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0",
                    "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e1 = g_1.e1 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
     }

     @Test public void testLeftOuterAssocitivtyWithMakedep1() throws Exception {
         BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
         caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
         TestOptimizer.helpPlan("SELECT pm1.g1.e3 from (pm1.g1 "
                 + "left outer join /*+ MAKEDEP */ pm2.g2 on pm1.g1.e1 = pm2.g2.e1) "
                 + "left outer join pm1.g2 on pm2.g2.e1 = pm1.g2.e1", //$NON-NLS-1$
                 RealMetadataFactory.example1Cached(),
                 new String[] {
                     "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0",
                     "SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0",
                     "SELECT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
      }

    @Test
    public void testNonTerminatingLeftOuterJoinOptimization() throws Exception {
        String sql = "SELECT c._COMPTE,\n" +
                "       u.CalculatedField,\n" +
                "       CH_4.CH4__VALEUR,\n" +
                "       ch_41.CH41__VALEUR,\n" +
                "       ch_5.CH5__VALEUR\n" +
                "  FROM pm1.c\n" +
                "  LEFT JOIN pm2.u\n" +
                "    ON CAST(u.BAAFREP_AFAINB AS STRING) = c._USERNAME\n" +
                "  LEFT JOIN (SELECT ch._COMPTE AS CH4__COMPTE,\n" +
                "                    ch._VALEUR AS CH4__VALEUR\n" +
                "               FROM pm1.ch\n" +
                "              WHERE ch._NUMCHAMPS = 4) CH_4\n" +
                "    ON c._COMPTE = CH_4.CH4__COMPTE\n" +
                "  LEFT JOIN (SELECT ch._COMPTE AS CH41__COMPTE,\n" +
                "                    ch._VALEUR AS CH41__VALEUR\n" +
                "               FROM pm1.ch\n" +
                "              WHERE ch._NUMCHAMPS = 41) ch_41\n" +
                "    ON c._COMPTE = ch_41.CH41__COMPTE\n" +
                "  LEFT JOIN (SELECT ch._COMPTE AS CH5__COMPTE,\n" +
                "                    ch._VALEUR AS CH5__VALEUR\n" +
                "               FROM pm1.ch\n" +
                "              WHERE ch._NUMCHAMPS = 5) ch_5\n" +
                "    ON c._COMPTE = ch_5.CH5__COMPTE LIMIT 0, 10";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        TestOptimizer.helpPlan(sql, RealMetadataFactory.fromDDL("x",
                new RealMetadataFactory.DDLHolder("pm1",
                        "create foreign table c (_COMPTE long, _USERNAME string); "
                                + "create foreign table ch (_VALEUR string, _COMPTE long, _NUMCHAMPS long);"),
                new RealMetadataFactory.DDLHolder("pm2",
                        "create foreign table u (BAAFREP_AFAINB long, FREP_AFAINB string, CalculatedField6 long, CalculatedField long); "
                                + "create foreign table cl (CHAMPSFCP__COMPTE long, CHAMPSFCP__VALEUR long)")),
                new String[] {
                        "SELECT g_0.\"_COMPTE\" AS c_0, g_0.\"_VALEUR\" AS c_1 FROM pm1.ch AS g_0 WHERE (g_0.\"_NUMCHAMPS\" = 5) AND (g_0.\"_COMPTE\" IN (<dependent values>)) ORDER BY c_0",
                        "SELECT g_0.\"_COMPTE\" AS c_0, g_0.\"_USERNAME\" AS c_1, g_1.\"_VALEUR\" AS c_2 FROM pm1.c AS g_0 LEFT OUTER JOIN pm1.ch AS g_1 ON g_0.\"_COMPTE\" = g_1.\"_COMPTE\" AND g_1.\"_NUMCHAMPS\" = 4 ORDER BY c_0",
                        "SELECT g_0.BAAFREP_AFAINB AS c_0, g_0.CalculatedField AS c_1, convert(g_0.BAAFREP_AFAINB, STRING) AS c_2 FROM pm2.u AS g_0 WHERE convert(g_0.BAAFREP_AFAINB, STRING) IN (<dependent values>) ORDER BY c_2",
                        "SELECT g_0.\"_COMPTE\" AS c_0, g_0.\"_VALEUR\" AS c_1 FROM pm1.ch AS g_0 WHERE (g_0.\"_NUMCHAMPS\" = 41) AND (g_0.\"_COMPTE\" IN (<dependent values>)) ORDER BY c_0" }, //$NON-NLS-1$
                new DefaultCapabilitiesFinder(caps),
                ComparisonMode.EXACT_COMMAND_STRING);

    }

    @Test public void testOptionalClone() throws Exception {
        String sql = "SELECT \n" +
                "    subquery_a.a, subquery_b.a, subquery_b.b\n" +
                "FROM (SELECT 'val' AS a) AS subquery_a\n" +
                "LEFT JOIN (\n" +
                "    SELECT \n" +
                "        subquery_inner_a.medium || subquery_inner_b.proc_res AS a,\n" +
                "        1 AS b\n" +
                "    FROM (\n" +
                "        SELECT 'medium' AS medium) AS subquery_inner_a\n" +
                "        , TABLE(CALL pr(subquery_inner_a.medium)) AS subquery_inner_b\n" +
                "--      , TABLE(SELECT 'some_value' proc_res) AS subquery_inner_b\n" +
                ") AS subquery_b \n" +
                "    ON subquery_b.a = subquery_a.a\n" +
                "    AND subquery_a.a = 'val' ;";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE PROCEDURE pr(str string) RETURNS (proc_res string) AS\n" +
                "    BEGIN\n" +
                "        SELECT 'some_value';\n" +
                "    END", "x", "y");
        HardcodedDataManager hdm = new HardcodedDataManager();
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, TestOptimizer.getGenericFinder());

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, new List<?>[] { Arrays.asList("val", null, null) });
    }

}
