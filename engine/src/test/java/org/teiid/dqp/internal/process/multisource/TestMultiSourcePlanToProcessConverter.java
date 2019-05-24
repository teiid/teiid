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

package org.teiid.dqp.internal.process.multisource;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.id.IDGenerator;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempCapabilitiesFinder;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TestAggregatePushdown;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.SourceSystemFunctions;

/**
 * It's important here that the MultiSourceCapabilityFinder is used since some capabilities
 * will never be pushed to the source
 *
 * @since 4.2
 */
@SuppressWarnings("nls")
public class TestMultiSourcePlanToProcessConverter {

    private final class MultiSourceDataManager extends HardcodedDataManager {

        public MultiSourceDataManager() {
            setMustRegisterCommands(false);
        }

        public TupleSource registerRequest(CommandContext context, Command command, String modelName, RegisterRequestParameter parameterObject) throws org.teiid.core.TeiidComponentException {
            assertNotNull(parameterObject.connectorBindingId);

            Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(command, true, true);

            for (ElementSymbol symbol : elements) {
                if (symbol.getMetadataID() instanceof MultiSourceElement) {
                    fail("Query Contains a MultiSourceElement -- MultiSource expansion did not happen"); //$NON-NLS-1$
                }
            }
            return super.registerRequest(context, command, modelName, parameterObject);
        }
    }

    private static final boolean DEBUG = false;

    public ProcessorPlan helpTestMultiSourcePlan(QueryMetadataInterface metadata, String userSql, String multiModel, int sourceCount, ProcessorDataManager dataMgr, List<?>[] expectedResults, VDBMetaData vdb) throws Exception {
        return helpTestMultiSourcePlan(metadata, userSql, multiModel, sourceCount, dataMgr, expectedResults, vdb, null, null);
    }


    public ProcessorPlan helpTestMultiSourcePlan(QueryMetadataInterface metadata, String userSql, String multiModel, int sourceCount, ProcessorDataManager dataMgr, List<?>[] expectedResults, VDBMetaData vdb, List<?> params, Options options) throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);

        bsc.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        return helpTestMultiSourcePlan(metadata, userSql, multiModel, sourceCount, dataMgr, expectedResults, vdb, params, options, bsc);
    }

    public ProcessorPlan helpTestMultiSourcePlan(QueryMetadataInterface metadata, String userSql, String multiModel, int sourceCount, ProcessorDataManager dataMgr, List<?>[] expectedResults, VDBMetaData vdb, List<?> params, Options options, SourceCapabilities bsc) throws Exception {
        Map<String, String> multiSourceModels = MultiSourceMetadataWrapper.getMultiSourceModels(vdb);
        for (String model:multiSourceModels.keySet()) {
            char sourceID = 'a';
            // by default every model has one binding associated, but for multi-source there were none assigned.
            ModelMetaData m = vdb.getModel(model);
            int x = m.getSourceNames().size();
            for(int i=x; i<sourceCount; i++, sourceID++) {
                 m.addSourceMapping("" + sourceID, "translator",  null); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        QueryMetadataInterface wrapper = new MultiSourceMetadataWrapper(metadata, multiSourceModels);
        wrapper = new TempMetadataAdapter(wrapper, new TempMetadataStore());
        DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(wrapper, vdb);

        AnalysisRecord analysis = new AnalysisRecord(DEBUG, DEBUG);

        Command command = TestResolver.helpResolve(userSql, wrapper);
        ValidatorReport report = Validator.validate(command, metadata);
        if (report.hasItems()) {
            fail(report.toString());
        }
        // Plan
        command = QueryRewriter.rewrite(command, wrapper, null);
        DefaultCapabilitiesFinder fakeFinder = new DefaultCapabilitiesFinder(bsc);

        CapabilitiesFinder finder = new TempCapabilitiesFinder(fakeFinder);
        IDGenerator idGenerator = new IDGenerator();

        CommandContext context = new CommandContext("test", "user", null, vdb.getName(), vdb.getVersion(), false); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        context.setDQPWorkContext(workContext);
        context.setOptions(options);
        ProcessorPlan plan = QueryOptimizer.optimizePlan(command, wrapper, idGenerator, finder, analysis, context);

        if(DEBUG) {
            System.out.println(analysis.getDebugLog());
            System.out.println("\nMultiSource Plan:"); //$NON-NLS-1$
            System.out.println(plan);

        }
        if (params != null) {
            TestProcessor.setParameterValues(params, command, context);
        }
        TestProcessor.helpProcess(plan, context, dataMgr, expectedResults);
        return plan;
    }

    @Test public void testNoReplacement() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE SOURCE_NAME = 'bogus'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected =
            new List[0];
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testSingleReplacement() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE SOURCE_NAME = 'a'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { null, null}) };
        final HardcodedDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(false);
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testSingleReplacementAltName() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE foo = 'a'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { null, null}) };
        final HardcodedDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(false);
        VDBMetaData vdb = RealMetadataFactory.exampleMultiBindingVDB();
        vdb.getModel("MultiModel").addProperty(MultiSourceMetadataWrapper.MULTISOURCE_COLUMN_NAME, "foo");
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, vdb);
    }

    @Test public void testPreparedReplacement() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys WHERE SOURCE_NAME = ?"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { null, null}) };
        final HardcodedDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(false);
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), Arrays.asList("a"), null);
    }

    @Test public void testMultiReplacement() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT * FROM MultiModel.Phys"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { null, null}),
                         Arrays.asList(new Object[] { null, null}),
                         Arrays.asList(new Object[] { null, null})};
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testMultiReplacementWithOrderBy() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT a, b, source_name || a FROM MultiModel.Phys order by a"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("e", "z", "be"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            Arrays.asList("f", "z", "bf"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            Arrays.asList("x", "z", "ax"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            Arrays.asList("y", "z", "ay"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a, g_0.b, concat('a', g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("y", "z", "ay"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                            Arrays.asList("x", "z", "ax")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        dataMgr.addData("SELECT g_0.a, g_0.b, concat('b', g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("e", "z", "be"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                            Arrays.asList("f", "z", "bf")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testNested() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "select max(source_name), a from (SELECT a, source_name FROM MultiModel.Phys union all select b, source_name from MultiModel.phys1) as x group by a"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
                Arrays.asList("b", 1),
            };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT v_0.c_0, MAX(v_0.c_1) FROM (SELECT g_1.a AS c_0, 'a' AS c_1 FROM MultiModel.Phys AS g_1 UNION ALL SELECT g_0.b AS c_0, 'a' AS c_1 FROM MultiModel.Phys1 AS g_0) AS v_0 GROUP BY v_0.c_0", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList(1, "a")});
        dataMgr.addData("SELECT v_0.c_0, MAX(v_0.c_1) FROM (SELECT g_1.a AS c_0, 'b' AS c_1 FROM MultiModel.Phys AS g_1 UNION ALL SELECT g_0.b AS c_0, 'b' AS c_1 FROM MultiModel.Phys1 AS g_0) AS v_0 GROUP BY v_0.c_0", //$NON-NLS-1$
                new List<?>[] {
                    Arrays.asList(1, "b")});

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        bsc.setCapabilitySupport(Capability.QUERY_UNION, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bsc.setFunctionSupport(SourceSystemFunctions.CONCAT, true);

        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, null, bsc);
    }

    @Test public void testMultiJoinImplicit() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT phys.a FROM MultiModel.Phys, MultiModel.phys1 where phys.a = phys1.b"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
                Arrays.asList("a"),
                Arrays.asList("a"),
            };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0, MultiModel.Phys1 AS g_1 WHERE g_0.a = g_1.b", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("a")});
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    /**
     * With implicit join turned off, we now get the join of the union of the source queries.
     */
    @Test public void testMultiJoinNotImplicit() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT phys.a FROM MultiModel.Phys, MultiModel.phys1 where phys.a = phys1.b"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
                Arrays.asList("a"),
                Arrays.asList("a"),
                Arrays.asList("a"),
                Arrays.asList("a"),
            };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List<?>[] {Arrays.asList("a")});
        dataMgr.addData("SELECT g_0.b FROM MultiModel.Phys1 AS g_0", //$NON-NLS-1$
                new List<?>[] {Arrays.asList("a")});
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, new Options().implicitMultiSourceJoin(false));
    }

    @Test public void testMultiJoinPartitionedExplicit() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT phys.a FROM MultiModel.Phys, MultiModel.phys1 where phys.a = phys1.b and Phys.source_name = phys1.source_name"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("a"),
            Arrays.asList("a"),
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0, MultiModel.Phys1 AS g_1 WHERE g_0.a = g_1.b", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("a")});
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, new Options().implicitMultiSourceJoin(false));
    }

    @Test public void testAggParitioned() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT max(phys.a), source_name FROM MultiModel.Phys group by Phys.source_name"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("y", "a"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            Arrays.asList("y", "b"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT MAX(g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("y")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcessorPlan plan = helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
        TestOptimizer.checkNodeTypes(plan, new int[] {
                3,      // Access
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
                1       // UnionAll
            });
    }

    @Test public void testAggNotParitioned() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT max(phys.a) FROM MultiModel.Phys"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("y"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT MAX(g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("y")});
        ProcessorPlan plan = helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
        TestOptimizer.checkNodeTypes(plan, new int[] {
                3,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                1       // UnionAll
            });
    }

    @Test public void testAggNotParitionedGroupBy() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT max(phys.a) FROM MultiModel.Phys group by b"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("y"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.b, MAX(g_0.a) FROM MultiModel.Phys AS g_0 GROUP BY g_0.b", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("x", "y")});
        ProcessorPlan plan = helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
        TestOptimizer.checkNodeTypes(plan, new int[] {
                3,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                1       // UnionAll
            });
    }

    @Test public void testGroupByNotParitioned() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT phys.a FROM MultiModel.Phys group by phys.a"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("y"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0 GROUP BY g_0.a", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("y")});
        ProcessorPlan plan = helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
        TestOptimizer.checkNodeTypes(plan, new int[] {
                3,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                1       // UnionAll
            });
    }

    @Test public void testMultiReplacementWithLimit() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT distinct a, b, source_name || a FROM MultiModel.Phys order by a limit 1"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("e", "z", "be"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a, g_0.b, concat('a', g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                new List<?>[] {
                    Arrays.asList("y", "z", "ay"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    Arrays.asList("x", "z", "ax")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        dataMgr.addData("SELECT g_0.a, g_0.b, concat('b', g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                new List<?>[] {
                    Arrays.asList("e", "z", "be"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    Arrays.asList("f", "z", "bf")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testMultiReplacementWithLimit1() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT a, b FROM MultiModel.Phys limit 1, 1"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("x", "z"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a AS c_0, g_0.b AS c_1 FROM MultiModel.Phys AS g_0 LIMIT 2", //$NON-NLS-1$
                new List<?>[] {
                    Arrays.asList("y", "z"), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    Arrays.asList("x", "z")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        RelationalPlan plan = (RelationalPlan)helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
        assertTrue(plan.getRootNode() instanceof LimitNode);
    }

    @Test public void testMultiReplacementWithProjectConstant() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "SELECT a, b, source_name || a, '1' FROM MultiModel.Phys"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("x", "z", "ax", "1"),
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a, g_0.b, concat('a', g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                new List<?>[] {
                    Arrays.asList("x", "z", "ax")}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        dataMgr.addData("SELECT g_0.a, g_0.b, concat('b', g_0.a) FROM MultiModel.Phys AS g_0", //$NON-NLS-1$
                new List<?>[] {});
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testMultiDependentJoin() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT a.a FROM MultiModel.Phys a inner join MultiModel.Phys b makedep on (a.a = b.a) order by a"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected =
            new List<?>[] { Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "x"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"}), //$NON-NLS-1$
                         Arrays.asList(new Object[] { "y"})}; //$NON-NLS-1$

        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0",  //$NON-NLS-1$
                        new List<?>[] { Arrays.asList(new Object[] { "x" }), //$NON-NLS-1$
                                     Arrays.asList(new Object[] { "y" })}); //$NON-NLS-1$
        dataMgr.addData("SELECT g_0.a FROM MultiModel.Phys AS g_0 WHERE g_0.a IN ('x', 'y')",  //$NON-NLS-1$
                        new List<?>[] { Arrays.asList(new Object[] { "x" }), //$NON-NLS-1$
                                     Arrays.asList(new Object[] { "y" })}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testSingleReplacementInDynamicCommand() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec Virt.sq1('a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List<?>[] { Arrays.asList(new Object[] { null, null}), };
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testSingleReplacementInDynamicCommandNullValue() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec Virt.sq1(null)"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List[0];
        final ProcessorDataManager dataMgr = new MultiSourceDataManager();
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testMultiUpdateAll() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "update MultiModel.Phys set a = '1' where b = 'z'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List<?>[] { Arrays.asList(3)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("UPDATE MultiModel.Phys SET a = '1' WHERE b = 'z'", new List<?>[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testInsertMatching() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "INSERT INTO MultiModel.Phys(a, SOURCE_NAME) VALUES ('a', 'a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List<?>[] { Arrays.asList(1)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("INSERT INTO MultiModel.Phys (a) VALUES ('a')", new List<?>[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testInsertNotMatching() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "INSERT INTO MultiModel.Phys(a, SOURCE_NAME) VALUES ('a', 'x')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List<?>[] { Arrays.asList(0)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testProcedure() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec MultiModel.proc('b', 'a')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List<?>[] { Arrays.asList(1)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("EXEC MultiModel.proc('b')", new List<?>[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testProcedureAll() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "exec MultiModel.proc(\"in\"=>'b')"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List<?>[] { Arrays.asList(1), Arrays.asList(1), Arrays.asList(1)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("EXEC MultiModel.proc('b')", new List<?>[] {Arrays.asList(1)}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testTempInsert() throws Exception {
        final QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();
        final String userSql = "INSERT INTO #x select * from MultiModel.Phys"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 3;
        final List<?>[] expected = new List<?>[] { Arrays.asList(3)};
        final MultiSourceDataManager dataMgr = new MultiSourceDataManager();
        dataMgr.setMustRegisterCommands(true);
        dataMgr.addData("SELECT g_0.a, g_0.b FROM MultiModel.Phys AS g_0", new List<?>[] {Arrays.asList("a", "b")}); //$NON-NLS-1$
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB());
    }

    @Test public void testUnsupportedPredicate() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT phys.a FROM MultiModel.Phys where Phys.source_name like 'a%'"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("a"),
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT MultiModel.Phys.a FROM MultiModel.Phys", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("a")});
        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, new Options().implicitMultiSourceJoin(false), new BasicSourceCapabilities());
        assertEquals(3, dataMgr.getCommandHistory().size());
    }

    @Test public void testCountStar() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT count(*) FROM MultiModel.Phys limit 100"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList(4),
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager(metadata);
        dataMgr.addData("SELECT COUNT(*) FROM Phys AS g_0", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList(2)});

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setFunctionSupport("ifnull", true);

        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, new Options().implicitMultiSourceJoin(false), bsc);
    }

    @Test public void testGroupByCountPushdownMultiSource() throws Exception {
        String userSql = "SELECT s.e1, m.e1, COUNT(*) FROM pm1.g1 AS s INNER JOIN pm2.g2 AS m ON s.e2 = m.e2 GROUP BY s.e1, m.e1";

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.e2, COUNT(*) FROM pm1.g1 AS g_0 GROUP BY g_0.e2",
                Arrays.asList(1, 2));
        hdm.addData("SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_0",
                Arrays.asList(1, "other"));

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();

        final List<?>[] expected = new List<?>[] {
                Arrays.asList("a", "other", 2),
                Arrays.asList("b", "other", 2),
            };

        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("exampleMultiBinding");
        vdb.setVersion(1);

        ModelMetaData model = new ModelMetaData();
        model.setName("pm1");
        model.setModelType(Model.Type.PHYSICAL);
        model.setVisible(true);

        model.setSupportsMultiSourceBindings(true);
        model.addProperty(MultiSourceMetadataWrapper.MULTISOURCE_COLUMN_NAME, "e1");
        vdb.addModel(model);

        helpTestMultiSourcePlan(metadata, userSql, "pm1", 2, hdm, expected, vdb, null, new Options().implicitMultiSourceJoin(false), bsc);
    }

    @Test public void testNonPartitionedDistinct() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT distinct a FROM MultiModel.Phys"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("a"),
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager(metadata);
        dataMgr.addData("SELECT DISTINCT g_0.a FROM Phys AS g_0", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("a")});

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);

        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, new Options().implicitMultiSourceJoin(false), bsc);
    }

    @Test public void testOtherPartitionedGroupBy() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT max(a) FROM MultiModel.Phys1 group by c"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("a"),
            Arrays.asList("a")
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager(metadata);
        dataMgr.addData("SELECT MAX(g_0.a) FROM Phys1 AS g_0 GROUP BY g_0.c", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("a")});

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();

        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, new Options().implicitMultiSourceJoin(false), bsc);
    }

    @Test public void testNotPartitionedGroupBy() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMultiBinding();

        final String userSql = "SELECT max(a) FROM MultiModel.Phys1 group by b"; //$NON-NLS-1$
        final String multiModel = "MultiModel"; //$NON-NLS-1$
        final int sources = 2;
        final List<?>[] expected = new List<?>[] {
            Arrays.asList("a")
        };
        final HardcodedDataManager dataMgr = new HardcodedDataManager(metadata);
        dataMgr.addData("SELECT g_0.b, MAX(g_0.a) FROM Phys1 AS g_0 GROUP BY g_0.b", //$NON-NLS-1$
                        new List<?>[] {
                            Arrays.asList("b", "a")});

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();

        helpTestMultiSourcePlan(metadata, userSql, multiModel, sources, dataMgr, expected, RealMetadataFactory.exampleMultiBindingVDB(), null, new Options().implicitMultiSourceJoin(false), bsc);
    }

}
