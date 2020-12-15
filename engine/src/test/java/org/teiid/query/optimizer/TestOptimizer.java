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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultTypeCodes;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.AliasGenerator;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.RuleChooseDependent;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.DependentAccessNode;
import org.teiid.query.processor.relational.DupRemoveNode;
import org.teiid.query.processor.relational.EnhancedSortMergeJoinStrategy;
import org.teiid.query.processor.relational.GroupingNode;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.JoinStrategy;
import org.teiid.query.processor.relational.MergeJoinStrategy;
import org.teiid.query.processor.relational.NestedLoopJoinStrategy;
import org.teiid.query.processor.relational.NestedTableJoinStrategy;
import org.teiid.query.processor.relational.NullNode;
import org.teiid.query.processor.relational.PlanExecutionNode;
import org.teiid.query.processor.relational.ProjectIntoNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.processor.relational.SelectNode;
import org.teiid.query.processor.relational.SortNode;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.processor.relational.UnionAllNode;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ExecutionFactory.TransactionSupport;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings({"nls"})
public class TestOptimizer {

    public interface DependentJoin {}
    public interface DependentSelectNode {}
    public interface SemiJoin {}
    public interface AntiSemiJoin {}
    public interface DependentProjectNode {}
    public interface DupRemoveSortNode {}

    public static final int[] FULL_PUSHDOWN = new int[] {
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
                                            0,      // Project
                                            0,      // Select
                                            0,      // Sort
                                            0       // UnionAll
                                        };

    public enum ComparisonMode { EXACT_COMMAND_STRING, CORRECTED_COMMAND_STRING, FAILED_PLANNING }

    public static final boolean SHOULD_SUCCEED = true;
    public static final boolean SHOULD_FAIL = false;

    // ################################## TEST HELPERS ################################

    public static BasicSourceCapabilities getTypicalCapabilities() {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.CRITERIA_BETWEEN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_OR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_UNRELATED, true);

        // set typical max set size
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        return caps;
    }

    public static CapabilitiesFinder getGenericFinder(boolean supportsJoins) {
        final BasicSourceCapabilities caps = getTypicalCapabilities();
        if (!supportsJoins) {
            caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
            caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
            caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        }
        return new DefaultCapabilitiesFinder(caps);
    }

    public static CapabilitiesFinder getGenericFinder() {
        return getGenericFinder(true);
    }

    public static ProcessorPlan helpPlan(String sql, QueryMetadataInterface md, String[] expectedAtomic) {
        return helpPlan(sql, md, null, getGenericFinder(), expectedAtomic, SHOULD_SUCCEED);
    }

    public static ProcessorPlan helpPlan(String sql,
            QueryMetadataInterface md, String[] expected,
            CapabilitiesFinder capFinder,
            ComparisonMode mode) throws TeiidComponentException, TeiidProcessingException {
        return helpPlan(sql, md, null, capFinder, expected, mode);
    }

    public static ProcessorPlan helpPlan(String sql, QueryMetadataInterface md, String[] expectedAtomic, ComparisonMode mode) throws TeiidComponentException, TeiidProcessingException {
        return helpPlan(sql, md, null, getGenericFinder(), expectedAtomic, mode);
    }

    public static ProcessorPlan helpPlan(String sql, QueryMetadataInterface md, List<String> bindings, CapabilitiesFinder capFinder, String[] expectedAtomic, boolean shouldSucceed) {
        Command command;
        try {
            command = helpGetCommand(sql, md);
        } catch (TeiidException err) {
            throw new TeiidRuntimeException(err);
        }

        return helpPlanCommand(command, md, capFinder, null, expectedAtomic, shouldSucceed ? ComparisonMode.CORRECTED_COMMAND_STRING : ComparisonMode.FAILED_PLANNING);
    }

    public static ProcessorPlan helpPlan(String sql, QueryMetadataInterface md, List<String> bindings, CapabilitiesFinder capFinder, String[] expectedAtomic, ComparisonMode mode) throws TeiidComponentException, TeiidProcessingException {
        Command command = helpGetCommand(sql, md);

        return helpPlanCommand(command, md, capFinder, null, expectedAtomic, mode);
    }


    public static Command helpGetCommand(String sql, QueryMetadataInterface md) throws TeiidComponentException, TeiidProcessingException {
        if(DEBUG) System.out.println("\n####################################\n" + sql);     //$NON-NLS-1$
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, md);

        ValidatorReport repo = Validator.validate(command, md);

        Collection<LanguageObject> failures = new ArrayList<LanguageObject>();
        repo.collectInvalidObjects(failures);
        if (failures.size() > 0){
            fail("Exception during validation (" + repo); //$NON-NLS-1$
        }

        // rewrite
        command = QueryRewriter.rewrite(command, md, new CommandContext());

        return command;
    }

    public static ProcessorPlan helpPlanCommand(Command command, QueryMetadataInterface md, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, String[] expectedAtomic, ComparisonMode mode) {
        if (capFinder == null){
            capFinder = getGenericFinder();
        }

        // Collect atomic queries
        ProcessorPlan plan = getPlan(command, md, capFinder, analysisRecord, mode != ComparisonMode.FAILED_PLANNING, new CommandContext());

        if (mode == ComparisonMode.CORRECTED_COMMAND_STRING) {
            checkAtomicQueries(expectedAtomic, plan, md, capFinder);
        } else if (mode == ComparisonMode.EXACT_COMMAND_STRING) {
            checkAtomicQueries(expectedAtomic, plan);
        }

        return plan;
    }

    public static void checkAtomicQueries(String[] expectedAtomic,
                                          ProcessorPlan plan) {
       Set<String> actualQueries = getAtomicQueries(plan);

       if (actualQueries.size() != 1 || expectedAtomic.length != 1) {
           // Compare atomic queries
           HashSet<String> expectedQueries = new HashSet<String>(Arrays.asList(expectedAtomic));
           assertEquals("Did not get expected atomic queries: ", expectedQueries, actualQueries); //$NON-NLS-1$
       } else {
           assertEquals("Did not get expected atomic query: ", expectedAtomic[0], actualQueries.iterator().next()); //$NON-NLS-1$
       }
   }

    public static void checkAtomicQueries(String[] expectedAtomic,
                                           ProcessorPlan plan, QueryMetadataInterface md, CapabilitiesFinder capFinder) {
        Set<String> actualQueries = getAtomicQueries(plan);

        HashSet<String> expectedQueries = new HashSet<String>();

        // Compare atomic queries
        for (int i = 0; i < expectedAtomic.length; i++) {
            final String sql = expectedAtomic[i];
            Command command;
            try {
                command = helpGetCommand(sql, md);
                Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroupsIgnoreInlineViews(command, false);
                final GroupSymbol symbol = groups.iterator().next();
                Object modelId = md.getModelID(symbol.getMetadataID());
                boolean supportsGroupAliases = CapabilitiesUtil.supportsGroupAliases(modelId, md, capFinder);
                boolean supportsProjection = CapabilitiesUtil.supports(Capability.QUERY_SELECT_EXPRESSION, modelId, md, capFinder);
                command.acceptVisitor(new AliasGenerator(supportsGroupAliases, !supportsProjection));
                expectedQueries.add(command.toString());
            } catch (Exception err) {
                throw new RuntimeException(err);
            }
        }

        assertEquals("Did not get expected atomic queries: ", expectedQueries, actualQueries); //$NON-NLS-1$
    }

    public static ProcessorPlan getPlan(Command command, QueryMetadataInterface md, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, boolean shouldSucceed, CommandContext cc) {
        ProcessorPlan plan = null;
        if (analysisRecord == null) {
            analysisRecord = new AnalysisRecord(false, DEBUG);
        }
        Exception exception = null;
        try {
            //do planning
            plan = QueryOptimizer.optimizePlan(command, md, null, capFinder, analysisRecord, cc);
        } catch (QueryPlannerException e) {
            exception = e;
        } catch (TeiidComponentException e) {
            exception = e;
        } catch (Throwable e) {
            throw new TeiidRuntimeException(e);
        } finally {
            if(DEBUG) {
                System.out.println(analysisRecord.getDebugLog());
            }
        }
        if (!shouldSucceed) {
            assertNotNull("Expected exception but did not get one.", exception); //$NON-NLS-1$
            return null;
        }
        if (plan == null) {
            throw new TeiidRuntimeException(exception);
        }
        assertNotNull("Output elements are null", plan.getOutputElements()); //$NON-NLS-1$
        if(DEBUG) System.out.println("\n" + plan);     //$NON-NLS-1$
        return plan;
    }

    public static Set<String> getAtomicQueries(ProcessorPlan plan) {
        Set<Command> atomicQueries = new HashSet<Command>();
        if(plan instanceof RelationalPlan) {
            getAtomicCommands( ((RelationalPlan)plan).getRootNode(), atomicQueries );
        }

        Set<String> stringQueries = new HashSet<String>();

        for (Command command : atomicQueries) {
           stringQueries.add(command.toString());
        }

        return stringQueries;
    }

    private static void getAtomicCommands(RelationalNode node, Set<Command> atomicQueries) {
        if(node instanceof AccessNode) {
            AccessNode accessNode = (AccessNode) node;
            atomicQueries.add( accessNode.getCommand());
        }

        // Recurse through children
        RelationalNode[] children = node.getChildren();
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                getAtomicCommands(children[i], atomicQueries);
            } else {
                break;
            }
        }
    }

    // Counts are (mostly) alphabetical:
    //   Access, DependentAccess, DependentSelect, DependentProject, DupRemove, Grouping, NestedLoopJoinStrategy, Null, PlanExecution, Project, Select, Sort, UnionAll
    private static final Class<?>[] COUNT_TYPES = new Class[] {
        AccessNode.class,
        DependentAccessNode.class,
        DependentSelectNode.class,
        DependentProjectNode.class,
        DupRemoveNode.class,
        GroupingNode.class,
        NestedLoopJoinStrategy.class,
        MergeJoinStrategy.class,
        NullNode.class,
        PlanExecutionNode.class,
        ProjectNode.class,
        SelectNode.class,
        SortNode.class,
        UnionAllNode.class
    };

    public static void checkNodeTypes(ProcessorPlan root, int[] expectedCounts) {
        checkNodeTypes(root, expectedCounts, COUNT_TYPES);
    }

    public static void checkNodeTypes(ProcessorPlan root, int[] expectedCounts, Class<?>[] types) {
        if(! (root instanceof RelationalPlan)) {
            return;
        }

        int[] actualCounts = new int[types.length];
        collectCounts(((RelationalPlan)root).getRootNode(), actualCounts, types);

        for(int i=0; i<expectedCounts.length; i++) {
            assertEquals("Did not find the correct number of nodes for type " + types[i], //$NON-NLS-1$
                        expectedCounts[i], actualCounts[i]);
        }
    }

    /**
     * Method collectCounts.
     * @param relationalNode
     */
    static void collectCounts(RelationalNode relationalNode, int[] counts, Class<?>[] types) {
        Class<?> nodeType = relationalNode.getClass();
        if(nodeType.equals(JoinNode.class)) {
            JoinStrategy strategy = ((JoinNode)relationalNode).getJoinStrategy();
            if (((JoinNode)relationalNode).getJoinType().equals(JoinType.JOIN_SEMI)) {
                updateCounts(SemiJoin.class, counts, types);
            } else if (((JoinNode)relationalNode).getJoinType().equals(JoinType.JOIN_ANTI_SEMI)) {
                updateCounts(AntiSemiJoin.class, counts, types);
            }
            if (strategy instanceof NestedLoopJoinStrategy) {
                updateCounts(NestedLoopJoinStrategy.class, counts, types);
            } else if (strategy instanceof MergeJoinStrategy) {
                updateCounts(MergeJoinStrategy.class, counts, types);
                if (strategy instanceof EnhancedSortMergeJoinStrategy) {
                    updateCounts(EnhancedSortMergeJoinStrategy.class, counts, types);
                }
            } else if (strategy instanceof NestedTableJoinStrategy) {
                updateCounts(NestedTableJoinStrategy.class, counts, types);
            }
            if (((JoinNode)relationalNode).isDependent()) {
                updateCounts(DependentJoin.class, counts, types);
            }
        }else if (nodeType.equals(ProjectNode.class)){
            if (ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(((ProjectNode)relationalNode).getSelectSymbols()).isEmpty()) {
                updateCounts(ProjectNode.class, counts, types);
            } else {
                updateCounts(DependentProjectNode.class, counts, types);
            }
        }else if (nodeType.equals(SelectNode.class)){
            if (ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(((SelectNode)relationalNode).getCriteria()).isEmpty()) {
                updateCounts(SelectNode.class, counts, types);
            } else {
                updateCounts(DependentSelectNode.class, counts, types);
            }
        } else if (nodeType.equals(SortNode.class)) {
            Mode mode = ((SortNode)relationalNode).getMode();
            switch(mode) {
            case DUP_REMOVE_SORT:
                updateCounts(DupRemoveSortNode.class, counts, types);
                break;
            case SORT:
                updateCounts(SortNode.class, counts, types);
                break;
            }
        } else {
            updateCounts(nodeType, counts, types);
        }

        RelationalNode[] children = relationalNode.getChildren();
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                collectCounts(children[i], counts, types);
            } else {
                break;
            }
        }
    }

    private static void updateCounts(Class<?> nodeClass, int[] counts, Class<?>[] types) {
        for(int i=0; i<types.length; i++) {
            if(types[i].equals(nodeClass)) {
                counts[i] = counts[i] + 1;
                return;
            }
        }
    }

    public static void checkDependentJoinCount(ProcessorPlan plan, int expectedCount) {
        checkNodeTypes(plan, new int[] {expectedCount}, new Class[] {DependentJoin.class});
    }

    public static TransformationMetadata example1() {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = RealMetadataFactory.createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema pm2 = RealMetadataFactory.createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        Schema vm1 = RealMetadataFactory.createVirtualModel("vm1", metadataStore);     //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = RealMetadataFactory.createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = RealMetadataFactory.createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm1g3 = RealMetadataFactory.createPhysicalGroup("g3", pm1); //$NON-NLS-1$
        Table pm1g4 = RealMetadataFactory.createPhysicalGroup("g4", pm1); //$NON-NLS-1$
        Table pm1g5 = RealMetadataFactory.createPhysicalGroup("g5", pm1); //$NON-NLS-1$
        Table pm1g6 = RealMetadataFactory.createPhysicalGroup("g6", pm1); //$NON-NLS-1$
        Table pm1g7 = RealMetadataFactory.createPhysicalGroup("g7", pm1); //$NON-NLS-1$
        Table pm1g8 = RealMetadataFactory.createPhysicalGroup("g8", pm1); //$NON-NLS-1$
        Table pm2g1 = RealMetadataFactory.createPhysicalGroup("g1", pm2); //$NON-NLS-1$
        Table pm2g2 = RealMetadataFactory.createPhysicalGroup("g2", pm2); //$NON-NLS-1$
        Table pm2g3 = RealMetadataFactory.createPhysicalGroup("g3", pm2); //$NON-NLS-1$

        // Create physical elements
        RealMetadataFactory.createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(pm1g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(pm1g4,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(pm1g5,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(pm1g6,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(pm1g7,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(pm1g8,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(pm2g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(pm2g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(pm2g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = RealMetadataFactory.createUpdatableVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        QueryNode vm1g2n1 = new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2 = RealMetadataFactory.createUpdatableVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$

        //defect 8096
        QueryNode vm1sub1n1 = new QueryNode("SELECT * FROM vm1.g1 WHERE e1 IN /*+ no_unnest */ (SELECT e1 FROM vm1.g3)"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1sub1 = RealMetadataFactory.createVirtualGroup("sub1", vm1, vm1sub1n1); //$NON-NLS-1$

        QueryNode vm1g3n1 = new QueryNode("SELECT * FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g3 = RealMetadataFactory.createUpdatableVirtualGroup("g3", vm1, vm1g3n1); //$NON-NLS-1$

        QueryNode vm1g4n1 = new QueryNode("SELECT pm1.g1.e1,  g2.e1 FROM pm1.g1, pm1.g2 g2 WHERE pm1.g1.e1= g2.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g4 = RealMetadataFactory.createUpdatableVirtualGroup("g4", vm1, vm1g4n1); //$NON-NLS-1$

        QueryNode vm1g5n1 = new QueryNode("SELECT DISTINCT pm1.g1.e1 FROM pm1.g1 ORDER BY pm1.g1.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g5 = RealMetadataFactory.createUpdatableVirtualGroup("g5", vm1, vm1g5n1); //$NON-NLS-1$

        QueryNode vm1g6n1 = new QueryNode("SELECT e1, convert(e2, string), 3 as e3, ((e2+e4)/3) as e4 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g6 = RealMetadataFactory.createUpdatableVirtualGroup("g6", vm1, vm1g6n1); //$NON-NLS-1$

        QueryNode vm1u1n1 = new QueryNode("SELECT * FROM pm1.g1 UNION SELECT * FROM pm1.g2 UNION ALL SELECT * FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u1 = RealMetadataFactory.createUpdatableVirtualGroup("u1", vm1, vm1u1n1); //$NON-NLS-1$

        QueryNode vm1u2n1 = new QueryNode("SELECT * FROM pm1.g1 UNION SELECT * FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u2 = RealMetadataFactory.createUpdatableVirtualGroup("u2", vm1, vm1u2n1); //$NON-NLS-1$

        QueryNode vm1u3n1 = new QueryNode("SELECT e1 FROM pm1.g1 UNION SELECT convert(e2, string) as x FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u3 = RealMetadataFactory.createUpdatableVirtualGroup("u3", vm1, vm1u3n1); //$NON-NLS-1$

        QueryNode vm1u4n1 = new QueryNode("SELECT concat(e1, 'x') as v1 FROM pm1.g1 UNION ALL SELECT e1 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u4 = RealMetadataFactory.createUpdatableVirtualGroup("u4", vm1, vm1u4n1); //$NON-NLS-1$

        QueryNode vm1u5n1 = new QueryNode("SELECT concat(e1, 'x') as v1 FROM pm1.g1 UNION ALL SELECT concat('a', e1) FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u5 = RealMetadataFactory.createUpdatableVirtualGroup("u5", vm1, vm1u5n1); //$NON-NLS-1$

        QueryNode vm1u6n1 = new QueryNode("SELECT x1.e1 AS elem, 'xyz' AS const FROM pm1.g1 AS x1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u6 = RealMetadataFactory.createUpdatableVirtualGroup("u6", vm1, vm1u6n1); //$NON-NLS-1$

        QueryNode vm1u7n1 = new QueryNode("SELECT 's1' AS const, e1 FROM pm1.g1 UNION ALL SELECT 's2', e1 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u7 = RealMetadataFactory.createUpdatableVirtualGroup("u7", vm1, vm1u7n1); //$NON-NLS-1$

        QueryNode vm1u8n1 = new QueryNode("SELECT const, e1 FROM vm1.u7 UNION ALL SELECT 's3', e1 FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u8 = RealMetadataFactory.createUpdatableVirtualGroup("u8", vm1, vm1u8n1); //$NON-NLS-1$

        QueryNode vm1u9n1 = new QueryNode("SELECT e1 as a, e1 as b FROM pm1.g1 UNION ALL SELECT e1, e1 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1u9 = RealMetadataFactory.createUpdatableVirtualGroup("u9", vm1, vm1u9n1); //$NON-NLS-1$

        QueryNode vm1a1n1 = new QueryNode("SELECT e1, SUM(e2) AS sum_e2 FROM pm1.g1 GROUP BY e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1a1 = RealMetadataFactory.createUpdatableVirtualGroup("a1", vm1, vm1a1n1); //$NON-NLS-1$

        QueryNode vm1a2n1 = new QueryNode("SELECT e1, SUM(e2) AS sum_e2 FROM pm1.g1 GROUP BY e1 HAVING SUM(e2) > 5"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1a2 = RealMetadataFactory.createUpdatableVirtualGroup("a2", vm1, vm1a2n1); //$NON-NLS-1$

        QueryNode vm1a3n1 = new QueryNode("SELECT SUM(e2) AS sum_e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1a3 = RealMetadataFactory.createUpdatableVirtualGroup("a3", vm1, vm1a3n1); //$NON-NLS-1$

        QueryNode vm1a4n1 = new QueryNode("SELECT COUNT(*) FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1a4 = RealMetadataFactory.createUpdatableVirtualGroup("a4", vm1, vm1a4n1); //$NON-NLS-1$

        QueryNode vm1a5n1 = new QueryNode("SELECT vm1.a4.count FROM vm1.a4 UNION ALL SELECT COUNT(*) FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1a5 = RealMetadataFactory.createUpdatableVirtualGroup("a5", vm1, vm1a5n1); //$NON-NLS-1$

        QueryNode vm1a6n1 = new QueryNode("SELECT COUNT(*) FROM vm1.u2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1a6 = RealMetadataFactory.createUpdatableVirtualGroup("a6", vm1, vm1a6n1); //$NON-NLS-1$

        QueryNode vm1g7n1 = new QueryNode("select DECODESTRING(e1, 'S,Pay,P,Rec') as e1, e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g7 = RealMetadataFactory.createVirtualGroup("g7", vm1, vm1g7n1); //$NON-NLS-1$

        // Create virtual elements
        RealMetadataFactory.createElements(vm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(vm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(vm1g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        //for defect 8096
        RealMetadataFactory.createElements(vm1sub1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(vm1g4,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1g5,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING});
        RealMetadataFactory.createElements(vm1g6,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(vm1g7,
            new String[] { "e1", "e2"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER});
        RealMetadataFactory.createElements(vm1u1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(vm1u2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(vm1u3,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1u4,
            new String[] { "v1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1u5,
            new String[] { "v1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1u6,
            new String[] { "elem", "const" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1u7,
            new String[] { "const", "e1" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1u8,
            new String[] { "const", "e1" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1u9,
            new String[] { "a", "b" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        RealMetadataFactory.createElements(vm1a1,
            new String[] { "e1", "sum_e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.LONG });
        RealMetadataFactory.createElements(vm1a2,
            new String[] { "e1", "sum_e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.LONG });
        RealMetadataFactory.createElements(vm1a3,
            new String[] { "sum_e2" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.LONG });
        RealMetadataFactory.createElements(vm1a4,
            new String[] { "count" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER });
        RealMetadataFactory.createElements(vm1a5,
            new String[] { "count" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER });
        RealMetadataFactory.createElements(vm1a6,
            new String[] { "count" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER });

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example1");
    }

    // ################################## ACTUAL TESTS ################################

    /**
     * Test defect 8096 - query a virtual group with subquery of another virtual group
     */
    @Test public void testVirtualSubqueryINClause_8096() {
        helpPlan("SELECT * FROM vm1.sub1", example1(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1"} ); //$NON-NLS-1$
    }

    @Test public void testQueryPhysical() {
        ProcessorPlan plan = helpPlan("SELECT pm1.g1.e1, e2, pm1.g1.e3 as a, e4 as b FROM pm1.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1, e2, pm1.g1.e3, e4 FROM pm1.g1"} ); //$NON-NLS-1$
        assertNull(plan.requiresTransaction(true));
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSelectStarPhysical() {
        ProcessorPlan plan = helpPlan("SELECT * FROM pm1.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1"} ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testQuerySingleSourceVirtual() {
        ProcessorPlan plan = helpPlan("SELECT * FROM vm1.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1"} ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testQueryMultiSourceVirtual() {
        ProcessorPlan plan = helpPlan("SELECT * FROM vm1.g2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2, g_1.e3, g_1.e4 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e1 = g_1.e1"} ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPhysicalVirtualJoinWithCriteria() throws Exception {
        ProcessorPlan plan = helpPlan("SELECT vm1.g2.e1 from vm1.g2, pm1.g3 where vm1.g2.e1=pm1.g3.e1 and vm1.g2.e2 > 0", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1, pm1.g3 AS g_2 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = g_2.e1) AND (g_0.e2 > 0)" }, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testQueryWithExpression() {
        helpPlan("SELECT e4 FROM pm3.g1 WHERE e4 < convert('2001-11-01 10:30:40.42', timestamp)", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT e4 FROM pm3.g1 WHERE e4 < {ts'2001-11-01 10:30:40.42'}"} ); //$NON-NLS-1$
    }

    @Test public void testInsert() {
        helpPlan("Insert into pm1.g1 (pm1.g1.e1, pm1.g1.e2) values ('MyString', 1)", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "INSERT INTO pm1.g1 (pm1.g1.e1, pm1.g1.e2) VALUES ('MyString', 1)"} ); //$NON-NLS-1$
    }

    @Test public void testUpdate1() {
          helpPlan("Update pm1.g1 Set pm1.g1.e1= LTRIM('MyString'), pm1.g1.e2= 1 where pm1.g1.e3= 'true'", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "UPDATE pm1.g1 SET pm1.g1.e1 = 'MyString', pm1.g1.e2 = 1 WHERE pm1.g1.e3 = TRUE"} ); //$NON-NLS-1$
      }

    @Test public void testUpdate2() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        DefaultCapabilitiesFinder dcf = new DefaultCapabilitiesFinder(bsc);
        helpPlan("Update pm1.g1 Set pm1.g1.e1= LTRIM('MyString'), pm1.g1.e2= 1 where pm1.g1.e2= convert(pm1.g1.e4, integer)", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "UPDATE pm1.g1 SET e1 = 'MyString', e2 = 1 WHERE pm1.g1.e2 = convert(pm1.g1.e4, integer)"}, dcf, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
    }

    @Test public void testDelete() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        DefaultCapabilitiesFinder dcf = new DefaultCapabilitiesFinder(bsc);
        helpPlan("Delete from pm1.g1 where pm1.g1.e1 = cast(pm1.g1.e2 AS string)", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "DELETE FROM pm1.g1 WHERE pm1.g1.e1 = convert(pm1.g1.e2, string)"}, dcf, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
      }

    // ############################# TESTS ON EXAMPLE 1 ############################

    @Test public void testCopyInAcrossJoin() throws Exception {
        ProcessorPlan plan = helpPlan("select pm1.g1.e1, pm2.g2.e1 from pm1.g1, pm2.g2 where pm1.g1.e1=pm2.g2.e1 and pm1.g1.e1 IN ('a', 'b')", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 WHERE g_0.e1 IN ('a', 'b') ORDER BY c_0", //$NON-NLS-1$
                           "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN ('a', 'b') ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testCopyMatchAcrossJoin() throws Exception {
        helpPlan("select pm1.g1.e1, pm2.g2.e1 from pm1.g1, pm2.g2 where pm1.g1.e1=pm2.g2.e1 and pm1.g1.e1 LIKE '%1'", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e1 LIKE '%1' ORDER BY c_0", //$NON-NLS-1$
                            "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 WHERE g_0.e1 LIKE '%1' ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCopyOrAcrossJoin() throws Exception {
        helpPlan("select pm1.g1.e1, pm1.g2.e1 from pm1.g1, pm1.g2 where pm1.g1.e1=pm1.g2.e1 and (pm1.g1.e1 = 'abc' OR pm1.g1.e1 = 'def')", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1 WHERE (pm1.g1.e1 = 'abc') OR (pm1.g1.e1 = 'def')", //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2 WHERE (pm1.g2.e1 = 'abc') OR (pm1.g2.e1 = 'def')" }, getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCopyMultiElementCritAcrossJoin() throws Exception {
        helpPlan("select pm1.g1.e1, pm1.g2.e1 from pm1.g1, pm1.g2 where pm1.g1.e1=pm1.g2.e1 and pm1.g1.e2=pm1.g2.e2 and (pm1.g1.e1 = 'abc' OR pm1.g1.e2 = 5)", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2 WHERE (pm1.g2.e1 = 'abc') OR (pm1.g2.e2 = 5)", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 WHERE (pm1.g1.e1 = 'abc') OR (pm1.g1.e2 = 5)" }, getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCantCopyAcrossJoin1() throws Exception {
        helpPlan("select pm1.g1.e1, pm1.g2.e1 from pm1.g1, pm1.g2 where pm1.g1.e1=pm1.g2.e1 and concat(pm1.g1.e1, pm1.g1.e2) = 'abc'", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2" }, getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCantCopyAcrossJoin2() throws Exception {
        helpPlan("select pm1.g1.e1, pm1.g2.e1 from pm1.g1, pm1.g2 where pm1.g1.e1=pm1.g2.e1 and (pm1.g1.e1 = 'abc' OR pm1.g1.e2 = 5)", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1 WHERE (pm1.g1.e1 = 'abc') OR (pm1.g1.e2 = 5)", //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2" }, getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testPushingCriteriaThroughFrame1() {
        helpPlan("select * from vm1.g1, vm1.g2 where vm1.g1.e1='abc' and vm1.g1.e1=vm1.g2.e1", example1(), //$NON-NLS-1$
            new String[] { "SELECT g1__1.e1, g1__1.e2, g1__1.e3, g1__1.e4 FROM pm1.g1 AS g1__1 WHERE g1__1.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'" } ); //$NON-NLS-1$
      }

    @Test public void testPushingCriteriaThroughFrame2() throws Exception {
        helpPlan("select * from vm1.g1, vm1.g3 where vm1.g1.e1='abc' and vm1.g1.e1=vm1.g3.e1", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2 WHERE pm1.g2.e1 = 'abc'",  //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'" }, getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING ); //$NON-NLS-1$
      }

    @Test public void testPushingCriteriaThroughFrame3() {
        helpPlan("select * from vm1.g1, vm1.g2, vm1.g1 as a where vm1.g1.e1='abc' and vm1.g1.e1=vm1.g2.e1 and vm1.g1.e1=a.e1", example1(), //$NON-NLS-1$
            new String[] { "SELECT g1__1.e1, g1__1.e2, g1__1.e3, g1__1.e4 FROM pm1.g1 AS g1__1 WHERE g1__1.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT g1__2.e1, g1__2.e2, g1__2.e3, g1__2.e4 FROM pm1.g1 AS g1__2 WHERE g1__2.e1 = 'abc'" } ); //$NON-NLS-1$
      }

    @Test public void testPushingCriteriaThroughUnion1() {
        helpPlan("select e1 from vm1.u1 where e1='abc'", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g3.e1, pm1.g3.e2, pm1.g3.e3, pm1.g3.e4 FROM pm1.g3 WHERE pm1.g3.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2 WHERE pm1.g2.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'" } ); //$NON-NLS-1$
      }

    @Test public void testPushingCriteriaThroughUnion2() {
        helpPlan("select e1 from vm1.u2 where e1='abc'", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2 WHERE pm1.g2.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'" } ); //$NON-NLS-1$
      }

    @Test public void testPushingCriteriaThroughUnion3() {
        helpPlan("select e1 from vm1.u1 where e1='abc' and e2=5", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g3.e1, pm1.g3.e2, pm1.g3.e3, pm1.g3.e4 FROM pm1.g3 WHERE (pm1.g3.e1 = 'abc') AND (pm1.g3.e2 = 5)", //$NON-NLS-1$
                            "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2 WHERE (pm1.g2.e1 = 'abc') AND (pm1.g2.e2 = 5)", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE (pm1.g1.e1 = 'abc') AND (pm1.g1.e2 = 5)" } ); //$NON-NLS-1$
      }

    @Test public void testPushingCriteriaThroughUnion4() {
        helpPlan("select e1 from vm1.u1 where e1='abc' or e2=5", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g3.e1, pm1.g3.e2, pm1.g3.e3, pm1.g3.e4 FROM pm1.g3 WHERE (pm1.g3.e1 = 'abc') OR (pm1.g3.e2 = 5)", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE (pm1.g1.e1 = 'abc') OR (pm1.g1.e2 = 5)", //$NON-NLS-1$
                            "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2 WHERE (pm1.g2.e1 = 'abc') OR (pm1.g2.e2 = 5)" } ); //$NON-NLS-1$
      }

    // expression in a subquery of the union
    @Test public void testPushingCriteriaThroughUnion5() {
        helpPlan("select e1 from vm1.u3 where e1='abc'", example1(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT e1 FROM pm1.g1 WHERE e1 = 'abc'" } ); //$NON-NLS-1$
      }

    /** defect #4956 */
    @Test public void testPushCriteriaThroughUnion6() {
        helpPlan("select v1 from vm1.u4 where vm1.u4.v1='x'", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g2 WHERE e1 = 'x'" } ); //$NON-NLS-1$
    }

    @Test public void testPushCriteriaThroughUnion7() {
        helpPlan("select v1 from vm1.u5 where vm1.u5.v1='x'", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g2" } ); //$NON-NLS-1$
    }

    @Test public void testPushCriteriaThroughUnion8() {
        helpPlan("select v1 from vm1.u5 where length(v1) > 0", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g2" } ); //$NON-NLS-1$
    }

    @Test public void testPushCriteriaThroughUnion11() {
        helpPlan("select * from vm1.u8 where const = 's3' or e1 is null", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g3", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g2 WHERE e1 IS NULL", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g1 WHERE e1 IS NULL" } );     //$NON-NLS-1$
    }

    @Test public void testPushCriteriaThroughUnion12() {
        helpPlan("select * from vm1.u8 where const = 's1' or e1 is null", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g3 WHERE e1 IS NULL", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g2 WHERE e1 IS NULL", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g1" } );     //$NON-NLS-1$
    }

    /** defect #4997 */
    @Test public void testCountStarNoRows() {
        ProcessorPlan plan = helpPlan("select count(*) from vm1.u4", example1(), //$NON-NLS-1$
            new String[] { "SELECT 1 FROM pm1.g2",  //$NON-NLS-1$
                            "SELECT 1 FROM pm1.g1" } ); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            2,      // Access
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

    @Test public void testPushingCriteriaWithCopy() {
        ProcessorPlan plan = helpPlan("select vm1.u1.e1 from vm1.u1, pm1.g1 where vm1.u1.e1='abc' and vm1.u1.e1=pm1.g1.e1", example1(), //$NON-NLS-1$
            new String[] { "SELECT 1 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g3.e1, pm1.g3.e2, pm1.g3.e3, pm1.g3.e4 FROM pm1.g3 WHERE pm1.g3.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2 WHERE pm1.g2.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'" } ); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            4,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            1,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            2       // UnionAll
        });
      }

    @Test public void testVirtualGroupWithAliasedElement() {
        helpPlan("select elem FROM vm1.u6 where elem='abc' and const='xyz'", example1(), //$NON-NLS-1$
            new String[] { "SELECT x1.e1 FROM pm1.g1 AS x1 WHERE x1.e1 = 'abc'" } );     //$NON-NLS-1$
    }

    @Test public void testPushThroughGroup1() {
        helpPlan("select * FROM vm1.a1 WHERE e1 = 'x'", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = 'x'" } );     //$NON-NLS-1$
    }

    @Test public void testPushThroughGroup2() {
        helpPlan("select * FROM vm1.a2 WHERE e1 = 'x'", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = 'x'" } );     //$NON-NLS-1$
    }

    @Test public void testPushThroughGroup3() {
        helpPlan("select * FROM vm1.a3 WHERE sum_e2 > 0", example1(), //$NON-NLS-1$
            new String[] { "SELECT e2 FROM pm1.g1" } );     //$NON-NLS-1$
    }

    @Test public void testPushMultiGroupCriteria() {
        ProcessorPlan plan = helpPlan("select pm2.g1.e1 from pm2.g1, pm2.g2 where pm2.g1.e1 = pm2.g2.e1 and (pm2.g1.e2 = 1 OR pm2.g2.e2 = 2)", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1 FROM pm2.g1, pm2.g2 WHERE (pm2.g1.e1 = pm2.g2.e1) AND ((pm2.g1.e2 = 1) OR (pm2.g2.e2 = 2))" } ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSimpleCrossJoin1() throws Exception {
        helpPlan("select pm1.g1.e1 FROM pm1.g1, pm1.g2", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", //$NON-NLS-1$
                "SELECT pm1.g2.e1 FROM pm1.g2" }, new DefaultCapabilitiesFinder(), ComparisonMode.EXACT_COMMAND_STRING );     //$NON-NLS-1$
    }

    @Test public void testSimpleCrossJoin2() {
        helpPlan("select pm2.g1.e1 FROM pm2.g1, pm2.g2", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1 FROM pm2.g1, pm2.g2"} ); //$NON-NLS-1$

    }

    @Test public void testSimpleCrossJoin3() {
        helpPlan("select pm2.g1.e1 FROM pm2.g1 CROSS JOIN pm2.g2", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1 FROM pm2.g1, pm2.g2"} ); //$NON-NLS-1$

    }

    @Test public void testMultiSourceCrossJoin() throws Exception {
        helpPlan("select pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", //$NON-NLS-1$
                "SELECT pm1.g2.e1 FROM pm1.g2", //$NON-NLS-1$
                "SELECT pm1.g3.e1 FROM pm1.g3" }, new DefaultCapabilitiesFinder(), ComparisonMode.EXACT_COMMAND_STRING );     //$NON-NLS-1$
    }

    @Test public void testSingleSourceCrossJoin() {
        helpPlan("select pm2.g1.e1 FROM pm2.g1, pm2.g2, pm2.g3", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1 FROM pm2.g1, pm2.g2, pm2.g3"} ); //$NON-NLS-1$
    }

    @Test public void testSelfJoins() {
        helpPlan("select pm2.g1.e1 FROM pm2.g1 JOIN pm2.g1 AS x ON pm2.g1.e1=x.e1", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1 FROM pm2.g1 order by e1", //$NON-NLS-1$
                "SELECT x.e1 FROM pm2.g1 AS x order by e1" } );     //$NON-NLS-1$
    }

    @Test public void testDefect5282_1() {
        helpPlan("select * FROM vm1.a4 WHERE vm1.a4.count > 0", example1(), //$NON-NLS-1$
            new String[] { "SELECT 1 FROM pm1.g1" } );     //$NON-NLS-1$
    }

    @Test public void testDefect5282_2() {
        helpPlan("select count(*) FROM vm1.a4", example1(), //$NON-NLS-1$
            new String[] { } );     //$NON-NLS-1$
    }

    @Test public void testDefect5282_3() {
        helpPlan("select * FROM vm1.a5 WHERE vm1.a5.count > 0", example1(), //$NON-NLS-1$
            new String[] { "SELECT 1 FROM pm1.g1" } );     //$NON-NLS-1$
    }

    @Test public void testDepJoinHintBaseline() throws Exception {
        ProcessorPlan plan = helpPlan("select * FROM vm1.g4", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2" }, new DefaultCapabilitiesFinder(), ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testDefect6425_1() {
        helpPlan("select * from vm1.u9", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT e1 FROM pm1.g2" } );     //$NON-NLS-1$
    }

    @Test public void testDefect6425_2() {
        helpPlan("select count(*) from vm1.u9", example1(), //$NON-NLS-1$
            new String[] { "SELECT 1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT 1 FROM pm1.g2" } );     //$NON-NLS-1$
    }

    @Test public void testDefect6517() {
        helpPlan("select count(*) from vm1.g5", example1(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm1.g1.e1 FROM pm1.g1" });     //$NON-NLS-1$
    }

    @Test public void testDefect5283() {
        helpPlan("select * from vm1.a6", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2" } ); //$NON-NLS-1$
    }

    @Test public void testManyJoinsOverThreshold() throws Exception {
        long begin = System.currentTimeMillis();
        helpPlan("SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3, pm1.g4, pm1.g5, pm1.g6, pm1.g7, pm1.g8, pm1.g1 AS x, pm1.g2 AS y WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g2.e1 = pm1.g3.e1 AND pm1.g3.e1 = pm1.g4.e1 AND pm1.g4.e1 = pm1.g5.e1 AND pm1.g5.e1=pm1.g6.e1 AND pm1.g6.e1=pm1.g7.e1 AND pm1.g7.e1=pm1.g8.e1", //$NON-NLS-1$
            example1(),
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2",  //$NON-NLS-1$
                            "SELECT pm1.g3.e1 FROM pm1.g3",  //$NON-NLS-1$
                            "SELECT pm1.g4.e1 FROM pm1.g4", //$NON-NLS-1$
                            "SELECT pm1.g5.e1 FROM pm1.g5",  //$NON-NLS-1$
                            "SELECT pm1.g6.e1 FROM pm1.g6",  //$NON-NLS-1$
                            "SELECT pm1.g7.e1 FROM pm1.g7", //$NON-NLS-1$
                            "SELECT pm1.g8.e1 FROM pm1.g8", //$NON-NLS-1$
                            "SELECT x.e1 FROM pm1.g1 AS x", //$NON-NLS-1$
                            "SELECT y.e1 FROM pm1.g2 AS y" }, new DefaultCapabilitiesFinder(), ComparisonMode.CORRECTED_COMMAND_STRING ); //$NON-NLS-1$

        long elapsed = System.currentTimeMillis() - begin;
        assertTrue("Did not plan many join query in reasonable time frame: " + elapsed + " ms", elapsed < 4000); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testManyJoinsGreedy() throws Exception {
        TransformationMetadata tm = example1();
        RealMetadataFactory.setCardinality("pm1.g5", 1000000, tm);
        RealMetadataFactory.setCardinality("pm1.g4", 1000000, tm);
        RealMetadataFactory.setCardinality("pm1.g1", 10000000, tm);
        RealMetadataFactory.setCardinality("pm1.g8", 100, tm);
        RealMetadataFactory.setCardinality("pm1.g3", 10000, tm);
        RealMetadataFactory.setCardinality("pm1.g6", 100000, tm);
        ProcessorPlan plan = helpPlan("SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3, pm1.g4, pm1.g5, pm1.g6, pm1.g7, pm1.g8 "
                + "WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g2.e1 = pm1.g3.e1 AND pm1.g3.e1 = pm1.g4.e1 AND pm1.g4.e1 = pm1.g5.e1 AND pm1.g5.e1=pm1.g6.e1 AND pm1.g6.e1=pm1.g7.e1 AND pm1.g7.e1=pm1.g8.e1", //$NON-NLS-1$
            tm,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2",  //$NON-NLS-1$
                            "SELECT pm1.g3.e1 FROM pm1.g3",  //$NON-NLS-1$
                            "SELECT pm1.g4.e1 FROM pm1.g4", //$NON-NLS-1$
                            "SELECT pm1.g5.e1 FROM pm1.g5",  //$NON-NLS-1$
                            "SELECT pm1.g6.e1 FROM pm1.g6",  //$NON-NLS-1$
                            "SELECT pm1.g7.e1 FROM pm1.g7", //$NON-NLS-1$
                            "SELECT pm1.g8.e1 FROM pm1.g8", //$NON-NLS-1$
                            }, new DefaultCapabilitiesFinder(), ComparisonMode.CORRECTED_COMMAND_STRING ); //$NON-NLS-1$

        RelationalPlan rp = (RelationalPlan)plan;
        //g1 should be last
        assertEquals("[pm1.g1.e1]", ((JoinNode)rp.getRootNode().getChildren()[0]).getRightExpressions().toString());
    }

    @Test public void testAggregateWithoutGroupBy() {
        ProcessorPlan plan = helpPlan("select count(e2) from pm1.g1", example1(), //$NON-NLS-1$
            new String[] { "SELECT e2 FROM pm1.g1" } );         //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            0       // UnionAll
        });
    }

    @Test public void testHavingWithoutGroupBy() {
        ProcessorPlan plan = helpPlan("select count(e2) from pm1.g1 HAVING count(e2) > 0", example1(), //$NON-NLS-1$
            new String[] { "SELECT e2 FROM pm1.g1" } );         //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testHavingAndGroupBy() {
        ProcessorPlan plan = helpPlan("select e1, count(e2) from pm1.g1 group by e1 having count(e2) > 0 and sum(e2) > 0", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1" } );         //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testAllJoinsInSingleClause() throws Exception {
        ProcessorPlan plan = helpPlan("select pm1.g1.e1 FROM pm1.g1 join (pm1.g2 right outer join pm1.g3 on pm1.g2.e1=pm1.g3.e1) on pm1.g1.e1=pm1.g3.e1", example1(),  //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1",  //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2", //$NON-NLS-1$
                            "SELECT pm1.g3.e1 FROM pm1.g3" }, new DefaultCapabilitiesFinder(), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
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

    @Test public void testSelectCountStarFalseCriteria() {
        ProcessorPlan plan = helpPlan("Select count(*) from pm1.g1 where 1=0", example1(),  //$NON-NLS-1$
            new String[] { });
        checkNodeTypes(plan, new int[] {
            0,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            1,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testSubquery1() {
        ProcessorPlan plan = helpPlan("Select e1 from (select e1 FROM pm1.g1) AS x", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSubquery2() {
        ProcessorPlan plan = helpPlan("Select e1, a from (select e1 FROM pm1.g1) AS x, (select e1 as a FROM pm1.g2) AS y WHERE x.e1=y.a", example1(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_1.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e1 = g_1.e1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSubquery3() {
        ProcessorPlan plan = helpPlan("Select e1 from (select e1 FROM pm1.g1) AS x WHERE x.e1 = 'a'", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 = 'a'" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSubquery4() {
        ProcessorPlan plan = helpPlan("Select e1 from (select e1 FROM pm1.g1 WHERE e1 = 'a') AS x", example1(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 = 'a'" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSubqueryInClause1() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ no_unnest */ (select e1 FROM pm2.g1)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testCompareSubquery1() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 < ALL (select e1 FROM pm2.g1)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testCompareSubquery3() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 >= all (select e1 FROM pm2.g1)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testExistsSubquery1() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where exists (select e1 FROM pm2.g1 where pm2.g1.e1 = pm1.g1.e1)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testNotPushDistinct() throws Exception {
        ProcessorPlan plan = helpPlan("select distinct e1 from pm1.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1" }, new DefaultCapabilitiesFinder(), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushDistinct() {
        ProcessorPlan plan = helpPlan("select distinct e1 from pm3.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT e1 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctSort() {
        ProcessorPlan plan = helpPlan("select distinct e1 from pm3.g1 order by e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT e1 FROM pm3.g1 ORDER BY e1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctWithCriteria() {
        ProcessorPlan plan = helpPlan("select distinct e1 from pm3.g1 where e1 = 'x'", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT e1 FROM pm3.g1 WHERE e1 = 'x'" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual1() {
        ProcessorPlan plan = helpPlan("select * from vm1.g12", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual2() {
        ProcessorPlan plan = helpPlan("select DISTINCT * from vm1.g12", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual3() {
        ProcessorPlan plan = helpPlan("select DISTINCT * from vm1.g12 ORDER BY e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 ORDER BY pm3.g1.e1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual4() {
        ProcessorPlan plan = helpPlan("select * from vm1.g13", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual5() {
        ProcessorPlan plan = helpPlan("select DISTINCT * from vm1.g13", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual6() {
        ProcessorPlan plan = helpPlan("select DISTINCT * from vm1.g13 ORDER BY e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 ORDER BY pm3.g1.e1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual7() {
        ProcessorPlan plan = helpPlan("select * from vm1.g14", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual8() {
        ProcessorPlan plan = helpPlan("select DISTINCT * from vm1.g14", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushDistinctVirtual9() {
        ProcessorPlan plan = helpPlan("select DISTINCT * from vm1.g14 ORDER BY e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 ORDER BY pm3.g1.e1" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Defect #7819
     */
    @Test public void testPushDistinctWithExpressions() {
        ProcessorPlan plan = helpPlan("SELECT DISTINCT * FROM vm1.g15", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm3.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
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

    @Test public void testNestedSubquery() {
        ProcessorPlan plan = helpPlan("SELECT IntKey, LongNum FROM (SELECT IntKey, LongNum FROM (SELECT IntKey, LongNum, DoubleNum FROM BQT2.SmallA ) AS x ) AS y ORDER BY IntKey", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[] { "SELECT IntKey, LongNum FROM BQT2.SmallA order by intkey" }); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /** Tests a user's order by is pushed to the source */
    @Test public void testPushOrderBy() {
        ProcessorPlan plan = helpPlan("SELECT pm3.g1.e1 FROM pm3.g1 ORDER BY e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm3.g1.e1 FROM pm3.g1 ORDER BY pm3.g1.e1"}); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /** Tests an order by is not pushed to source due to join */
    @Test public void testDontPushOrderByWithJoin() {
        ProcessorPlan plan = helpPlan("SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1 INNER JOIN pm2.g2 ON pm3.g1.e1 = pm2.g2.e1 ORDER BY pm3.g1.e2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1 ORDER BY pm3.g1.e1", //$NON-NLS-1$
                           "SELECT pm2.g2.e1 FROM pm2.g2 ORDER BY pm2.g2.e1"}); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            1,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Tests that user's order by gets pushed to the source, but query
     * transformation order by is discarded
     */
    @Test public void testPushOrderByThroughFrame() {
        ProcessorPlan plan = helpPlan("SELECT e1, e2 FROM vm1.g14 ORDER BY e2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1 ORDER BY pm3.g1.e2"}); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Tests that query transformation order by is discarded by
     */
    @Test public void testPushOrderByThroughFrame2() {
        ProcessorPlan plan = helpPlan("SELECT e1, e2 FROM vm1.g1 ORDER BY e2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 order by e2"}); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Tests that a user's order by does not get pushed to the source
     * if there is a UNION in the query transformation
     */
    @Test public void testPushOrderByThroughFrame4_Union() {
        ProcessorPlan plan = helpPlan("SELECT e1, e2 FROM vm1.g17 ORDER BY e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1", //$NON-NLS-1$
                           "SELECT pm3.g2.e1, pm3.g2.e2 FROM pm3.g2"}); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            1,      // Sort
            1       // UnionAll
        });
    }

    /** Tests outer join defect #7945 - see also defect #10050*/
    @Test public void testOuterJoinDefect7945() {
        ProcessorPlan plan = helpPlan(
            "SELECT BQT1.SmallA.IntKey AS SmallA_IntKey, BQT2.MediumB.IntKey AS MediumB_IntKey, BQT3.MediumB.IntKey AS MediumC_IntKey " +  //$NON-NLS-1$
            "FROM (BQT1.SmallA RIGHT OUTER JOIN BQT2.MediumB ON BQT1.SmallA.IntKey = BQT2.MediumB.IntKey) " +  //$NON-NLS-1$
            "RIGHT OUTER JOIN BQT3.MediumB ON BQT2.MediumB.IntKey = BQT3.MediumB.IntKey " +   //$NON-NLS-1$
            "WHERE BQT3.MediumB.IntKey < 1500",  //$NON-NLS-1$
            RealMetadataFactory.exampleBQTCached(),
            new String[] {
                "SELECT BQT3.MediumB.IntKey FROM BQT3.MediumB WHERE BQT3.MediumB.IntKey < 1500 order by intkey", //$NON-NLS-1$
                "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE BQT1.SmallA.IntKey < 1500 order by intkey", //$NON-NLS-1$
                "SELECT BQT2.MediumB.IntKey FROM BQT2.MediumB WHERE BQT2.MediumB.IntKey < 1500 order by intkey" }); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
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

    /** Tests outer join defect #7945 */
    @Test public void testFunctionSimplification1() {
        ProcessorPlan plan = helpPlan(
            "SELECT x FROM vm1.g18 WHERE x = 92.0",   //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            new String[] {
                "SELECT e4 FROM pm1.g1 WHERE e4 = 0.92" }); //$NON-NLS-1$

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testCantPushJoin1() {
        ProcessorPlan plan = helpPlan(
            "SELECT a.e1, b.e2 FROM pm1.g1 a, pm1.g2 b WHERE a.e1 = b.e1",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, TestOptimizer.getGenericFinder(false),
            new String[] {"SELECT a.e1 FROM pm1.g1 AS a", "SELECT b.e1, b.e2 FROM pm1.g2 AS b"}, //$NON-NLS-1$ //$NON-NLS-2$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

        assertTrue(plan.requiresTransaction(true));
        assertFalse(plan.requiresTransaction(false));
    }

    @Test public void testCantPushJoin2() {
        ProcessorPlan plan = helpPlan(
            "SELECT a.e1, b.e2 FROM pm1.g1 a, pm1.g2 b, pm2.g1 c WHERE a.e1 = b.e1 AND b.e1 = c.e1",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, TestOptimizer.getGenericFinder(false),
            new String[] {"SELECT a.e1 FROM pm1.g1 AS a",  //$NON-NLS-1$
                           "SELECT b.e1, b.e2 FROM pm1.g2 AS b", //$NON-NLS-1$
                           "SELECT c.e1 FROM pm2.g1 AS c"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
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

    @Test public void testPushSelfJoin1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT a.e1, b.e2 FROM pm1.g1 a, pm1.g1 b WHERE a.e1 = b.e1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT a.e1, b.e2 FROM pm1.g1 AS a, pm1.g1 AS b WHERE a.e1 = b.e1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushSelfJoin2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT a.e1 AS x, concat(a.e2, b.e2) AS y FROM pm1.g1 a, pm1.g1 b WHERE a.e1 = b.e1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT a.e1, a.e2, b.e2 FROM pm1.g1 AS a, pm1.g1 AS b WHERE a.e1 = b.e1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushOuterJoin1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT pm1.g1.e1 FROM pm1.g1 RIGHT OUTER JOIN pm1.g2 ON pm1.g1.e1 = pm1.g2.e1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g2 LEFT OUTER JOIN pm1.g1 ON pm1.g1.e1 = pm1.g2.e1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushOuterJoin2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT pm1.g1.e1 FROM pm1.g1 RIGHT OUTER JOIN pm1.g2 ON pm1.g1.e1 = pm1.g2.e1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm1.g2.e1 FROM pm1.g2"}, //$NON-NLS-1$ //$NON-NLS-2$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    // With join expression that can't be pushed
    @Test public void testPushOuterJoin3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT pm1.g1.e1 FROM pm1.g1 RIGHT OUTER JOIN pm1.g2 ON pm1.g1.e1 = pm1.g2.e1 || 'x'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm1.g2.e1 FROM pm1.g2"}, //$NON-NLS-1$ //$NON-NLS-2$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

    }

    @Test public void testPushGroupBy1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e1, e2 as x FROM pm1.g1 GROUP BY e1, e2",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1, e2 FROM pm1.g1 GROUP BY e1, e2"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);

    }

    @Test public void testPushGroupBy2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1, MAX(e2) FROM pm1.g1 GROUP BY e1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);

    }

    @Test public void testPushGroupBy3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e1, e2 as x FROM pm1.g1 GROUP BY e1, e2",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1, e2 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            0       // UnionAll
        });

    }

    @Test public void testPushGroupBy4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT x+2 AS y FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT MAX(e2) FROM pm1.g1 GROUP BY e1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

    }

    @Test public void testPushHaving1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e1 FROM pm1.g1 GROUP BY e1 HAVING MAX(e1) = 'zzz'",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1 GROUP BY e1 HAVING MAX(e1) = 'zzz'"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushHaving2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e1 FROM pm1.g1 GROUP BY e1 HAVING MAX(e1) = 'zzz'",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushHaving3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e1 FROM pm1.g1 GROUP BY e1 HAVING MAX(e1) = 'zzz'",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushAggregate1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT MAX(e1) FROM pm1.g1",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT MAX(e1) FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushAggregate2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT MAX(e1) FROM pm1.g1 GROUP BY e1", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT MAX(e1) FROM pm1.g1 GROUP BY e1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushAggregate3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e2, MAX(e1) FROM pm1.g1 GROUP BY e2", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e2, MAX(e1) FROM pm1.g1 GROUP BY e2"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushAggregate4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e2, MAX(e1) FROM pm1.g1 GROUP BY e2 HAVING COUNT(e1) > 0", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e2, MAX(e1) FROM pm1.g1 GROUP BY e2 HAVING COUNT(e1) > 0"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Can't push aggs due to not being able to push COUNT in the HAVING clause.
     */
    @Test public void testPushAggregate5() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT e2, MAX(e1) FROM pm1.g1 GROUP BY e2 HAVING COUNT(e1) > 0", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e2, e1 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Can't push aggs due to not being able to push function inside the aggregate
     */
    @Test public void testPushAggregate6() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT COUNT(length(e1)) FROM pm1.g1", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            0       // UnionAll
        });
    }

    /**
     * Can't push aggs due to not being able to push function inside having
     */
    @Test public void testPushAggregate7() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT COUNT(*) FROM pm1.g1 GROUP BY e1 HAVING length(e1) > 0", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * BQT query that is failing
     */
    @Test public void testPushAggregate8() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sqlIn =
            "SELECT intkey FROM bqt1.smalla AS sa WHERE (sa.intkey = 46) AND " + //$NON-NLS-1$
            "(sa.stringkey IN (46)) AND (sa.datevalue = (" + //$NON-NLS-1$
            "SELECT MAX(sa.datevalue) FROM bqt1.smalla AS sb " + //$NON-NLS-1$
            "WHERE (sb.intkey = sa.intkey) AND (sa.stringkey = sb.stringkey) ))"; //$NON-NLS-1$

        String sqlOut = "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE (g_0.IntKey = 46) AND (g_0.StringKey = '46') AND (g_0.DateValue = (SELECT MAX(g_0.DateValue) FROM BQT1.SmallA AS g_1 WHERE (g_1.IntKey = g_0.IntKey) AND (g_1.StringKey = g_0.StringKey)))"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sqlIn,
            RealMetadataFactory.exampleBQTCached(),
            null, capFinder,
            new String[] {sqlOut},
            ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testQueryManyJoin() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = helpPlan("SELECT pm1.g1.e1 FROM pm1.g1 JOIN ((pm1.g2 JOIN pm1.g3 ON pm1.g2.e1=pm1.g3.e1) JOIN pm1.g4 ON pm1.g3.e1=pm1.g4.e1) ON pm1.g1.e1=pm1.g4.e1",  //$NON-NLS-1$
            metadata,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1, pm1.g3 AS g_2, pm1.g4 AS g_3 WHERE (g_1.e1 = g_2.e1) AND (g_2.e1 = g_3.e1) AND (g_0.e1 = g_3.e1)"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushSelectDistinct() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = helpPlan("SELECT DISTINCT e1 FROM pm3.g1",  //$NON-NLS-1$
            metadata,
            new String[] { "SELECT DISTINCT e1 FROM pm3.g1"} ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInCriteria1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT e1 FROM pm1.g1 WHERE upper(e1) = 'X'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1 WHERE ucase(e1) = 'X'"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInSelect1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT lower(e1) FROM pm1.g1 WHERE upper(e1) = 'X'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT lower(e1) FROM pm1.g1 WHERE ucase(e1) = 'X'"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInSelect2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT lower(e1), upper(e1), e2 FROM pm1.g1 WHERE upper(e1) = 'X'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT lower(e1), upper(e1), e2 FROM pm1.g1 WHERE ucase(e1) = 'X'"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInSelect3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT lower(e1), upper(e1) FROM pm1.g1 WHERE upper(e1) = 'X'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1 WHERE ucase(e1) = 'X'"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushFunctionInSelect4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT x FROM (SELECT lower(e1) AS x, upper(e1) AS y FROM pm1.g1 WHERE upper(e1) = 'X') AS z",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT lcase(e1) FROM pm1.g1 WHERE ucase(e1) = 'X'"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInSelect5() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT y, e, x FROM (SELECT lower(e1) AS x, upper(e1) AS y, 5 as z, e1 AS e FROM pm1.g1 WHERE upper(e1) = 'X') AS w",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT ucase(e1), e1, lcase(e1) FROM pm1.g1 WHERE ucase(e1) = 'X'"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInSelect6_defect_10081() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setFunctionSupport("upper", true); //$NON-NLS-1$
        caps.setFunctionSupport("lower", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT upper(lower(e1)) FROM pm1.g1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushFunctionInSelectWithOrderBy1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT e1, lower(e1) FROM pm1.g1 WHERE upper(e1) = 'X' ORDER BY e1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1, lcase(e1) FROM pm1.g1 WHERE ucase(e1) = 'X' ORDER BY e1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /** defect 13336 */
    @Test public void testPushFunctionInSelectWithOrderBy1a() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT e1, lower(e1) AS x FROM pm1.g1 WHERE upper(e1) = 'X' ORDER BY x",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1, lcase(e1) AS x FROM pm1.g1 WHERE ucase(e1) = 'X' ORDER BY x"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /** defect 13336 */
    @Test public void testPushFunctionInSelectWithOrderBy2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport(SourceSystemFunctions.LCASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT e1, x FROM (SELECT e1, lower(e1) AS x FROM pm1.g1 WHERE upper(e1) = 'X') AS z ORDER BY x",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1, lcase(e1) AS EXPR FROM pm1.g1 WHERE ucase(e1) = 'X' ORDER BY EXPR"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInJoin1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT pm1.g1.e1, pm1.g2.e3 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = convert(pm1.g2.e2, string) AND upper(pm1.g1.e1) = 'X'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT pm1.g1.e1, pm1.g2.e3 FROM pm1.g1, pm1.g2 WHERE (pm1.g1.e1 = convert(pm1.g2.e2, string)) AND (ucase(pm1.g1.e1) = 'X')"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushFunctionInJoin2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT pm1.g1.e1, pm1.g2.e3 FROM pm1.g1, pm1.g2, pm1.g3 WHERE pm1.g1.e1 = convert(pm1.g2.e2, string) AND pm1.g1.e1 = concat(pm1.g3.e1, 'a') AND upper(pm1.g1.e1) = 'X'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT pm1.g1.e1, pm1.g2.e3 FROM pm1.g1, pm1.g2 WHERE (pm1.g1.e1 = convert(pm1.g2.e2, string)) AND (ucase(pm1.g1.e1) = 'X') AND (ucase(convert(pm1.g2.e2, string)) = 'X')", //$NON-NLS-1$
                    "SELECT pm1.g3.e1 FROM pm1.g3"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushFunctionInJoin3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setFunctionSupport(SourceSystemFunctions.UCASE, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT pm1.g1.e1, pm1.g2.e3 FROM pm1.g1, pm1.g2, (SELECT e1 AS x FROM pm1.g3) AS g WHERE pm1.g1.e1 = convert(pm1.g2.e2, string) AND pm1.g1.e1 = concat(g.x, 'a') AND upper(pm1.g1.e1) = 'X'",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT pm1.g1.e1, pm1.g2.e3 FROM pm1.g1, pm1.g2 WHERE (pm1.g1.e1 = convert(pm1.g2.e2, string)) AND (ucase(pm1.g1.e1) = 'X') AND (ucase(convert(pm1.g2.e2, string)) = 'X')", //$NON-NLS-1$
                    "SELECT e1 FROM pm1.g3"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testUnionOverFunctions() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(
            "SELECT StringCol AS E " +  //$NON-NLS-1$
            "FROM (SELECT CONVERT(BQT1.SmallA.IntNum, string) AS StringCol, BQT1.SmallA.IntNum AS IntCol FROM BQT1.SmallA " + //$NON-NLS-1$
            "UNION ALL SELECT BQT1.SmallB.StringNum, CONVERT(BQT1.SmallB.StringNum, integer) FROM BQT1.SmallB) AS x",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT CONVERT(BQT1.SmallA.IntNum, string) FROM BQT1.SmallA", //$NON-NLS-1$
                    "SELECT BQT1.SmallB.StringNum FROM BQT1.SmallB"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testDefect9827() {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan("SELECT intkey, c FROM (SELECT DISTINCT b.intkey, b.intnum, a.stringkey AS c FROM bqt1.smalla AS a, bqt1.smallb AS b WHERE a.INTKEY = b.INTKEY) AS x ORDER BY x.intkey", metadata, //$NON-NLS-1$
            new String[] {"SELECT DISTINCT b.intkey, b.intnum, a.stringkey FROM bqt1.smalla AS a, bqt1.smallb AS b WHERE a.INTKEY = b.INTKEY"} ); //$NON-NLS-1$

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
            0,      // Select
            1,      // Sort
            0       // UnionAll
        });
    }

    /**
     * This tests that a criteria with no elements is not pushed down,
     * but instead is cleaned up properly later
     * See defect 9865
     */
    @Test public void testCrossJoinNoElementCriteriaOptimization2() {
        ProcessorPlan plan = helpPlan("select Y.e1, Y.e2 FROM vm1.g1 X, vm1.g1 Y where {b'true'} = {b'true'}", example1(),  //$NON-NLS-1$
            new String[]{"SELECT 1 FROM pm1.g1 AS g1__1", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
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
     * <p>This tests that a SELECT node with no groups is not pushed down without the capability to have a subquery in the where clause.
     */
    @Test public void testCrossJoinNoElementCriteriaOptimization3() {
        ProcessorPlan plan = helpPlan("select Y.e1, Y.e2 FROM vm1.g1 X, vm1.g1 Y where {b'true'} in (select e3 FROM vm1.g1)", example1(),  //$NON-NLS-1$
            new String[]{"SELECT 1 FROM pm1.g1 AS g1__1", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * <p>This tests that a SELECT node with no groups is pushed down.
     */
    @Test public void testCrossJoinNoElementCriteriaOptimization4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select Y.e1, Y.e2 FROM vm1.g1 X, vm1.g1 Y where {b'true'} in (select e3 FROM vm1.g1)", example1(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT 1 FROM pm1.g1 AS g1__1 WHERE TRUE IN (SELECT pm1.g1.e3 FROM pm1.g1)", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, true); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
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
     * See also case 2912.
     */
    @Test public void testCopyCriteriaWithOuterJoin5_defect10050(){
        helpPlan("select pm2.g1.e1, pm1.g2.e1, pm2.g3.e1 from ( (pm2.g1 right outer join pm1.g2 on pm2.g1.e1=pm1.g2.e1) right outer join pm2.g3 on pm1.g2.e1=pm2.g3.e1) where pm2.g3.e1 = 'a'", example1(), //$NON-NLS-1$
                new String[] { "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 = 'a'", "SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e1 = 'a'", "SELECT g_0.e1 FROM pm2.g3 AS g_0 WHERE g_0.e1 = 'a'" }); //$NON-NLS-1$
    }

    @Test public void testCopyCriteriaWithTransitivePushdown(){
        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1, pm2.g2, pm2.g3 where pm2.g1.e1 = pm2.g2.e1 and pm2.g2.e1 = pm2.g3.e1", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_1.e1 FROM pm2.g1 AS g_0, pm2.g2 AS g_1, pm2.g3 AS g_2 WHERE (g_0.e1 = g_1.e1) AND (g_1.e1 = g_2.e1)" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCopyCriteriaWithTransitivePushdown2(){
        ProcessorPlan plan = helpPlan("select pm1.g1.e1, pm1.g2.e1 from pm1.g1, pm1.g4, pm1.g2, pm1.g3 where pm1.g1.e1 = pm1.g2.e1 and pm1.g2.e1 = pm1.g3.e1 and pm1.g1.e1 = 'a'", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_1.e1, g_2.e1 FROM pm1.g4 AS g_0, pm1.g1 AS g_1, pm1.g2 AS g_2, pm1.g3 AS g_3 WHERE (g_2.e1 = g_3.e1) AND (g_1.e1 = g_2.e1) AND (g_1.e1 = 'a') AND (g_2.e1 = 'a') AND (g_3.e1 = 'a')" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCopyCriteriaWithTransitivePushdown3() throws TeiidComponentException, TeiidProcessingException{
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        ProcessorPlan plan = helpPlan("select pm1.g1.e1 from pm1.g1, pm1.g2, pm1.g3 where pm1.g1.e1 = pm1.g2.e1 and pm1.g1.e1 = pm1.g3.e2 and pm1.g3.e2 = pm1.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1, pm1.g3 AS g_2 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = convert(g_2.e2, string)) AND (convert(g_2.e2, string) = g_1.e1)" }
        , new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCleanCriteria(){

        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1, pm2.g2 where pm2.g1.e1=pm2.g2.e1 and pm2.g1.e2 IN (1, 2)", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1, pm2.g2.e1 FROM pm2.g1, pm2.g2 WHERE (pm2.g1.e1 = pm2.g2.e1) AND (pm2.g1.e2 IN (1, 2))" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCleanCriteria2(){

        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1, pm2.g2 where pm2.g1.e1=pm2.g2.e1 and pm2.g1.e1 = 'a'", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_1.e1 FROM pm2.g1 AS g_0, pm2.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = 'a')" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCleanCriteria3(){

        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1 inner join pm2.g2 on pm2.g1.e1=pm2.g2.e1 where pm2.g1.e1 = 'a'", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_1.e1 FROM pm2.g1 AS g_0, pm2.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = 'a')" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }



    /** Should use merge join since neither access node is "strong" - order by's pushed to source */
    @Test public void testUseMergeJoin3() throws Exception{
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY pm1.g1.e1", "SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY pm1.g2.e1" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** Model supports order by, should be pushed to the source */
    @Test public void testUseMergeJoin4() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 500, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY pm1.g1.e1", "SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY pm1.g2.e1" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** Should use merge join, since costs are not known, neither access node is "strong" */
    @Test public void testUseMergeJoin5_CostsNotKnown(){
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm1.g2.e1 FROM pm1.g2" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** one side of join supports order by, the other doesn't*/
    @Test public void testUseMergeJoin7() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g2 WHERE pm1.g1.e1 = pm2.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 500, metadata);
        RealMetadataFactory.setCardinality("pm2.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY pm1.g1.e1", "SELECT pm2.g2.e1 FROM pm2.g2" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** reverse of testUseMergeJoin7 */
    @Test public void testUseMergeJoin7a() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g2 WHERE pm1.g1.e1 = pm2.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 500, metadata);
        RealMetadataFactory.setCardinality("pm2.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm2.g2.e1 FROM pm2.g2 ORDER BY pm2.g2.e1" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** function on one side of join should prevent order by from being pushed down*/
    @Test public void testUseMergeJoin8() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g2 WHERE concat(pm1.g1.e1, 'x') = pm2.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 500, metadata);
        RealMetadataFactory.setCardinality("pm2.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm2.g2.e1 FROM pm2.g2 ORDER BY pm2.g2.e1" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** Model supports order by, functions in join criteria */
    @Test public void testUseMergeJoin9() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE concat(pm1.g1.e1, 'x') = concat(pm1.g2.e1, 'x')";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 500, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm1.g2.e1 FROM pm1.g2" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            3,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** should be one dependent join */
    @Test public void testMultiMergeJoin1() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3 WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g2.e1 = pm1.g3.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE / 4, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE, metadata);
        RealMetadataFactory.setCardinality("pm1.g3", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm1.g2.e1 FROM pm1.g2", "SELECT pm1.g3.e1 FROM pm1.g3" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
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

    @Test public void testLargeSetCriteria() throws TeiidComponentException, TeiidProcessingException {
        //      Create query
        String sql = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA INNER JOIN BQT2.SmallB ON BQT1.SmallA.IntKey = BQT2.SmallB.IntKey WHERE BQT1.SmallA.IntKey IN (1,2,3,4,5)";     //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1)); //the language bridge factory will handle the expansion
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
            null, capFinder,
            new String[] { "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE BQT1.SmallA.IntKey IN (1, 2, 3, 4, 5) ORDER BY BQT1.SmallA.IntKey",
            "SELECT BQT2.SmallB.IntKey FROM BQT2.SmallB WHERE BQT2.SmallB.IntKey IN (1, 2, 3, 4, 5) ORDER BY BQT2.SmallB.IntKey" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testMergeJoin_defect11236(){
        // Create query
        String sql = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA, BQT1.SmallB WHERE BQT1.SmallA.IntKey = (BQT1.SmallB.IntKey + 1)";     //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT BQT1.SmallB.IntKey FROM BQT1.SmallB",  //$NON-NLS-1$
                            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA ORDER BY BQT1.SmallA.IntKey" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testNoFrom() {
        ProcessorPlan plan = helpPlan("SELECT 1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {} );

        checkNodeTypes(plan, new int[] {
            0,      // Access
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

    @Test public void testINCriteria_defect10718() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY -1, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY pm1.g1.e1", "SELECT pm1.g2.e1 FROM pm1.g2 ORDER BY pm1.g2.e1"}, SHOULD_SUCCEED); //$NON-NLS-1$  //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testDefect10711(){
        ProcessorPlan plan = helpPlan("SELECT * from vm1.g1a as X", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1"} ); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);

    }

    // SELECT 5, SUM(IntKey) FROM BQT1.SmallA
    @Test public void testAggregateNoGroupByWithExpression() {
        ProcessorPlan plan = helpPlan("SELECT 5, SUM(IntKey) FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[] { "SELECT IntKey FROM BQT1.SmallA"  }); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            1,      // Access
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
            0       // UnionAll
        });
    }

    /** defect 11630 - note that the lookup function is not pushed down, it will actually be evaluated before being sent to the connector */
    @Test public void testLookupFunction() {

        ProcessorPlan plan = helpPlan("SELECT e1 FROM pm1.g2 WHERE LOOKUP('pm1.g1','e1', 'e2', 1) IS NULL", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g2 WHERE LOOKUP('pm1.g1', 'e1', 'e2', 1) IS NULL"  }); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);

    }

    /** case 5213 - note here that the lookup cannot be pushed down since it is dependent upon an element symbol*/
    @Test public void testLookupFunction2() throws Exception {

        ProcessorPlan plan = helpPlan("SELECT e1 FROM pm1.g2 WHERE LOOKUP('pm1.g1','e1', 'e2', e2) IS NULL", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e2, g_0.e1 FROM pm1.g2 AS g_0"  }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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

    /** defect 21965 */
    @Test public void testLookupFunctionInSelect() {
        ProcessorPlan plan = helpPlan("SELECT e1, LOOKUP('pm1.g1','e1', 'e2', 1) FROM pm1.g2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT e1, LOOKUP('pm1.g1','e1', 'e2', 1) FROM pm1.g2"  }); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    // SELECT * FROM (SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT DISTINCT IntNum FROM BQT1.SmallA) AS x WHERE IntKey = 0
    @Test public void testCase1649() {
        ProcessorPlan plan = helpPlan("SELECT * FROM (SELECT DISTINCT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA) AS x WHERE IntKey = 0", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT IntKey FROM BQT1.SmallA WHERE IntKey = 0", "SELECT IntNum FROM BQT1.SmallA WHERE IntNum = 0"  }); //$NON-NLS-1$ //$NON-NLS-2$

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    // SELECT * FROM (SELECT IntKey a, IntNum b FROM BQT1.SmallA UNION ALL SELECT Intkey, Intkey FROM BQT1.SmallA) as x WHERE b = 0
    @Test public void testCase1727_1() {
        ProcessorPlan plan = helpPlan("SELECT * FROM (SELECT IntKey a, IntNum b FROM BQT1.SmallA UNION ALL SELECT Intkey, Intkey FROM BQT1.SmallA) as x WHERE b = 0", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[] {
                "SELECT IntKey, IntNum FROM BQT1.SmallA WHERE IntNum = 0", //$NON-NLS-1$
                "SELECT IntKey FROM BQT1.SmallA WHERE IntKey = 0"  }); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    // SELECT * FROM (SELECT IntKey a, IntNum b FROM BQT1.SmallA UNION ALL SELECT Intkey, Intkey FROM BQT1.SmallA) as x WHERE b = 0
    @Test public void testCase1727_2() {
        ProcessorPlan plan = helpPlan("SELECT * FROM (SELECT IntKey a, IntKey b FROM BQT1.SmallA UNION ALL SELECT IntKey, IntNum FROM BQT1.SmallA) as x WHERE b = 0", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[] {
                "SELECT IntKey, IntNum FROM BQT1.SmallA WHERE IntNum = 0", //$NON-NLS-1$
                "SELECT IntKey FROM BQT1.SmallA WHERE IntKey = 0"  }); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testCountStarOverSelectDistinct() {
        ProcessorPlan plan = helpPlan("SELECT COUNT(*) FROM (SELECT DISTINCT IntNum, Intkey FROM bqt1.smalla) AS x", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
                                      new String[] {
                                          "SELECT DISTINCT IntNum, Intkey FROM bqt1.smalla" }); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
        });
    }

    //virtual group with two elements. One selectable, one not
    @Test public void testVirtualGroup1() {
        ProcessorPlan plan = helpPlan("select e2 from vm1.g35", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT e2 FROM pm1.g1" } ); //$NON-NLS-1$

        checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testBQT9500_126() throws Exception {
        String sql = "SELECT IntKey, LongNum, expr FROM (SELECT IntKey, LongNum, concat(LongNum, 'abc') as expr FROM BQT2.SmallA ) AS x ORDER BY IntKey"; //$NON-NLS-1$
        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                      new String[] {
                                          "SELECT g_0.IntKey AS c_0, g_0.LongNum AS c_1 FROM BQT2.SmallA AS g_0 ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
        });

    }

    public void helpTestUnionPushdown(boolean queryHasOrderBy, boolean hasUnionCapability, boolean hasUnionOrderByCapability) {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, hasUnionCapability);
        caps.setCapabilitySupport((Capability.QUERY_ORDERBY), hasUnionOrderByCapability);
        caps.setCapabilitySupport((Capability.QUERY_SET_ORDER_BY), hasUnionOrderByCapability);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sqlUnion = "SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB";//$NON-NLS-1$
        String sqlOrderBy = sqlUnion + " ORDER BY IntKey"; //$NON-NLS-1$
        String sql = null;
        if(queryHasOrderBy) {
            sql = sqlOrderBy;
        } else {
            sql = sqlUnion;
        }

        String[] expectedSql = null;
        if(hasUnionCapability) {
            if(queryHasOrderBy && hasUnionOrderByCapability) {
                expectedSql = new String[] {sqlOrderBy };
            } else {
                expectedSql = new String[] {sqlUnion };
            }
        } else {
            expectedSql = new String[] { "SELECT IntKey FROM BQT1.SmallA", "SELECT IntKey FROM BQT1.SmallB" };  //$NON-NLS-1$//$NON-NLS-2$
        }

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql, metadata,
                                      null, capFinder, expectedSql, SHOULD_SUCCEED);

        int accessCount = hasUnionCapability ? 1 : 2;
        int projectCount = 0;
        int sortCount = 0;
        if(queryHasOrderBy && ! (hasUnionCapability && hasUnionOrderByCapability)) {
            sortCount = 1;
        }
        int unionCount = hasUnionCapability ? 0 : 1;



        checkNodeTypes(plan, new int[] {
                                        accessCount,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        0,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        projectCount,      // Project
                                        0,      // Select
                                        sortCount,      // Sort
                                        unionCount       // UnionAll
        });
    }

    /**
     * Query has union but no order by and no capabilities.
     */
    @Test public void testUnionPushdown1() {
        helpTestUnionPushdown(false, false, false);
    }

    /**
     * Query has union but no order by and only union capability.
     */
    @Test public void testUnionPushdown2() {
        helpTestUnionPushdown(false, true, false);
    }

    /**
     * Query has union with order by and no capabilities.
     */
    @Test public void testUnionPushdown3() {
        helpTestUnionPushdown(true, false, false);
    }

    /**
     * Query has union with order by and just union capability.
     */
    @Test public void testUnionPushdown4() {
        helpTestUnionPushdown(true, true, false);
    }

    /**
     * Query has union with order by and both capabilities.
     */
    @Test public void testUnionPushdown5() {
        helpTestUnionPushdown(true, true, true);
    }

    @Test public void testUnionPushdownWithSelectNoFrom() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT 1 UNION ALL SELECT 2", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder, new String[] {}, SHOULD_SUCCEED);

        checkNodeTypes(plan, new int[] {
                                        0,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        0,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        2,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        1       // UnionAll
        });
    }

    @Test public void testUnionPushdownWithSelectNoFromFirstBranch() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT 1 UNION ALL SELECT IntKey FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder, new String[] {"SELECT IntKey FROM BQT1.SmallA"}, SHOULD_SUCCEED);   //$NON-NLS-1$

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
                                        0,      // Select
                                        0,      // Sort
                                        1       // UnionAll
        });
    }

    @Test public void testUnionPushdownWithSelectNoFromSecondBranch() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT 1", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder, new String[] {"SELECT IntKey FROM BQT1.SmallA"}, SHOULD_SUCCEED);   //$NON-NLS-1$

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
                                        0,      // Select
                                        0,      // Sort
                                        1       // UnionAll
        });
    }

    @Test public void testUnionPushdownMultipleBranches() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB UNION ALL SELECT IntKey FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB UNION ALL SELECT IntKey FROM BQT1.SmallA"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUnionPushdownMultipleBranchesMixedModels1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB UNION ALL SELECT IntKey FROM BQT2.SmallA", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB", "SELECT IntKey FROM BQT2.SmallA"},  //$NON-NLS-1$ //$NON-NLS-2$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        0,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        0,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        1       // UnionAll
        });
    }

    @Test public void testUnionPushdownMultipleBranchesNoDupRemoval() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT IntKey FROM BQT1.SmallA UNION SELECT IntKey FROM BQT1.SmallB UNION SELECT IntKey FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA UNION SELECT IntKey FROM BQT1.SmallB UNION SELECT IntKey FROM BQT1.SmallA"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testAggregateOverUnionPushdown() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT COUNT(*) FROM (SELECT IntKey FROM BQT1.SmallA UNION SELECT IntKey FROM BQT1.SmallB) AS x", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA UNION SELECT IntKey FROM BQT1.SmallB"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
        });
    }

    @Test public void testUnionPushdownWithFunctionsAndAliases() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT IntKey+2, StringKey AS x FROM BQT1.SmallA UNION SELECT IntKey, StringKey FROM BQT1.SmallB", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT (IntKey + 2), StringKey AS x FROM BQT1.SmallA UNION SELECT IntKey, StringKey FROM BQT1.SmallB"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUnionPushdownWithInternalOrderBy() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("(SELECT IntKey FROM BQT1.SmallA ORDER BY IntKey) UNION ALL SELECT IntKey FROM BQT1.SmallB", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUnionPushdownWithInternalDistinct() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan("SELECT DISTINCT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB", metadata,  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT DISTINCT IntKey FROM BQT1.SmallA UNION ALL SELECT IntKey FROM BQT1.SmallB"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUnionNoAllPushdownInInlineView() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT x FROM (SELECT IntKey+2, StringKey AS x FROM BQT1.SmallA UNION SELECT IntKey, StringKey FROM BQT1.SmallB) AS g", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT (IntKey + 2), StringKey AS x FROM BQT1.SmallA UNION SELECT IntKey, StringKey FROM BQT1.SmallB"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

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
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
        });
    }

    @Test public void testUnionAllPushdownInInlineView() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, false);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT x FROM (SELECT IntKey+2, StringKey AS x FROM BQT1.SmallA UNION ALL SELECT IntKey, StringKey FROM BQT1.SmallB) AS g", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT StringKey AS x FROM BQT1.SmallA UNION ALL SELECT StringKey FROM BQT1.SmallB"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUnionAllPushdownVirtualGroup() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT * FROM vm1.g4", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT e1 FROM pm1.g1 UNION ALL SELECT convert(e2, string) FROM pm1.g2"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUnionAllPushdownVirtualGroup2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT e2 FROM vm1.g17", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
                                      null, capFinder,
                                      new String[] {"SELECT pm3.g1.e2 FROM pm3.g1 UNION ALL SELECT pm3.g2.e2 FROM pm3.g2"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUnionAllPushdownVirtualGroup3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT * FROM (SELECT intkey, 5 FROM BQT1.SmallA UNION ALL SELECT intnum, 10 FROM bqt1.smalla) AS x",  //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT intkey FROM BQT1.SmallA", "SELECT IntNum FROM bqt1.smalla"},  //$NON-NLS-1$ //$NON-NLS-2$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        0,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        2,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        1       // UnionAll
        });
    }

    // Allow pushing literals
    @Test public void testUnionAllPushdownVirtualGroup4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT * FROM (SELECT intkey, 5 FROM BQT1.SmallA UNION ALL SELECT intnum, 10 FROM bqt1.smalla) AS x",  //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT intkey, 5 FROM BQT1.SmallA UNION ALL SELECT IntNum, 10 FROM bqt1.smalla"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushCaseInSelect() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT CASE WHEN e1 = 'a' THEN 10 ELSE 0 END FROM pm1.g1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT CASE WHEN e1 = 'a' THEN 10 ELSE 0 END FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testCantPushCaseInSelectWithFunction() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT CASE e1 WHEN 'a' THEN 10 ELSE (e2+0) END FROM pm1.g1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1, e2 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushSearchedCaseInSelect() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT CASE WHEN e1 = 'a' THEN 10 ELSE 0 END FROM pm1.g1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT CASE WHEN e1 = 'a' THEN 10 ELSE 0 END FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testCantPushSearchedCaseInSelectWithFunction() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT CASE WHEN e1 = 'a' THEN 10 ELSE (e2+0) END FROM pm1.g1",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1, e2 FROM pm1.g1"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushdownFunctionNotEvaluated() {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        caps.setFunctionSupport("xyz", true);         //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(
            "SELECT e1 FROM pm1.g1 WHERE xyz() > 0",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT e1 FROM pm1.g1 WHERE xyz() > 0"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testNoSourceQuery() {
        ProcessorPlan plan = helpPlan("SELECT * FROM (select parsetimestamp(x,'yyyy-MM-dd') as c1 from (select '2004-10-20' as x) as y) as z " +//$NON-NLS-1$
                                      "WHERE c1= '2004-10-20 00:00:00.0'", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[] {  });

        checkNodeTypes(plan, new int[] {
            0,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** defect 14510 */
    @Test public void testDefect14510LookupFunction() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("bqt1.smallb", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt1.smalla", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA, BQT1.SmallB WHERE (BQT1.SmallA.IntKey = lookup('BQT1.SmallB', 'IntKey', 'StringKey', BQT1.SmallB.StringKey)) AND (BQT1.SmallA.IntKey = 1)",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT g_1.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1 WHERE g_0.IntKey = 1"}, //$NON-NLS-1$ //$NON-NLS-2$
            SHOULD_SUCCEED );

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

    /** defect 14510 */
    @Test public void testDefect14510LookupFunction2() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("bqt1.mediumb", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt1.smalla", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT BQT1.SmallA.IntKey, BQT1.MediumB.IntKey FROM BQT1.SmallA LEFT OUTER JOIN BQT1.MediumB ON BQT1.SmallA.IntKey = lookup('BQT1.MediumB', 'IntKey', 'StringKey', BQT1.MediumB.StringKey)",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA", "SELECT BQT1.MediumB.StringKey, BQT1.MediumB.IntKey FROM BQT1.MediumB"}, //$NON-NLS-1$ //$NON-NLS-2$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** defect 14510 */
    @Test public void testDefect14510LookupFunction3() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("bqt1.mediumb", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt1.smalla", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
            "SELECT BQT1.SmallA.IntKey, BQT1.MediumB.IntKey FROM BQT1.MediumB RIGHT OUTER JOIN BQT1.SmallA ON BQT1.SmallA.IntKey = lookup('BQT1.MediumB', 'IntKey', 'StringKey',BQT1.MediumB.StringKey)",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA", "SELECT BQT1.MediumB.StringKey, BQT1.MediumB.IntKey FROM BQT1.MediumB"}, //$NON-NLS-1$ //$NON-NLS-2$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testCase2125() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sql = "SELECT OD.IntKEy, P.IntKEy, O.IntKey " +  //$NON-NLS-1$
            "FROM (bqt1.smalla AS OD INNER JOIN bqt1.smallb AS P ON OD.StringKey = P.StringKey) " +  //$NON-NLS-1$
            "INNER JOIN bqt1.mediuma AS O ON O.IntKey = OD.IntKey " +  //$NON-NLS-1$
            "WHERE (OD.IntNum > /*+ no_unnest */ (SELECT SUM(IntNum) FROM bqt1.smalla)) AND " +  //$NON-NLS-1$
            "(P.longnum > /*+ no_unnest */ (SELECT AVG(LongNum) FROM bqt1.smallb WHERE bqt1.smallb.datevalue = O.datevalue))"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT g_1.LongNum, g_2.DateValue, g_0.IntKey, g_1.IntKey, g_2.IntKey FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1, BQT1.MediumA AS g_2 WHERE (g_0.StringKey = g_1.StringKey) AND (g_2.IntKey = g_0.IntKey) AND (convert(g_0.IntNum, long) > /*+ NO_UNNEST */ (SELECT SUM(g_3.IntNum) FROM BQT1.SmallA AS g_3))"}, //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
                                        0,      // DependentAccess
                                        1,      // DependentSelect
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

    @Test public void testPushdownLiteralInSelectUnderAggregate() {
        String sql = "SELECT COUNT(*) FROM (SELECT '' AS y, a.IntKey FROM BQT1.SmallA a union all select '', b.intkey from bqt1.smallb b) AS x"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT 1 AS c_0 FROM BQT1.SmallA AS a UNION ALL SELECT 1 AS c_0 FROM bqt1.smallb AS b"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
                                    });
    }

    @Test public void testPushdownLiteralInSelectUnderAggregate2() {
        String sql = "SELECT SUM(z) FROM (SELECT '' AS y, a.IntKey as z FROM BQT1.SmallA a union all select b.stringkey, 0 from bqt1.smallb b) AS x group by z"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT a.IntKey AS z FROM BQT1.SmallA AS a UNION ALL SELECT 0 FROM bqt1.smallb AS b"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
                                    });
    }

    @Test public void testPushdownLiteralInSelectUnderAggregate3() {
        String sql = "SELECT code, SUM(ID) FROM (SELECT IntKey AS ID, '' AS Code FROM BQT1.SmallA union all select intkey, stringkey from bqt1.smallb b) AS x group by code"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT '' AS Code, IntKey AS ID FROM BQT1.SmallA UNION ALL SELECT stringkey, intkey FROM bqt1.smallb AS b"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
                                    });
    }

    @Test public void testPushdownLiteralInSelectWithOrderBy() {
        String sql = "SELECT 1, concat('a', 'b' ) AS X FROM BQT1.SmallA where intkey = 0 " +  //$NON-NLS-1$
            "UNION ALL " +  //$NON-NLS-1$
            "select 2, 'Hello2' from BQT1.SmallA where intkey = 1 order by X desc"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT 1, 'ab' AS X FROM BQT1.SmallA WHERE intkey = 0 UNION ALL SELECT 2, 'Hello2' FROM BQT1.SmallA WHERE IntKey = 1 ORDER BY X DESC"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUpdateWithElement() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.ADD_OP, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sql = "UPDATE BQT1.SmallA SET IntKey = IntKey + 1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"UPDATE BQT1.SmallA SET IntKey = (IntKey + 1)"}, //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testCase2187() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT t.intkey FROM (SELECT a.IntKey FROM bqt1.smalla a left outer join bqt1.smallb b on a.intkey=b.intkey, bqt1.smalla x) as t full outer JOIN bqt1.smallb c on t.intkey = c.intkey"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT a.IntKey FROM ((bqt1.smalla AS a LEFT OUTER JOIN bqt1.smallb AS b ON a.intkey = b.intkey) CROSS JOIN bqt1.smalla AS x) FULL OUTER JOIN bqt1.smallb AS c ON a.IntKey = c.intkey"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testMultiUnionMergeVirtual() throws Exception {
        String sql = "SELECT * FROM " +  //$NON-NLS-1$
            "(SELECT IntKey, 'a' AS s FROM (SELECT intkey, stringkey from BQT1.SmallA) as a union all " +  //$NON-NLS-1$
            "select IntKey, 'b' FROM (SELECT intkey, stringkey from BQT1.SmallA) as b union all " +  //$NON-NLS-1$
            "select IntKey, 'c' FROM (SELECT intkey, stringkey from BQT1.SmallA) as c " +  //$NON-NLS-1$
            ") AS x"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT g_2.IntKey AS c_0, 'a' AS c_1 FROM BQT1.SmallA AS g_2 UNION ALL SELECT g_1.IntKey AS c_0, 'b' AS c_1 FROM BQT1.SmallA AS g_1 UNION ALL SELECT g_0.IntKey AS c_0, 'c' AS c_1 FROM BQT1.SmallA AS g_0"}, //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testDefect16848_groupAliasNotSupported_1() {
        String sql = "SELECT sa.intkey, sa.objectvalue FROM bqt1.smalla AS sa WHERE (sa.intkey = 46) AND (sa.stringkey IN (46)) AND (sa.datevalue = /*+ no_unnest */ (SELECT MAX(sb.datevalue) FROM bqt1.smalla AS sb WHERE (sb.intkey = sa.intkey) AND (sa.stringkey = sb.stringkey) ))"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT BQT1.SmallA.datevalue, BQT1.SmallA.intkey, BQT1.SmallA.stringkey, BQT1.SmallA.objectvalue FROM BQT1.SmallA WHERE (BQT1.SmallA.intkey = 46) AND (BQT1.SmallA.stringkey = '46')"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
                                        0,      // DependentAccess
                                        1,      // DependentSelect
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

        ProcessorPlan subplan = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(((SelectNode)plan.getRootNode().getChildren()[0]).getCriteria()).get(0).getCommand().getProcessorPlan();

        // Collect atomic queries
        Set<String> actualQueries = getAtomicQueries(subplan);

        // Compare atomic queries
        HashSet<String> expectedQueries = new HashSet<String>(Arrays.asList(new String[] { "SELECT BQT1.SmallA.DateValue FROM BQT1.SmallA WHERE (BQT1.SmallA.IntKey = sa.IntKey) AND (BQT1.SmallA.StringKey = sa.StringKey)"})); //$NON-NLS-1$
        assertEquals("Did not get expected atomic queries for subplan: ", expectedQueries, actualQueries); //$NON-NLS-1$

        checkNodeTypes(subplan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
                                    });

    }

    @Test public void testFunctionOfAggregate1() {
        String sql = "SELECT SUM(IntKey) + 1 AS x FROM BQT1.SmallA GROUP BY IntKey"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT (SUM(IntKey) + 1) FROM BQT1.SmallA GROUP BY IntKey"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testFunctionOfAggregateCantPush1() {
        String sql = "SELECT SUM(IntKey) + 1 AS x FROM BQT1.SmallA GROUP BY IntKey"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
                                    });
    }

    @Test public void testFunctionOfAggregateCantPush3() {
        String sql = "SELECT avg(intkey) * 2 FROM BQT1.SmallA "; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT intkey FROM BQT1.SmallA"}, //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        1,      // Access
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
                                        0       // UnionAll
                                    });
    }

    private void helpTestCase2589NonPushdown(String sql, String[] expected) {

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, false);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$

        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      expected,
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
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

    private void helpTestCase2589(String sql, String expected) throws Exception {

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$

        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {expected},
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase2589() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589 ON MediumA.IntKey = SmallA_2589.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$

        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589a() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589a ON MediumA.IntKey = SmallA_2589a.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.IntKey = BQT1.SmallB.IntKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589b() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589b ON MediumA.IntKey = SmallA_2589b.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589c() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB, BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589b ON MediumA.IntKey = SmallA_2589b.IntKey " + //$NON-NLS-1$
                     "WHERE BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB INNER JOIN (BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10') ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589d() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB INNER JOIN " + //$NON-NLS-1$
                     "(BQT1.MediumA LEFT OUTER JOIN VQT.SmallA_2589b ON MediumA.IntKey = SmallA_2589b.IntKey) " + //$NON-NLS-1$
                     "ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB INNER JOIN (BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10') ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589e() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB LEFT OUTER JOIN " +  //$NON-NLS-1$
                     "(BQT1.MediumA LEFT OUTER JOIN VQT.SmallA_2589b ON MediumA.IntKey = SmallA_2589b.IntKey) " +  //$NON-NLS-1$
                     "ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB LEFT OUTER JOIN (BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10') ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589f() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB LEFT OUTER JOIN " + //$NON-NLS-1$
                     "(BQT1.MediumA INNER JOIN VQT.SmallA_2589b ON MediumA.IntKey = SmallA_2589b.IntKey) " + //$NON-NLS-1$
                     "ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB LEFT OUTER JOIN (BQT1.MediumA INNER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey) ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey AND BQT1.SmallA.StringNum = '10'";//$NON-NLS-1$";
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589g() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB LEFT OUTER JOIN " + //$NON-NLS-1$
                     "(BQT1.MediumA INNER JOIN VQT.SmallA_2589c ON MediumA.IntKey = SmallA_2589c.IntKey) " + //$NON-NLS-1$
                     "ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumB LEFT OUTER JOIN " + //$NON-NLS-1$
                     "(BQT1.MediumA INNER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB " + //$NON-NLS-1$
                     "ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey AND " +  //$NON-NLS-1$
                     "concat(BQT1.SmallA.StringNum, BQT1.SmallB.StringNum) = '1010') " +  //$NON-NLS-1$
                     "ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey) ON BQT1.MediumB.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589h() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN VQT.SmallA_2589c " + //$NON-NLS-1$
                     "ON MediumA.IntKey = SmallA_2589c.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey AND concat(BQT1.SmallA.StringNum, BQT1.SmallB.StringNum) = '1010') ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    @Test public void testCase2589i() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN VQT.SmallA_2589d " + //$NON-NLS-1$
                     "ON MediumA.IntKey = SmallA_2589d.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10' AND BQT1.SmallA.IntNum = 10"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * Test optimization doesn't happen if an outer join isn't involved
     */
    @Test public void testCase2589j() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA INNER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589 ON MediumA.IntKey = SmallA_2589.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA, BQT1.SmallA WHERE (BQT1.MediumA.IntKey = BQT1.SmallA.IntKey) AND (BQT1.SmallA.StringNum = '10')"; //$NON-NLS-1$

        helpTestCase2589(sql, expected);
    }

    /**
     * Test optimization doesn't happen if an outer join isn't involved
     */
    @Test public void testCase2589k() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA INNER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589b ON MediumA.IntKey = SmallA_2589b.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA, BQT1.SmallA, BQT1.SmallB WHERE (BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) AND (BQT1.MediumA.IntKey = BQT1.SmallA.IntKey) AND (BQT1.SmallA.StringNum = '10')"; //$NON-NLS-1$


        helpTestCase2589(sql, expected);
    }

    /**
     * Same as testCase2589 except right outer join
     */
    @Test public void testCase2589l() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM VQT.SmallA_2589 RIGHT OUTER JOIN " + //$NON-NLS-1$
                     "BQT1.MediumA ON MediumA.IntKey = SmallA_2589.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$

        helpTestCase2589(sql, expected);
    }

    /**
     * Same as testCase2589 except full outer join - criteria "below" full outer join cannot be
     * raised into the join criteria, so basically the virtual groups cannot be merged in this test.
     */
    @Test public void testCase2589m() {
        String sql = "SELECT BQT1.MediumA.IntKey FROM VQT.SmallA_2589 FULL OUTER JOIN " + //$NON-NLS-1$
                     "BQT1.MediumA ON MediumA.IntKey = SmallA_2589.IntKey"; //$NON-NLS-1$

        String[] expected = new String[] {
            "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA",  //$NON-NLS-1$
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE StringNum = '10'" //$NON-NLS-1$
        };

        helpTestCase2589NonPushdown(sql, expected);
    }

    /**
     * Same as testCase2589b except full outer join
     */
    @Test public void testCase2589n() {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA FULL OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589b ON MediumA.IntKey = SmallA_2589b.IntKey"; //$NON-NLS-1$

        String[] expected = new String[] {
            "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA", //$NON-NLS-1$
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA, BQT1.SmallB WHERE (BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) AND (BQT1.SmallA.StringNum = '10')" //$NON-NLS-1$
        };
        helpTestCase2589NonPushdown(sql, expected);

    }

    /**
     * Same as testCase2589 except with two virtual layers instead of one
     */
    @Test public void testCase2589o() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589f ON MediumA.IntKey = SmallA_2589f.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$

        helpTestCase2589(sql, expected);
    }

    /**
     * Same as testCase2589b except with two virtual layers instead of one
     */
    @Test public void testCase2589p() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589g ON MediumA.IntKey = SmallA_2589g.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * Test 3 frames, where top frame has outer join, middle frame has inner join, and
     * bottom frame has criteria that must be made into join criteria.
     */
    @Test public void testCase2589q() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589h ON MediumA.IntKey = SmallA_2589h.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * Similar to testCase2589b, except virtual transformation has criteria on an
     * element from each physical table
     */
    @Test public void testCase2589r() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589i ON MediumA.IntKey = SmallA_2589i.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.StringKey = BQT1.SmallB.StringKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10' AND BQT1.SmallB.StringNum = '10'"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * Test user criteria that should NOT be moved into join clause
     */
    @Test public void testCase2589s() throws Exception {
        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589 ON MediumA.IntKey = SmallA_2589.IntKey " + //$NON-NLS-1$
                     "WHERE BQT1.MediumA.IntNum = 10"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10' WHERE BQT1.MediumA.IntNum = 10"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * Test user criteria that should NOT be moved into join clause
     */
    @Test public void testCase2589t() throws Exception {
        String sql = "SELECT z.IntKey FROM (SELECT IntKey FROM BQT1.MediumA WHERE BQT1.MediumA.IntNum = 10) as z " + //$NON-NLS-1$
                     "LEFT OUTER JOIN " + //$NON-NLS-1$
                     "VQT.SmallA_2589 ON z.IntKey = SmallA_2589.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10' WHERE BQT1.MediumA.IntNum = 10"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * The above test written with an inline view instead of a virtual group.
     * This test translates to - how can this query be rewritten without subqueries such
     * that the same results are produced?  More specifically, where should the criteria
     * go - WHERE clause or FROM clause?
     */
    @Test public void testCase2589u() throws Exception {
        String sql = "SELECT z.IntKey FROM (SELECT IntKey FROM BQT1.MediumA WHERE IntNum = 10) as z " + //$NON-NLS-1$
                     "LEFT OUTER JOIN " + //$NON-NLS-1$
                     "(SELECT IntKey FROM BQT1.SmallA WHERE StringNum = '10') as y " + //$NON-NLS-1$
                     "ON z.IntKey = y.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10' WHERE BQT1.MediumA.IntNum = 10"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * Same sql as testCase2589, but the model doesn't support outer joins, so
     * case 2589 optimization shouldn't happen.
     */
    @Test public void testCase2589v() {

        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
        "VQT.SmallA_2589 ON MediumA.IntKey = SmallA_2589.IntKey"; //$NON-NLS-1$

        String expected[] = new String[] {
            "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA", //$NON-NLS-1$
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE StringNum = '10'" //$NON-NLS-1$
        };


        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, false);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$

        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      expected,
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        1,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });
    }

    /**
     * Same as previous testCase2589v, but with full outer join.
     */
    @Test public void testCase2589w() {

        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA FULL OUTER JOIN " + //$NON-NLS-1$
        "VQT.SmallA_2589 ON MediumA.IntKey = SmallA_2589.IntKey"; //$NON-NLS-1$

        String expected[] = new String[] {
            "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA", //$NON-NLS-1$
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE StringNum = '10'" //$NON-NLS-1$
        };

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, false);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$

        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      expected,
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        1,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });
    }

    /**
     * Test a complicated join tree involving multiple models, but with a nested
     * outer join predicate spanning only one model, and see if the case 2589
     * fix happens.  The important thing is the criteria "StringNum = '10'" needs
     * to be put in the join criteria, not the where clause, of the second atomic
     * query, because in the user query it is on the inner side of an outer join.
     */
    @Test public void testCase2589x() throws Exception {

        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT2.SmallA INNER JOIN " + //$NON-NLS-1$
        "(BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
        "(SELECT IntKey FROM BQT1.SmallA WHERE StringNum = '10') as y " + //$NON-NLS-1$
        "ON MediumA.IntKey = y.IntKey) " + //$NON-NLS-1$
        "ON BQT2.SmallA.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$

        String expected[] = new String[] {
            "SELECT BQT2.SmallA.IntKey FROM BQT2.SmallA", //$NON-NLS-1$
            "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'" //$NON-NLS-1$
        };


        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$

        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      expected,
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        1,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });
    }

    /**
     * Test two outer joins, one nested within the other, all pushable to one source,
     * with inline views having criteria that each need to be migrated to their
     * respective join predicate join criteria.
     *
     * The tree below illustrates the canonical plan (plus access nodes).  'y' and
     * 'z' are two inline views.  Notice each has a SELECT node underneath - the
     * criteria represented by each of those SELECT nodes is on the inner side of
     * their respective left outer joins (LOJ).  So, each criteria needs to be
     * migrated to the join criteria.
     *
     * <pre>
     *          LOJ
     *         /    \
     *      LOJ      SRC z
     *    /    \       |
     *  SRC    SRC y  SEL
     *  MedB    |      |
     *         SEL    ACC
     *          |      |
     *         ACC    SRC SmA
     *          |
     *         SRC MedA
     * </pre>
     * Here's a diagram of what the join plan of the resulting atomic query should
     * look like.
     * <pre>
     *          ACC
     *           |
     *          LOJ**
     *         /    \
     *      LOJ**    SRC       **criteria migrated to here
     *    /    \     SmA
     *  SRC    SRC
     *  MedB   MedA
     * </pre>
     */
    @Test public void testCase2589y() throws Exception {
        String sql = "SELECT L.IntKey, y.IntKey, z.IntKey " + //$NON-NLS-1$
                     "FROM (BQT1.MediumB as L LEFT OUTER JOIN " + //$NON-NLS-1$
                     "(SELECT IntKey FROM BQT1.MediumA as M WHERE M.IntNum = 4) as y ON y.IntKey = L.IntKey) " + //$NON-NLS-1$
                     "LEFT OUTER JOIN (SELECT IntKey FROM BQT1.SmallA as S WHERE S.StringNum = '10') as z " + //$NON-NLS-1$
                     "ON z.IntKey = y.IntKey"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumB.IntKey, BQT1.MediumA.IntKey, BQT1.SmallA.IntKey " + //$NON-NLS-1$
                     "FROM (BQT1.MediumB LEFT OUTER JOIN " + //$NON-NLS-1$
                     "BQT1.MediumA ON BQT1.MediumA.IntKey = BQT1.MediumB.IntKey AND BQT1.MediumA.IntNum = 4) " + //$NON-NLS-1$
                     "LEFT OUTER JOIN BQT1.SmallA " + //$NON-NLS-1$
                     "ON BQT1.SmallA.IntKey = BQT1.MediumA.IntKey AND BQT1.SmallA.StringNum = '10'"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * Test a complicated join tree involving multiple models, but with a nested
     * outer join predicate spanning only one model, and see if the case 2589
     * fix happens.  The important thing is the criteria "StringNum = '10'" needs
     * to be put in the join criteria, not the where clause, of the second atomic
     * query, because in the user query it is on the inner side of an outer join.
     */
    @Test public void testCase2589z() {

        String sql = "SELECT BQT1.MediumA.IntKey FROM BQT2.SmallA INNER JOIN " + //$NON-NLS-1$
        "(BQT1.MediumA LEFT OUTER JOIN " + //$NON-NLS-1$
        "(SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA, BQT1.SmallB " + //$NON-NLS-1$
        "WHERE BQT1.SmallA.IntKey = BQT1.SmallB.IntKey AND BQT1.SmallA.StringNum = '10') as y " + //$NON-NLS-1$
        "ON MediumA.IntKey = y.IntKey) " + //$NON-NLS-1$
        "ON BQT2.SmallA.IntKey = BQT1.MediumA.IntKey"; //$NON-NLS-1$

        String expected[] = new String[] {
            "SELECT BQT2.SmallA.IntKey FROM BQT2.SmallA", //$NON-NLS-1$
            "SELECT BQT1.MediumA.IntKey FROM BQT1.MediumA LEFT OUTER JOIN (BQT1.SmallA INNER JOIN BQT1.SmallB ON BQT1.SmallA.IntKey = BQT1.SmallB.IntKey) ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10'" //$NON-NLS-1$
        };


        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$

        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      expected,
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        1,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });
    }

    /**
     * Union with multiple joins underneath
     */
    @Test public void testCase2589aa() throws Exception {
        String sql = "SELECT * FROM (SELECT z.IntKey FROM (SELECT IntKey FROM BQT1.MediumA WHERE IntNum = 10) as z " + //$NON-NLS-1$
                     "LEFT OUTER JOIN " + //$NON-NLS-1$
                     "(SELECT IntKey FROM BQT1.SmallA WHERE StringNum = '10') as y " + //$NON-NLS-1$
                     "ON z.IntKey = y.IntKey " + //$NON-NLS-1$
                    "UNION ALL SELECT z.IntKey FROM (SELECT IntKey FROM BQT1.MediumA WHERE IntNum = 10) as z " + //$NON-NLS-1$
                    "LEFT OUTER JOIN " + //$NON-NLS-1$
                    "(SELECT IntKey FROM BQT1.SmallA WHERE StringNum = '10') as y " + //$NON-NLS-1$
                    "ON z.IntKey = y.IntKey) as x"; //$NON-NLS-1$

        String expected = "SELECT BQT1.MediumA.IntKey AS c_0 FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10' WHERE BQT1.MediumA.IntNum = 10 UNION ALL SELECT BQT1.MediumA.IntKey AS c_0 FROM BQT1.MediumA LEFT OUTER JOIN BQT1.SmallA ON BQT1.MediumA.IntKey = BQT1.SmallA.IntKey AND BQT1.SmallA.StringNum = '10' WHERE BQT1.MediumA.IntNum = 10"; //$NON-NLS-1$
        helpTestCase2589(sql, expected);
    }

    /**
     * A final rewrite will ensure the correct order by
     */
    @Test public void testOrderByDuplicates() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT intkey, x FROM (select intkey, intkey x from bqt1.smalla) z ORDER BY x, intkey"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 ORDER BY g_0.IntKey"},  //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    //Test use of OrderBy with expression
    @Test public void testCase2507() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT vqt.smallb.a12345 FROM vqt.smallb ORDER BY vqt.smallb.a12345"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT Concat(stringKey, stringNum) AS EXPR FROM BQT1.SmallA ORDER BY EXPR"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase2507A() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) as EXPR, bqt1.smalla.stringKey as EXPR_1 FROM bqt1.smalla  ORDER BY EXPR, EXPR_1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) AS EXPR, bqt1.smalla.stringKey AS EXPR_1 FROM bqt1.smalla ORDER BY EXPR, EXPR_1"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase2507B() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum), bqt1.smalla.stringKey as EXPR_1 FROM bqt1.smalla ORDER BY EXPR_1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum), bqt1.smalla.stringKey AS EXPR_1 FROM bqt1.smalla ORDER BY EXPR_1"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * RulePlanJoins does not initially allow the cross join push.
     * The subsequent RuleRaiseAccess does since we believe it was the intent of the user
     */
    @Test public void testPushCrossJoins() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT b1.intkey from (bqt1.SmallA a1 cross join bqt1.smalla a2 cross join bqt1.mediuma b1) " +    //$NON-NLS-1$
            " left outer join bqt1.mediumb b2 on b1.intkey = b2.intkey"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_2.IntKey FROM (BQT1.SmallA AS g_0 CROSS JOIN BQT1.SmallA AS g_1) CROSS JOIN (BQT1.MediumA AS g_2 LEFT OUTER JOIN BQT1.MediumB AS g_3 ON g_2.IntKey = g_3.IntKey)"},  //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase3023() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT bqt1.SmallA.intkey from (bqt1.SmallA inner join (" //$NON-NLS-1$
            + "SELECT BAD.intkey from bqt1.SmallB as BAD left outer join bqt1.MediumB on BAD.intkey = bqt1.MediumB.intkey) as X on bqt1.SmallA.intkey = X.intkey) inner join bqt1.MediumA on X.intkey = bqt1.MediumA.intkey"; //$NON-NLS-1$

         helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT bqt1.SmallA.intkey FROM (bqt1.SmallA INNER JOIN (bqt1.SmallB AS BAD LEFT OUTER JOIN bqt1.MediumB ON BAD.intkey = bqt1.MediumB.intkey) ON bqt1.SmallA.intkey = BAD.intkey) INNER JOIN bqt1.MediumA ON BAD.intkey = bqt1.MediumA.intkey"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED );
    }

    @Test public void testCase3367() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = example1();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select e1 from pm1.g1 where pm1.g1.e1 IN /*+ no_unnest */ (SELECT pm1.g2.e1 FROM pm1.g2 WHERE (pm1.g1.e1 = 2))", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN /*+ NO_UNNEST */ (SELECT g_1.e1 FROM pm1.g2 AS g_1 WHERE g_0.e1 = '2')" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /*
     * Set criteria was not getting pushed down correctly when there was a self-join
     * of a virtual table containing a join in it's transformation.  All virtual
     * models use the same physical model pm1.
     */
    @Test public void testCase3778() throws Exception {

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(
                "select a.e1, b.e1 from vm2.g1 a, vm2.g1 b where a.e1 = b.e1 and a.e2 in /*+ no_unnest */ (select e2 from vm1.g1)",  //$NON-NLS-1$
                metadata, null, capFinder, new String[] {"SELECT g_0.e1, g_2.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1, pm1.g1 AS g_2, pm1.g2 AS g_3 WHERE (g_2.e2 = g_3.e2) AND (g_0.e2 = g_1.e2) AND (g_0.e1 = g_2.e1) AND (g_0.e2 IN /*+ NO_UNNEST */ (SELECT g_4.e2 FROM pm1.g1 AS g_4))"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Ensures that order by expressions are not repeated when multiple criteria span a merge join
     */
    @Test public void testCase3832() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "select bqt1.smalla.intkey from bqt1.smalla, bqt2.smalla, bqt2.smallb where bqt1.smalla.intkey = bqt2.smalla.intkey and bqt1.smalla.intkey = bqt2.smallb.intkey and bqt2.smalla.stringkey = bqt2.smallb.stringkey"; //$NON-NLS-1$

        helpPlan(sql,
                 metadata,
                 null,
                 capFinder,
                 new String[] {
                     "SELECT BQT2.SmallB.IntKey, BQT2.SmallA.IntKey FROM BQT2.SmallA, BQT2.SmallB WHERE BQT2.SmallA.StringKey = BQT2.SmallB.StringKey ORDER BY BQT2.SmallB.IntKey, BQT2.SmallA.IntKey",
                     "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA ORDER BY BQT1.SmallA.IntKey"}, //$NON-NLS-1$
                 ComparisonMode.EXACT_COMMAND_STRING);

    }

    /*
     * Functions containing exec statements should not be evaluated
     */
    @Test public void testDefect21972() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1 where e1 = convert((exec pm1.sq11(e2, 2)), integer)"; //$NON-NLS-1$

        helpPlan(sql,
                 RealMetadataFactory.example1Cached(),
                 null,
                 capFinder,
                 new String[] {
                     "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, //$NON-NLS-1$
                 SHOULD_SUCCEED);

    }

    @Test public void testExpressionSymbolPreservation() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT * from (select '1' as test, intkey from bqt2.smalla) foo, (select '2' as test, intkey from bqt2.smalla) foo2 where foo.intkey = foo2.intkey"; //$NON-NLS-1$

        helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.IntKey, g_1.IntKey FROM BQT2.SmallA AS g_0, BQT2.SmallA AS g_1 WHERE g_0.IntKey = g_1.IntKey"},  //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

    }

    //since this does not support convert, it should not be collapsed
    @Test public void testBadCollapseUnion() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "select convert(e2+1,string) from pm1.g1 union all select e1 from pm1.g2";//$NON-NLS-1$
        String[] expectedSql = new String[] {"SELECT e2 FROM pm1.g1", "SELECT e1 FROM pm1.g2"};//$NON-NLS-1$ //$NON-NLS-2$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(),
                                      null, capFinder, expectedSql, SHOULD_SUCCEED);

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
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
                                        0,      // Select
                                        0,      // Sort
                                        1       // UnionAll
        });

    }

    @Test public void testCase3966() {
        ProcessorPlan plan = helpPlan("insert into vm1.g37 (e1, e2, e3, e4) values('test', 1, convert('true', boolean) , convert('12', double) )", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                                      new String[] {"INSERT INTO pm4.g1 (pm4.g1.e1, pm4.g1.e2, pm4.g1.e3, pm4.g1.e4) VALUES ('test', 1, TRUE, 12.0)"} );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /*
     * Select literals created by runtime evaluation should not be pushed down.
     */
    @Test public void testCase4017() throws Exception {

        String sql = "SELECT env('soap_host') AS HOST, intkey from bqt2.smalla"; //$NON-NLS-1$

        helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(),
                                      new String[] {"SELECT BQT2.SmallA.IntKey FROM BQT2.SmallA"}, ComparisonMode.EXACT_COMMAND_STRING);  //$NON-NLS-1$
    }

    /**
     * Test of RuleCopyCriteria.  Criteria should NOT be copied across a join if the join has any other operator
     * other than an equality operator, but if the single group criteria is equality, then we can copy into a join criteria
     */
    @Test public void testCase4265() throws Exception {
        String sql = "SELECT X.intkey, Y.intkey FROM BQT1.SmallA X, BQT1.SmallA Y WHERE X.IntKey <> Y.IntKey and Y.IntKey = 1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                      new String[] {
                                          "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey <> 1",
                                          "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey = 1" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
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
     * Test of RuleCopyCriteria.  Criteria should be copied across a join only for an equality operator in
     * the join criteria.
     */
    @Test public void testCase4265ControlTest() throws Exception {
        String sql = "SELECT X.intkey, Y.intkey FROM BQT1.SmallA X, BQT1.SmallA Y WHERE X.IntKey = Y.IntKey and Y.IntKey = 1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                      new String[] {
                                          "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey = 1" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
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
     * The bug was in FrameUtil.convertCriteria() method, where ExistsCriteria was not being checked for.
     */
    @Test public void testExistsCriteriaInSelect() {
        String sql = "select intkey, case when exists (select stringkey from bqt1.smallb where intkey = vqt.smalla.intkey) then 'nuge' end as a from vqt.smalla"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                      new String[] {
                                          "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA" }); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            1,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

    }

    /**
     * Try substituting "is not null" for "exists" criteria
     */
    @Test public void testScalarSubQueryInSelect() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select intkey, case when (select stringkey from bqt1.smallb) is not null then 'nuge' end as a from vqt.smalla"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                      new String[] {
                                          "SELECT g_0.IntKey, CASE WHEN (SELECT g_0.StringKey FROM BQT1.SmallB AS g_0) IS NOT NULL THEN 'nuge' END FROM BQT1.SmallA AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
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
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey FROM BQT1.SmallB AS g_0", Arrays.asList("a"));
        hdm.addData("SELECT g_0.IntKey FROM BQT1.SmallA AS g_0", Arrays.asList(1));
        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1, "nuge")});

    }

    @Test public void testScalarSubQueryInSelect1() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select intkey, case when (select stringkey from bqt1.smallb where intkey = vqt.smalla.intkey) is not null then 'nuge' end as a from vqt.smalla"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(),
                                      new String[] {
                                          "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            1,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

    }

    @Test public void testCase4263() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = example1();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select vm1.g1.e1 from vm1.g1 left outer join (select * from vm1.g2 as v where v.e1 = /*+ no_unnest */ (select max(vm1.g2.e1) from vm1.g2 where v.e1 = vm1.g2.e1)) f2 on (f2.e1 = vm1.g1.e1)", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e1 = /*+ NO_UNNEST */ (SELECT MAX(g_1.e1) FROM pm1.g1 AS g_1 WHERE g_1.e1 = g_0.e1) ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        caps.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, true);

        plan = helpPlan("select vm1.g1.e1 from vm1.g1 left outer join (select * from vm1.g2 as v where v.e1 = /*+ no_unnest */ (select max(vm1.g2.e1) from vm1.g2 where v.e1 = vm1.g2.e1)) f2 on (f2.e1 = vm1.g1.e1)", metadata,  //$NON-NLS-1$
                null, capFinder,
                new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g1 AS g_1 ON g_1.e1 = g_0.e1 AND g_1.e1 = /*+ NO_UNNEST */ (SELECT MAX(g_2.e1) FROM pm1.g1 AS g_2 WHERE g_2.e1 = g_1.e1)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }


    @Test public void testCase4263a() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = example1();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpPlan("select vm1.g1.e1 from vm1.g1 left outer join (select * from vm1.g2 as v where v.e1 in /*+ no_unnest */ (select vm1.g2.e1 from vm1.g2)) f2 on (f2.e1 = vm1.g1.e1)", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN /*+ NO_UNNEST */ (SELECT g_1.e1 FROM pm1.g1 AS g_1) ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCase4263b() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = example1();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select vm1.g1.e1 from vm1.g1 left outer join (select * from vm1.g2 as v where v.e1 = /*+ no_unnest */ (select max(pm2.g1.e1) from pm2.g1 where v.e1 = pm2.g1.e1)) f2 on (f2.e1 = vm1.g1.e1)", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT g1__1.e1 FROM pm1.g1 AS g1__1" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testCase4279() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = example1();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select * from (select v1.e1, v2.e1 as e1_1, v1.e2, v2.e2 as e2_2 from (select * from vm1.g7 where vm1.g7.e2 = 1) v1 left outer join (select * from vm1.g7 where vm1.g7.e2 = 1) v2 on v1.e2 = v2.e2) as v3 where v3.e2 = 1", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT CASE WHEN g_0.e1 = 'S' THEN 'Pay' WHEN g_0.e1 = 'P' THEN 'Rec' ELSE g_0.e1 END, CASE WHEN g_1.e1 = 'S' THEN 'Pay' WHEN g_1.e1 = 'P' THEN 'Rec' ELSE g_1.e1 END, g_0.e2, g_1.e2 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g1 AS g_1 ON g_0.e2 = g_1.e2 AND g_1.e2 = 1 WHERE g_0.e2 = 1" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase4312() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpPlan("select ? + 1, pm1.g1.e1 AS EXPR_1 FROM pm1.g1", example1(), null, capFinder, //$NON-NLS-1$
                 new String[] {
                     "SELECT (? + 1) AS expr, pm1.g1.e1 FROM pm1.g1"}, true); //$NON-NLS-1$

    }

    @Test public void testCase2507_2(){

        String sql = "SELECT a FROM (SELECT concat(BQT1.SmallA.StringKey, BQT1.SmallA.StringNum) as a " +  //$NON-NLS-1$
                     "FROM BQT1.SmallA, BQT1.SmallB WHERE SmallA.IntKey = SmallB.IntKey) as X ORDER BY X.a"; //$NON-NLS-1$

        String expected = "SELECT concat(BQT1.SmallA.StringKey, BQT1.SmallA.StringNum) AS EXPR " +  //$NON-NLS-1$
                     "FROM BQT1.SmallA, BQT1.SmallB WHERE BQT1.SmallA.IntKey = BQT1.SmallB.IntKey ORDER BY EXPR";  //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {expected},
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    private void helpTestCase2430and2507(String sql, String expected) {

        // TEST PLANNING

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {expected},
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    //Test use of OrderBy with Alias
    @Test public void testCase2430D() {
        String sql = "SELECT bqt1.smalla.longnum + bqt1.smalla.longnum as c1234567890123456789012345678901234567890, " + //$NON-NLS-1$
                     "bqt1.smalla.doublenum as EXPR FROM bqt1.smalla ORDER BY c1234567890123456789012345678901234567890, EXPR "; //$NON-NLS-1$

        String expected = "SELECT (bqt1.smalla.longnum + bqt1.smalla.longnum) AS c1234567890123456789012345678901234567890, bqt1.smalla.doublenum AS EXPR " + //$NON-NLS-1$
                     "FROM bqt1.smalla ORDER BY c1234567890123456789012345678901234567890, EXPR"; //$NON-NLS-1$
        helpTestCase2430and2507(sql, expected);
    }

    @Test public void testCase2430E() {
        String sql = "SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) as c1234567890123456789012345678901234567890, " + //$NON-NLS-1$
                     "CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) AS EXPR FROM bqt1.smalla ORDER BY c1234567890123456789012345678901234567890, EXPR "; //$NON-NLS-1$

        String expected = "SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) AS c_0 " + //$NON-NLS-1$
                     "FROM bqt1.smalla ORDER BY c_0"; //$NON-NLS-1$
        helpTestCase2430and2507(sql, expected);
    }

    @Test public void testCase2430G() {
        String sql = "SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) as c1234567890123456789012345678901234567890, " + //$NON-NLS-1$
                     "CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) AS EXPR FROM bqt1.smalla ORDER BY c1234567890123456789012345678901234567890"; //$NON-NLS-1$

        String expected = "SELECT CONCAT(bqt1.smalla.stringKey, bqt1.smalla.stringNum) AS c_0 " + //$NON-NLS-1$
                     "FROM bqt1.smalla ORDER BY c_0"; //$NON-NLS-1$
        helpTestCase2430and2507(sql, expected);
    }

    @Test public void testCase2507_1(){

        String sql = "SELECT a FROM (SELECT concat(BQT1.SmallA.StringKey, BQT1.SmallA.StringNum) as a " +  //$NON-NLS-1$
                     "FROM BQT1.SmallA) as X ORDER BY X.a"; //$NON-NLS-1$

        String expected = "SELECT concat(BQT1.SmallA.StringKey, BQT1.SmallA.StringNum) AS EXPR FROM BQT1.SmallA ORDER BY EXPR";  //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {expected},
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * This is taken from testPushCorrelatedSubquery1.  However this subquery is not expected to be pushed down since the correlated
     * reference expression cannot be evaluated by the source.
     */
    @Test public void testDefect23614() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        //the correlated reference expression cannot be pre-evaluated
        ProcessorPlan plan = helpPlan("SELECT intkey FROM bqt1.smalla AS n WHERE intkey = /*+ no_unnest */ (SELECT MAX(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = concat(n.stringkey, s.stringnum) )", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT intkey, n.stringkey FROM bqt1.smalla AS n" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

        //the correlated reference expression can be pre-evaluated - should still prevent pushdown
        plan = helpPlan("SELECT intkey FROM bqt1.smalla AS n WHERE intkey = /*+ no_unnest */ (SELECT MAX(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = concat(n.stringkey, 'a') )", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT intkey, n.stringkey FROM bqt1.smalla AS n" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Normally the following queries would plan as if they were federated, but setting the connector_id source property
     * allows them to be planned as if they were the same source.
     */
    @Test public void testSameConnector() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setSourceProperty(Capability.CONNECTOR_ID, "1"); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(
            "SELECT A.IntKey, B.IntKey FROM BQT1.SmallA A LEFT OUTER JOIN BQT2.MediumB B ON A.IntKey = B.IntKey",  //$NON-NLS-1$
            metadata, null, capFinder,
            new String[] {
              "SELECT A.IntKey, B.IntKey FROM BQT1.SmallA AS A LEFT OUTER JOIN BQT2.MediumB AS B ON A.IntKey = B.IntKey"},  //$NON-NLS-1$
              true);

        checkNodeTypes(plan, FULL_PUSHDOWN);

        plan = helpPlan(
              "SELECT A.IntKey FROM BQT1.SmallA A UNION select B.intkey from BQT2.MediumB B",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] {
                "SELECT A.IntKey FROM BQT1.SmallA AS A UNION SELECT B.intkey FROM BQT2.MediumB AS B"},  //$NON-NLS-1$
                true);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Test changes to RuleCollapseSource for removing aliases
     */
    @Test public void testCase4898() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(
              "SELECT 'a' as A FROM BQT1.SmallA A UNION select 'b' as B from BQT1.MediumB B",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] {
                "SELECT 'a' AS A FROM BQT1.SmallA AS A UNION SELECT 'b' FROM BQT1.MediumB AS B"},  //$NON-NLS-1$
                true);

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testDefect13971() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "select b from (select distinct booleanvalue b, intkey from bqt1.smalla) as x"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, new String[] {"SELECT DISTINCT booleanvalue, intkey FROM bqt1.smalla"}, SHOULD_SUCCEED); //$NON-NLS-1$

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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Ensures that aliases are not stripped from projected symbols if they might conflict with an order by element
     */
    @Test public void testCase5067() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT a.intkey as stringkey, b.stringkey as key2 from bqt1.smalla a, bqt1.smallb b where a.intkey = b.intkey order by stringkey"; //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, new String[] {"SELECT a.intkey AS stringkey, b.stringkey AS key2 FROM bqt1.smalla AS a, bqt1.smallb AS b WHERE a.intkey = b.intkey ORDER BY stringkey"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushConvertObject() throws TeiidComponentException, TeiidProcessingException {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(
            "SELECT intkey from bqt1.smalla WHERE stringkey = convert(objectvalue, string)",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE BQT1.SmallA.StringKey = convert(BQT1.SmallA.ObjectValue, string)"}, //$NON-NLS-1$
            ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushConvertClobToString() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("LOB", caps); //$NON-NLS-1$

        // Add join capability to pm1
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = helpPlan(
            "SELECT ClobValue from LOB.LobTbl WHERE convert(ClobValue, string) = ?",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT LOB.LobTbl.ClobValue FROM LOB.LobTbl WHERE convert(LOB.LobTbl.ClobValue, string) = ?"}, //$NON-NLS-1$
            SHOULD_SUCCEED );

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSelectIntoWithDistinct() throws Exception {
        String sql = "select distinct e1 into #temp from pm1.g1"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(sql, metadata, new String[] {"SELECT DISTINCT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        //txn required, since the insert could be split
        assertTrue(plan.requiresTransaction(false));

        checkNodeTypes(plan, FULL_PUSHDOWN);

        checkNodeTypes(plan, new int[] {1}, new Class[] {ProjectIntoNode.class});
    }

    @Test public void testInsertQueryExpression() throws Exception {
        String sql = "insert into pm1.g1 (e1) select e1 from pm1.g2"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(sql, metadata, new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        //requires a txn, since an non pushdown/iterated insert is used
        assertTrue(plan.requiresTransaction(false));

        checkNodeTypes(plan, new int[] {1}, new Class[] {ProjectIntoNode.class});
    }

    /**
     * Ensure that the pushdown check doesn't fail
     * @throws Exception
     */
    @Test public void testInsertQueryExpression1() throws Exception {
        String sql = "insert into pm1.g1 (e1) select e1 || 1 from pm1.g2"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(sql, metadata, new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        //requires a txn, since an non pushdown/iterated insert is used
        assertTrue(plan.requiresTransaction(false));

        checkNodeTypes(plan, new int[] {1}, new Class[] {ProjectIntoNode.class});

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setSourceProperty(Capability.TRANSACTION_SUPPORT, TransactionSupport.NONE);
        plan = helpPlan(sql, metadata, new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        //requires a txn, since an non pushdown/iterated insert is used
        assertFalse(plan.requiresTransaction(false));

        checkNodeTypes(plan, new int[] {1}, new Class[] {ProjectIntoNode.class});
    }

    /**
     * previously the subqueries were being pushed too far and then not having the appropriate correlated references
     */
    @Test public void testCorrelatedSubqueryOverJoin() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        String sql = "select pm1.g1.e1 from pm1.g1, (select * from pm1.g2) y where (pm1.g1.e1 = y.e1) and exists (select e2 from pm1.g2 where e1 = y.e1) and exists (select e3 from pm1.g2 where e1 = y.e1)"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder,
                new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (EXISTS (SELECT g_2.e2 FROM pm1.g2 AS g_2 WHERE g_2.e1 = g_1.e1)) AND (EXISTS (SELECT g_3.e3 FROM pm1.g2 AS g_3 WHERE g_3.e1 = g_1.e1))"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * see testSimpleCrossJoin3
     */
    @Test public void testMaxFromGroups() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.MAX_QUERY_FROM_GROUPS, new Integer(1));
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        helpPlan("select pm2.g1.e1 FROM pm2.g1 CROSS JOIN pm2.g2", example1(), null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1 FROM pm2.g1", "SELECT 1 FROM pm2.g2"}, true ); //$NON-NLS-1$ //$NON-NLS-2$

    }

    @Test public void testMaxProjectedColumns() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.MAX_QUERY_PROJECTED_COLUMNS, 2);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        helpPlan("select pm2.g1.e1, pm2.g1.e2, pm2.g2.e1 FROM pm2.g1 CROSS JOIN pm2.g2", example1(), null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm2.g2 AS g_0", "SELECT g_0.e1, g_0.e2 FROM pm2.g1 AS g_0"}, true ); //$NON-NLS-1$ //$NON-NLS-2$

    }

    @Test public void testCase6249() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        String sql = "select count(*) from (select intkey from bqt1.smalla union all select intkey from bqt1.smallb) as a"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT COUNT(*) FROM (SELECT 1 FROM bqt1.smalla UNION ALL SELECT 1 FROM bqt1.smallb) AS a"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase6181() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "select a.e1 from (select 1 e1) a, (select e1, 1 as a, x from (select e1, CASE WHEN e1 = 'a' THEN e2 ELSE e3 END as x from pm1.g2) y group by e1, x) b where a.e1 = b.x"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder,
                                      new String[] {"SELECT 1 FROM (SELECT CASE WHEN g_0.e1 = 'a' THEN g_0.e2 ELSE convert(g_0.e3, integer) END AS c_0 FROM pm1.g2 AS g_0 GROUP BY g_0.e1, CASE WHEN g_0.e1 = 'a' THEN g_0.e2 ELSE convert(g_0.e3, integer) END) AS v_0 WHERE v_0.c_0 = 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase6325() {
        String sql = "select e1 into #temp from pm4.g1 where e1='1'"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(sql, metadata, new String[] {"SELECT e1 FROM pm4.g1 WHERE e1 = '1'"}); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase6364() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sql = "select * from (SELECT 1+ SUM(intnum) AS s FROM bqt1.smalla) a WHERE a.s>10"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT (1 + SUM(intnum)) FROM bqt1.smalla HAVING SUM(intnum) > 9"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testExceptPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_EXCEPT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "select e1 from pm1.g1 except select e1 from pm1.g2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_1.e1 AS c_0 FROM pm1.g1 AS g_1 EXCEPT SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCase6597() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Create query
        String sql = "select IntKey from bqt1.smalla where stringkey not like '2%'"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT stringkey, IntKey FROM bqt1.smalla"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$

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

    @Test public void testCopyCriteriaWithIsNull() {
        String sql = "select * from (select a.intnum, a.intkey y, b.intkey from bqt1.smalla a, bqt2.smalla b where a.intkey = b.intkey) x where intkey is null"; //$NON-NLS-1$

        helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {});
    }

    /**
     * Test <code>QueryOptimizer</code>'s ability to plan a fully-pushed-down
     * query containing a <code>BETWEEN</code> comparison in the queries
     * <code>WHERE</code> statement.
     * <p>
     * For example:
     * <p>
     * SELECT * FROM pm1.g1 WHERE e2 BETWEEN 1 AND 2
     */
    @Test public void testBetween() {
        helpPlan("select * from pm1.g1 where e2 between 1 and 2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                new String[] { "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 WHERE (e2 >= 1) AND (e2 <= 2)"} ); //$NON-NLS-1$
    }

    /**
     * Test <code>QueryOptimizer</code>'s ability to plan a fully-pushed-down
     * query containing a <code>CASE</code> expression in which a
     * <code>BETWEEN</code> comparison is used in the queries
     * <code>SELECT</code> statement.
     * <p>
     * For example:
     * <p>
     * SELECT CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END FROM pm1.g1
     */
    @Test public void testBetweenInCase() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpPlan("select case when e2 between 3 and 5 then e2 else -1 end from pm1.g1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(), null, capFinder,
                new String[] { "SELECT CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END FROM pm1.g1"},  //$NON-NLS-1$
                TestOptimizer.SHOULD_SUCCEED);
    }

    /**
     * Test <code>QueryOptimizer</code>'s ability to plan a fully-pushed-down
     * query containing an aggregate SUM with a <code>CASE</code> expression
     * in which a <code>BETWEEN</code> comparison is used in the queries
     * <code>SELECT</code> statement.
     * <p>
     * For example:
     * <p>
     * SELECT SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END) FROM pm1.g1
     */
    @Test public void testBetweenInCaseInSum() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpPlan("select sum(case when e2 between 3 and 5 then e2 else -1 end) from pm1.g1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(), null, capFinder,
                new String[] { "SELECT SUM(CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END) FROM pm1.g1"},  //$NON-NLS-1$
                TestOptimizer.SHOULD_SUCCEED);
    }

    /**
     * Test <code>QueryOptimizer</code>'s ability to plan a fully-pushed-down
     * query containing an aggregate SUM with a <code>CASE</code> expression
     * in which a <code>BETWEEN</code> comparison is used in the queries
     * <code>SELECT</code> statement and a GROUP BY is specified.
     * <p>
     * For example:
     * <p>
     * SELECT e1, SUM(CASE WHEN e2 BETWEEN 3 AND 5 THEN e2 ELSE -1 END)
     * FROM pm1.g1 GROUP BY e1
     */
    @Test public void testBetweenInCaseInSumWithGroupBy() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpPlan("select sum(case when e2 between 3 and 5 then e2 else -1 end) from pm1.g1 group by e1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(), null, capFinder,
                new String[] { "SELECT SUM(CASE WHEN (e2 >= 3) AND (e2 <= 5) THEN e2 ELSE -1 END) FROM pm1.g1 GROUP BY e1"},  //$NON-NLS-1$
                TestOptimizer.SHOULD_SUCCEED);
    }

    /**
     * Test the query optimizer's ability to properly plan and optimize a query
     * that uses ambiguous alias names in the top level query and its sub-query
     * and uses columns belonging to the alias as a parameter to a function.
     * <p>
     * For example, <code>SELECT CONVERT(A.e2, biginteger) AS e2 FROM (SELECT
     * CONVERT(e2, long) AS e2 FROM pm1.g1 AS A) AS A</code>
     * <p>
     * The test is to ensure that A.e2 from the top level is not confused with
     * e2 in the second level.
     * <p>
     * Related Defects: JBEDSP-1137
     */
    @Test public void testAmbiguousAliasFunctionInSubQuerySource() throws Exception {
        // Create query
        String sql = "SELECT CONVERT(A.e2, biginteger) AS e2 FROM (" + //$NON-NLS-1$
        "   SELECT CONVERT(e2, long) AS e2 FROM pm1.g1 AS A" + //$NON-NLS-1$
        ") AS A"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        helpPlan(sql, metadata, new String[] {"SELECT e2 FROM pm1.g1 AS A"}); //$NON-NLS-1$

        // Add convert capability to pm1 and try it again
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpPlan(sql, metadata, null, capFinder,
            new String[] {"SELECT CONVERT(CONVERT(g_0.e2, long), biginteger) FROM pm1.g1 AS g_0"}, //$NON-NLS-1$
            ComparisonMode.EXACT_COMMAND_STRING );
    }

    @Test public void testNestedTable() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        ProcessorPlan plan = helpPlan("select pm2.g1.e1, x.e1 from pm2.g1, table(select * from pm2.g2 where pm2.g1.e1=pm2.g2.e1) x where pm2.g1.e2 IN (1, 2)", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm2.g2 AS g_0 WHERE g_0.e1 = pm2.g1.e1", "SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e2 IN (1, 2)" }, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {1}, new Class[] {NestedTableJoinStrategy.class});
    }

    @Test public void testUpdatePushdownFails() {
        helpPlan("update pm1.g1 set e1 = 1 where exists (select 1 from pm1.g2 where e1 = pm1.g1.e1)", RealMetadataFactory.example1Cached(), null, //$NON-NLS-1$
            null, null, false); //$NON-NLS-1$
    }

    @Test public void testUnnamedAggInView() throws Exception {
        MetadataStore metadataStore = new MetadataStore();

        Schema bqt1 = RealMetadataFactory.createPhysicalModel("BQT1", metadataStore); //$NON-NLS-1$
        Schema vqt = RealMetadataFactory.createVirtualModel("VQT", metadataStore); //$NON-NLS-1$

        Table bqt1SmallA = RealMetadataFactory.createPhysicalGroup("SmallA", bqt1); //$NON-NLS-1$
        RealMetadataFactory.createElement("col", bqt1SmallA, DataTypeManager.DefaultDataTypes.STRING);

        Table agg3 = RealMetadataFactory.createVirtualGroup("Agg3", vqt, new QueryNode("select count(*) from smalla"));
        RealMetadataFactory.createElement("count", agg3, DataTypeManager.DefaultDataTypes.INTEGER);

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "x");
        BasicSourceCapabilities bac = getTypicalCapabilities();
        bac.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bac.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        bac.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        helpPlan("select count(*) from agg3", metadata, new String[] {}, new DefaultCapabilitiesFinder(bac), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testMergeGroupBy1() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT a, b FROM (select 1 as a, 2 as b from pm1.g1) as x group by a, b", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT pm1.g1.e1 FROM pm1.g1 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
    }

    @Test public void testNonJoinComparison() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_ONLY_LITERAL_COMPARE, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT intkey from bqt1.smalla where intkey = intnum", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT BQT1.SmallA.IntKey, BQT1.SmallA.IntNum FROM BQT1.SmallA"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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

    @Test public void testConvertSignature() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        caps.setTranslator(new ExecutionFactory<Object, Object>() {
            @Override
            public boolean supportsConvert(int fromType, int toType) {
                return (fromType == DefaultTypeCodes.INTEGER && toType == DefaultTypeCodes.STRING);
            }
           });
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e1 from pm1.g1 where e1 = e2 and e1 = e3", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT pm1.g1.e1, pm1.g1.e3 FROM pm1.g1 WHERE pm1.g1.e1 = convert(pm1.g1.e2, string)"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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

    @Test public void testParseFormat() throws Exception {
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ONLY_FORMAT_LITERALS, true);
        caps.setFunctionSupport(SourceSystemFunctions.FORMATTIMESTAMP, true);
        caps.setFunctionSupport(SourceSystemFunctions.PARSEBIGDECIMAL, true);
        caps.setTranslator(new ExecutionFactory<Object, Object> () {
            @Override
            public boolean supportsFormatLiteral(String literal,
                    org.teiid.translator.ExecutionFactory.Format format) {
                return (format == Format.DATE && literal.equals("yyyy")) || (format == Format.NUMBER && literal.equals("$"));
            }
        });
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT stringkey from bqt1.smalla where formattimestamp(timestampvalue, 'yyyy') = '1921' and parsebigdecimal(stringkey, '$') = 1 and formattimestamp(timestampvalue, 'yy') = '19'", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.TimestampValue, g_0.StringKey FROM BQT1.SmallA AS g_0 WHERE (formattimestamp(g_0.TimestampValue, 'yyyy') = '1921') AND (parsebigdecimal(g_0.StringKey, '$') = 1)"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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

        caps.setFunctionSupport(SourceSystemFunctions.PARSETIMESTAMP, true);

        //test case sensitity
        plan = TestOptimizer.helpPlan("SELECT ParseTimeStamp(stringkey, 'yyyy') from bqt1.smalla where FormatTimestamp(timestampvalue, 'yyyy') = '1921'", //$NON-NLS-1$
                RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(caps),
                new String[] {
                    "SELECT ParseTimeStamp(g_0.StringKey, 'yyyy') FROM BQT1.SmallA AS g_0 WHERE FormatTimestamp(g_0.TimestampValue, 'yyyy') = '1921'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);

    }

    @Test public void testDistinctConstant() throws Exception {
        String sql = "select distinct 1 from pm1.g1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, getGenericFinder(),
                                      new String[] {"SELECT DISTINCT 1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    //TODO: there is an unnecessary dup removal here since it is the root and the optimization cannot modify the plan root
    @Test public void testDistinctConstant1() throws Exception {
        String sql = "select distinct 1 from pm1.g1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(),
                                      new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                1,      // DupRemove
                0,      // Grouping
                1,        // Limit
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            }, TestLimit.NODE_TYPES);
    }

    @Test public void testDistinctConstant2() throws Exception {
        String sql = "select distinct 1 from pm1.g1"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT 1 AS c_0 FROM pm1.g1 AS g_0 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testDistinctConstant3() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        TestOptimizer.helpPlan("SELECT DISTINCT c1, null as c2, null as c3 FROM(SELECT c1, c2 FROM ("
                + "SELECT 'const_col_1' as c1, e1 as c2 FROM pm1.g1 UNION ALL "
                + "SELECT 'const_col_2' as c1, e1 as c2 FROM pm2.g2 ) as v ) as v1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT 'const_col_2' AS c_0 FROM pm2.g2 AS g_0 LIMIT 1", "SELECT 'const_col_1' AS c_0 FROM pm1.g1 AS g_0 LIMIT 1"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testDistinctConstant4() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        TestOptimizer.helpPlan("SELECT DISTINCT c1, null as c2, null as c3 FROM(SELECT c1, c2 FROM ("
                + "SELECT 'const_col_1' as c1, e1 as c2 FROM pm1.g1 UNION ALL "
                + "SELECT 'const_col_2' as c1, e1 as c2 FROM pm2.g2 ) as v ) as v1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT g_0.e1 FROM pm2.g2 AS g_0 LIMIT 1", "SELECT g_0.e1 FROM pm1.g1 AS g_0 LIMIT 1"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testPlanNodeAnnotation() throws Exception {
        PlanNode pn = new PlanNode();
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("pm1");
        AnalysisRecord record = new AnalysisRecord(true, true);
        pn.recordDebugAnnotation("hello", modelID, "world", record, metadata);
        assertEquals("[LOW [Relational Planner] hello pm1 - world Unknown: 0(groups=[]]", record.getAnnotations().toString());
    }

    @Test public void testRecursiveView() throws Exception {
        String ddl = "CREATE view x (y string) as (select * from x)";

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");

        getPlan(helpGetCommand("select * from x", metadata), metadata, getGenericFinder(), null, false, null);
    }

    @Test(expected=QueryPlannerException.class) public void testInvalidSource() throws Exception {
        String sql = "select * from pm1.g1"; //$NON-NLS-1$
        QueryMetadataInterface md = RealMetadataFactory.example1Cached();
        QueryOptimizer.optimizePlan(helpGetCommand(sql, md), md, null,  new DefaultCapabilitiesFinder() {
            @Override
            public boolean isValid(String modelName) {
                return false;
            }
        }, null, new CommandContext());
    }

    @Test public void testUnaliased() throws Exception {
        String sql = "SELECT x.count + 1 FROM agg x"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table smalla (intkey integer); create view agg (count integer) as select intkey from smalla order by intkey limit 1", "x", "y");
        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setFunctionSupport("+", true);
        TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT (v_0.c_0 + 1) FROM (SELECT g_0.intkey AS c_0 FROM y.smalla AS g_0 ORDER BY c_0 LIMIT 1) AS v_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testLikeEscapeRestriction() throws Exception {
        String sql = "SELECT e2 FROM pm1.g1 where e1 like 'a%b' escape '!'"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setSourceProperty(Capability.REQUIRED_LIKE_ESCAPE, '\\');
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e1 LIKE 'a%b'"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);

        sql = "SELECT e2 FROM pm1.g1 where e1 like '!_a%b' escape '!'"; //$NON-NLS-1$

        //TODO - when possible this should modify the match pattern to use the required escape
        bsc.setSourceProperty(Capability.REQUIRED_LIKE_ESCAPE, '\\');
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);

        sql = "SELECT e2 FROM pm1.g1 where e1 like '\\_a%b' escape '\\'"; //$NON-NLS-1$

        bsc.setSourceProperty(Capability.REQUIRED_LIKE_ESCAPE, '\\');
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e1 LIKE '\\_a%b' ESCAPE '\\'"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testTimestampAddRestriction() throws Exception {
        String sql = "SELECT timestampadd(sql_tsi_second, 1, timestampvalue) from bqt1.smalla"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.TIMESTAMPADD, true);
        bsc.setCapabilitySupport(Capability.ONLY_TIMESTAMPADD_LITERAL, true);
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT timestampadd(sql_tsi_second, 1, g_0.TimestampValue) FROM BQT1.SmallA AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);

        sql = "SELECT timestampadd(sql_tsi_second, intkey, timestampvalue) from bqt1.smalla"; //$NON-NLS-1$

        //TODO - when possible this should modify the match pattern to use the required escape
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.IntKey, g_0.TimestampValue FROM BQT1.SmallA AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);

        bsc.setCapabilitySupport(Capability.ONLY_TIMESTAMPADD_LITERAL, false);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT timestampadd(sql_tsi_second, g_0.IntKey, g_0.TimestampValue) FROM BQT1.SmallA AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testUnionAliasingWithNullNestedProjections() throws Exception {
        String sql = "select x, y FROM (select x, y, null as z from inner_view union select intkey, stringkey, null from smallb) v LIMIT 99999";

        String expected = "SELECT v_0.c_0, v_0.c_1 FROM (SELECT null AS c_0, null AS c_1, null AS c_2 FROM x.smalla AS g_1 UNION SELECT convert(g_0.intkey, string) AS c_0, g_0.stringkey AS c_1, null AS c_2 FROM x.smallb AS g_0) AS v_0 LIMIT 99999"; //$NON-NLS-1$

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_UNION, true);
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bsc.setFunctionSupport("convert", true);
        bsc.setFunctionSupport("concat", true);

        TestOptimizer.helpPlan(sql,
        RealMetadataFactory.fromDDL(
          "create foreign table smalla (intkey integer, stringkey string); "
          + "create foreign table smallb (intkey integer, stringkey string); "
          + "create view inner_view as (select null as x, null as y from smalla);", "x", "x"),
        new String[] {expected},
        new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    public static final boolean DEBUG = false;

}
