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

package org.teiid.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil.DependentCostAnalysis;
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.DependentSetCriteria.AttributeComparison;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Option.MakeDep;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Finds nodes that can be turned into dependent joins
 */
public final class RuleChooseDependent implements OptimizerRule {

    private static AtomicInteger ID = new AtomicInteger();

    private static class CandidateJoin {
        PlanNode joinNode;
        boolean leftCandidate;
        boolean rightCandidate;
    }

    public static final int DEFAULT_INDEPENDENT_CARDINALITY = PropertiesUtils.getHierarchicalProperty("org.teiid.defaultIndependentCardinality", 10, Integer.class); //$NON-NLS-1$
    public static final int UNKNOWN_INDEPENDENT_CARDINALITY = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;

    private boolean fullPushOnly;
    private boolean traditionalOnly;

    public RuleChooseDependent() {
    }

    public RuleChooseDependent(boolean b) {
        this.fullPushOnly = b;
    }

    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        // Find first criteria node in plan with conjuncts
        List<CandidateJoin> matches = null;

        if (fullPushOnly) {
            //full push requires bottom up processing,
            matches = findCandidate(plan, metadata, analysisRecord);
            Collections.reverse(matches);
        } else {
            if (!traditionalOnly) {
                plan = new RuleChooseDependent(true).execute(plan, metadata, capFinder, rules, analysisRecord, context);
            }
            matches = findCandidate(plan, metadata, analysisRecord);
        }

        boolean pushCriteria = false;

        // Handle all cases where both siblings are possible matches
        for (CandidateJoin entry : matches) {
            PlanNode joinNode = entry.joinNode;

            if (fullPushOnly && NodeEditor.findParent(joinNode, NodeConstants.Types.ACCESS) != null) {
                continue; //already consumed by full pushdown
            }

            PlanNode sourceNode = entry.leftCandidate?joinNode.getFirstChild():joinNode.getLastChild();

            PlanNode siblingNode = entry.leftCandidate?joinNode.getLastChild():joinNode.getFirstChild();

            boolean bothCandidates = entry.leftCandidate&&entry.rightCandidate;

            PlanNode chosenNode = chooseDepWithoutCosting(sourceNode, siblingNode, analysisRecord, bothCandidates);
            if(chosenNode != null) {
                pushCriteria |= markDependent(chosenNode, joinNode, metadata, null, false, capFinder, context, rules, analysisRecord);
                continue;
            }

            DependentCostAnalysis dca = NewCalculateCostUtil.computeCostForDepJoin(joinNode, !entry.leftCandidate, metadata, capFinder, context);
            PlanNode dependentNode = sourceNode;

            if (bothCandidates && dca.expectedCardinality == null) {
                dca = NewCalculateCostUtil.computeCostForDepJoin(joinNode, true, metadata, capFinder, context);
                if (dca.expectedCardinality != null) {
                    dependentNode = siblingNode;
                }
            }

            if (dca.expectedCardinality != null) {
                pushCriteria |= markDependent(dependentNode, joinNode, metadata, dca, null, capFinder, context, rules, analysisRecord);
            } else {
                float sourceCost = NewCalculateCostUtil.computeCostForTree(sourceNode, metadata);
                float siblingCost = NewCalculateCostUtil.computeCostForTree(siblingNode, metadata);

                List leftExpressions = (List)joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS);
                List rightExpressions = (List)joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS);

                float sourceNdv = NewCalculateCostUtil.getNDVEstimate(sourceNode, metadata, sourceCost, entry.leftCandidate?leftExpressions:rightExpressions, true);
                float siblingNdv = NewCalculateCostUtil.getNDVEstimate(siblingNode, metadata, siblingCost, entry.leftCandidate?rightExpressions:leftExpressions, true);

                if (sourceCost != NewCalculateCostUtil.UNKNOWN_VALUE && sourceNdv == NewCalculateCostUtil.UNKNOWN_VALUE) {
                    sourceNdv = sourceCost;
                }
                if (siblingCost != NewCalculateCostUtil.UNKNOWN_VALUE && siblingNdv == NewCalculateCostUtil.UNKNOWN_VALUE) {
                    siblingNdv = siblingCost;
                }

                if (bothCandidates && sourceNdv != NewCalculateCostUtil.UNKNOWN_VALUE && ((sourceNdv <= RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY
                        && sourceNdv < siblingNdv && sourceCost < 4*siblingCost) || (siblingCost == NewCalculateCostUtil.UNKNOWN_VALUE && sourceNdv <= UNKNOWN_INDEPENDENT_CARDINALITY))) {
                    pushCriteria |= markDependent(siblingNode, joinNode, metadata, null, sourceCost > RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY?true:null, capFinder, context, rules, analysisRecord);
                } else if (siblingNdv != NewCalculateCostUtil.UNKNOWN_VALUE && ((siblingNdv <= RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY && siblingNdv < sourceNdv && siblingCost < 4*sourceCost) || (sourceCost == NewCalculateCostUtil.UNKNOWN_VALUE && siblingNdv <= UNKNOWN_INDEPENDENT_CARDINALITY))) {
                    pushCriteria |= markDependent(sourceNode, joinNode, metadata, null, siblingCost > RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY?true:null, capFinder, context, rules, analysisRecord);
                }
            }
        }

        if (pushCriteria) {
            // Insert new rules to push down the SELECT criteria
            rules.push(RuleConstants.CLEAN_CRITERIA); //it's important to run clean criteria here since it will remove unnecessary dependent sets
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }

        if (!matches.isEmpty() && plan.getParent() != null) {
            //this can happen if we create a fully pushable plan from full dependent join pushdown
            return plan.getParent();
        }

        return plan;
    }

    /**
     * Walk the tree pre-order, finding all access nodes that are candidates and
     * adding them to the matches list.
     * @param metadata Metadata implementation
     * @param root Root node to search
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    List<CandidateJoin> findCandidate(PlanNode root, QueryMetadataInterface metadata, AnalysisRecord analysisRecord) throws QueryMetadataException, TeiidComponentException {

        List<CandidateJoin> candidates = new ArrayList<CandidateJoin>();

        for (PlanNode joinNode : NodeEditor.findAllNodes(root, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS)) {
            CandidateJoin candidate = null;

            for (Iterator<PlanNode> j = joinNode.getChildren().iterator(); j.hasNext();) {
                PlanNode child = j.next();
                child = FrameUtil.findJoinSourceNode(child);

                if(child.hasBooleanProperty(NodeConstants.Info.MAKE_NOT_DEP) || !isValidJoin(joinNode, child, analysisRecord)) {
                    continue;
                }
                if (candidate == null) {
                    candidate = new CandidateJoin();
                    candidate.joinNode = joinNode;
                }
                if (j.hasNext()) {
                    JoinType jtype = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
                    if (!jtype.isOuter()) {
                        candidate.leftCandidate=true;
                        candidates.add(candidate);
                    }
                } else {
                    candidate.rightCandidate=true;
                    if (!candidate.leftCandidate) {
                        candidates.add(candidate);
                    }
                }
            }
        }

        return candidates;

    }

    /**
     * Check whether a join is valid.  Invalid joins are CROSS JOIN, FULL OUTER JOIN,
     * any join without criteria, any join with no equality criteria, and any outer
     * join that has the outer side not the same as the dependent.
     * @param joinNode The join node to check
     * @param sourceNode The access node being considered
     * @param analysisRecord
     * @return True if valid for making dependent
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    boolean isValidJoin(PlanNode joinNode, PlanNode sourceNode, AnalysisRecord analysisRecord) throws QueryMetadataException, TeiidComponentException {
        JoinType jtype = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);

        // Check that join is not a CROSS join or FULL OUTER join
        if(jtype.equals(JoinType.JOIN_CROSS)) {
            sourceNode.recordDebugAnnotation("parent join is CROSS", null, "Rejecting dependent join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            return false;
        }

        if (!joinNode.getExportedCorrelatedReferences().isEmpty()) {
            sourceNode.recordDebugAnnotation("parent join has a correlated nested table", null, "Rejecting dependent join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            return false;
        }

        // Check that join criteria exist
        List jcrit = (List) joinNode.getProperty(NodeConstants.Info.JOIN_CRITERIA);
        if(jcrit == null || jcrit.size() == 0) {
            sourceNode.recordDebugAnnotation("parent join has has no join criteria", null, "Rejecting dependent join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            return false;
        }

        if(joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS) == null) {
            sourceNode.recordDebugAnnotation("parent join has no equa-join predicates", null, "Rejecting dependent join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            return false;
        }

        return true;
    }

    PlanNode chooseDepWithoutCosting(PlanNode rootNode1, PlanNode rootNode2, AnalysisRecord analysisRecord, boolean bothCandidates) throws QueryMetadataException, TeiidComponentException  {
        PlanNode sourceNode1 = FrameUtil.findJoinSourceNode(rootNode1);
        if (sourceNode1.getType() == NodeConstants.Types.GROUP) {
            //after push aggregates it's possible that the source is a grouping node
            sourceNode1 = FrameUtil.findJoinSourceNode(sourceNode1.getFirstChild());
        }
        PlanNode sourceNode2 = FrameUtil.findJoinSourceNode(rootNode2);
        if (sourceNode2.getType() == NodeConstants.Types.GROUP) {
            //after push aggregates it's possible that the source is a grouping node
            sourceNode2 = FrameUtil.findJoinSourceNode(sourceNode2.getFirstChild());
        }

        if(sourceNode1.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS) ) {
            if (sourceNode2.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS) ) {
                //Return null - query planning should fail because both access nodes
                //have unsatisfied access patterns
                rootNode1.getParent().recordDebugAnnotation("both children have unsatisfied access patterns", null, "Neither node can be made dependent", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
                return null;
            }
            rootNode1.recordDebugAnnotation("unsatisfied access pattern detected", null, "marking as dependent side of join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            return rootNode1;
        }
        if (bothCandidates && sourceNode2.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS) ) {
            //Access node 2 has unsatisfied access pattern,
            //so try to make node 2 dependent
            sourceNode2.recordDebugAnnotation("unsatisfied access pattern detected", null, "marking as dependent side of join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            return rootNode2;
        }

        // Check for hints, which over-rule heuristics
        if(sourceNode1.hasProperty(NodeConstants.Info.MAKE_DEP)) {
            sourceNode1.recordDebugAnnotation("MAKE_DEP hint detected", null, "marking as dependent side of join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            rootNode1.setProperty(Info.MAKE_DEP, sourceNode1.getProperty(Info.MAKE_DEP));
            return rootNode1;
        }
        if(bothCandidates && sourceNode2.hasProperty(NodeConstants.Info.MAKE_DEP)) {
            sourceNode2.recordDebugAnnotation("MAKE_DEP hint detected", null, "marking as dependent side of join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            rootNode2.setProperty(Info.MAKE_DEP, sourceNode2.getProperty(Info.MAKE_DEP));
            return rootNode2;
        }
        if (bothCandidates && sourceNode1.hasProperty(NodeConstants.Info.MAKE_IND)) {
            sourceNode2.recordDebugAnnotation("MAKE_IND hint detected", null, "marking as dependent side of join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            rootNode2.setProperty(Info.MAKE_DEP, sourceNode1.getProperty(Info.MAKE_IND));
            return rootNode2;
        }
        if (sourceNode2.hasProperty(NodeConstants.Info.MAKE_IND)) {
            sourceNode1.recordDebugAnnotation("MAKE_IND hint detected", null, "marking as dependent side of join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            rootNode1.setProperty(Info.MAKE_DEP, sourceNode2.getProperty(Info.MAKE_IND));
            return rootNode1;
        }

        return null;
    }

    /**
     * Mark the specified access node to be made dependent
     * @param sourceNode Node to make dependent
     * @param dca
     * @param rules
     * @param analysisRecord
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws QueryPlannerException
     */
    boolean markDependent(PlanNode sourceNode, PlanNode joinNode, QueryMetadataInterface metadata, DependentCostAnalysis dca,
            Boolean bound, CapabilitiesFinder capabilitiesFinder, CommandContext context, RuleStack rules, AnalysisRecord analysisRecord) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {

        boolean isLeft = joinNode.getFirstChild() == sourceNode;

        // Get new access join node properties based on join criteria
        List independentExpressions = (List)(isLeft?joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS));
        List dependentExpressions = (List)(isLeft?joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS));

        if(independentExpressions == null || independentExpressions.isEmpty()) {
            return false;
        }

        PlanNode indNode = isLeft?joinNode.getLastChild():joinNode.getFirstChild();

        if (bound == null) {
            List<PlanNode> sources = NodeEditor.findAllNodes(indNode, NodeConstants.Types.SOURCE);
            for (PlanNode planNode : sources) {
                for (GroupSymbol gs : planNode.getGroups()) {
                    if (gs.isTempTable() && metadata.getCardinality(gs.getMetadataID()) == QueryMetadataInterface.UNKNOWN_CARDINALITY) {
                        bound = true;
                        break;
                    }
                }
            }
            if (bound == null) {
                bound = false;
            }
        }
        MakeDep makeDep = (MakeDep)sourceNode.getProperty(Info.MAKE_DEP);
        if (fullPushOnly) {
            boolean result = fullyPush(sourceNode, joinNode, metadata, capabilitiesFinder, context, indNode, rules, makeDep, analysisRecord, independentExpressions);
            //before partial aggreage pushdown carry over a hint
            if (!result && !indNode.hasBooleanProperty(Info.MAKE_IND) && dca != null
                    && NodeEditor.findParent(joinNode, NodeConstants.Types.GROUP) != null) {
                //hint to be picked up on the next run
                MakeDep indHint = new MakeDep();
                //TODO: may need to consider all ndvs
                if (dca.maxNdv[0] != null) {
                    indHint.setMax((int)Math.min(dca.maxNdv[0], Integer.MAX_VALUE));
                }
                indNode.setProperty(Info.MAKE_IND, indHint);
            }
            return false;
        }

        // Check that for a outer join the dependent side must be the inner
        JoinType jtype = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
        if(jtype == JoinType.JOIN_FULL_OUTER || (jtype.isOuter() && JoinUtil.getInnerSideJoinNodes(joinNode)[0] != sourceNode)) {
            sourceNode.recordDebugAnnotation("node is on outer side of the join", null, "Rejecting dependent join", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            return false;
        }

        String id = nextId();
        // Create DependentValueSource and set on the independent side as this will feed the values
        joinNode.setProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE, id);

        PlanNode depNode = isLeft?joinNode.getFirstChild():joinNode.getLastChild();
        depNode = FrameUtil.findJoinSourceNode(depNode);
        if (!depNode.hasCollectionProperty(Info.ACCESS_PATTERNS)) {
            //in some situations a federated join will span multiple tables using the same key
            handleDuplicate(joinNode, isLeft, independentExpressions, dependentExpressions);
            handleDuplicate(joinNode, !isLeft, dependentExpressions, independentExpressions);
        }

        PlanNode crit = getDependentCriteriaNode(id, independentExpressions, dependentExpressions, indNode, metadata, dca, bound, makeDep);

        sourceNode.addAsParent(crit);

        if (isLeft) {
            JoinUtil.swapJoinChildren(joinNode);
        }

        return true;
    }

    private void handleDuplicate(PlanNode joinNode, boolean isLeft,
            List independentExpressions, List dependentExpressions) {
        Map<Expression, Integer> seen = new HashMap<Expression, Integer>();
        for (int i = 0; i < dependentExpressions.size(); i++) {
            Expression ex = (Expression) dependentExpressions.get(i);
            Integer index = seen.get(ex);
            if (index == null) {
                seen.put(ex, i);
            } else {
                Expression e1 = (Expression)independentExpressions.get(i);
                Expression e2 = (Expression)independentExpressions.get(index);
                CompareCriteria cc = new CompareCriteria(e1, CompareCriteria.EQ, e2);
                PlanNode impliedCriteria = RelationalPlanner.createSelectNode(cc, false);
                if (isLeft) {
                    joinNode.getLastChild().addAsParent(impliedCriteria);
                } else {
                    joinNode.getFirstChild().addAsParent(impliedCriteria);
                }
                independentExpressions.remove(i);
                dependentExpressions.remove(i);
                i--;
            }
        }
    }

    public static String nextId() {
        return "$dsc/id" + ID.getAndIncrement(); //$NON-NLS-1$
    }

    /**
     * Check for fully pushable dependent joins
     * currently we only look for the simplistic scenario where there are no intervening
     * nodes above the dependent side
     * @param independentExpressions
     */
    private boolean fullyPush(PlanNode sourceNode, PlanNode joinNode,
            QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, CommandContext context,
            PlanNode indNode,
            RuleStack rules, MakeDep makeDep, AnalysisRecord analysisRecord, List independentExpressions) throws QueryMetadataException,
            TeiidComponentException, QueryPlannerException {
        if (sourceNode.getType() != NodeConstants.Types.ACCESS) {
            return false; //don't remove as we may raise an access node to make this possible
        }
        Object modelID = RuleRaiseAccess.getModelIDFromAccess(sourceNode, metadata);

        boolean hasHint = false;

        if (makeDep != null && makeDep.getJoin() != null) {
            if (!makeDep.getJoin()) {
                sourceNode.recordDebugAnnotation("cannot pushdown dependent join", modelID, "honoring hint", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
                return false;
            }
            hasHint = true;
        }

        if (!CapabilitiesUtil.supports(Capability.FULL_DEPENDENT_JOIN, modelID, metadata, capabilitiesFinder)) {
            if (hasHint) {
                sourceNode.recordDebugAnnotation("cannot pushdown dependent join", modelID, "dependent join pushdown needs enabled at the source", analysisRecord, null); //$NON-NLS-1$ //$NON-NLS-2$
            }
            return false;
        }

        List<? extends Expression> projected = (List<? extends Expression>) indNode.getProperty(Info.OUTPUT_COLS);
        if (projected == null) {
            PlanNode plan = sourceNode;
            while (plan.getParent() != null) {
                plan = plan.getParent();
            }
            new RuleAssignOutputElements(false).execute(plan, metadata, capabilitiesFinder, null, AnalysisRecord.createNonRecordingRecord(), context);
            projected = (List<? extends Expression>) indNode.getProperty(Info.OUTPUT_COLS);
        }

        if (!hasHint) {
            //require no lobs
            for (Expression ex : projected) {
                if (DataTypeManager.isLOB(ex.getClass())) {
                    return false;
                }
            }

            //old optimizer tests had no buffermanager
            if (context.getBufferManager() == null) {
                return false;
            }

            if (makeDep != null && makeDep.getMax() != null) {
                //if the user specifies a max, it's best to just use a regular dependent join
                return false;
            }
        }

        /*
         * check to see how far the access node can be raised
         */
        PlanNode tempAccess = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        GroupSymbol gs = RulePlaceAccess.recontextSymbol(new GroupSymbol("TEIID_TEMP"), context.getGroups()); //$NON-NLS-1$
        gs.setDefinition(null);
        tempAccess.addGroup(gs);
        tempAccess.setProperty(Info.MODEL_ID, modelID);
        indNode.addAsParent(tempAccess);

        PlanNode originalSource = sourceNode;
        sourceNode = originalSource.clone();
        //more deeply clone
        if (sourceNode.hasCollectionProperty(Info.ACCESS_PATTERNS)) {
            sourceNode.setProperty(Info.ACCESS_PATTERNS, new ArrayList<AccessPattern>((List)sourceNode.getProperty(Info.ACCESS_PATTERNS)));
        }
        if (sourceNode.hasCollectionProperty(Info.CONFORMED_SOURCES)) {
            sourceNode.setProperty(Info.CONFORMED_SOURCES, new LinkedHashSet<Object>((Set)sourceNode.getProperty(Info.CONFORMED_SOURCES)));
        }
        originalSource.addAsParent(sourceNode);
        boolean raised = false;
        boolean moreProcessing = false;
        boolean first = true;
        while (sourceNode.getParent() != null && RuleRaiseAccess.raiseAccessNode(sourceNode, sourceNode, metadata, capabilitiesFinder, true, null, context) != null) {
            raised = true;
            if (first) { //raising over join required
                first = false;
                continue;
            }
            switch (sourceNode.getFirstChild().getType()) {
            case NodeConstants.Types.PROJECT:
                //TODO: check for correlated subqueries
                if (sourceNode.getFirstChild().hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)) {
                    moreProcessing = true;
                }
                break;
            case NodeConstants.Types.SORT:
            case NodeConstants.Types.DUP_REMOVE:
            case NodeConstants.Types.GROUP:
            case NodeConstants.Types.SELECT:
            case NodeConstants.Types.TUPLE_LIMIT:
            case NodeConstants.Types.JOIN:
                moreProcessing = true;
                break;
            }
        }
        if (!raised) {
            tempAccess.getParent().replaceChild(tempAccess, tempAccess.getFirstChild());
            sourceNode.getParent().replaceChild(sourceNode, sourceNode.getFirstChild());
            return false;
        }
        if (!moreProcessing && !hasHint) {
            //restore the plan
            if (sourceNode.getParent() != null) {
                sourceNode.getParent().replaceChild(sourceNode, sourceNode.getFirstChild());
            } else {
                sourceNode.removeAllChildren();
            }
            return false;
        }
        originalSource.getParent().replaceChild(originalSource, originalSource.getFirstChild());
        //all the references to any groups from this join have to changed over to the new group
        //and we need to insert a source/project node to turn this into a proper plan
        PlanNode project = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        PlanNode source = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        source.addGroup(gs);

        project.setProperty(Info.OUTPUT_COLS, projected);
        project.setProperty(Info.PROJECT_COLS, projected);

        Set<GroupSymbol> newGroups = Collections.singleton(gs);
        ArrayList<ElementSymbol> virtualSymbols = new ArrayList<ElementSymbol>(projected.size());
        for (int i = 0; i < projected.size(); i++) {
            ElementSymbol es = new ElementSymbol("col" + (i+1)); //$NON-NLS-1$
            Expression ex = projected.get(i);
            es.setType(ex.getType());
            virtualSymbols.add(es);
            //TODO: set a metadata id from either side
            if (ex instanceof ElementSymbol) {
                es.setMetadataID(((ElementSymbol)ex).getMetadataID());
            }
        }
        List<ElementSymbol> newCols = RulePushAggregates.defineNewGroup(gs, virtualSymbols, metadata);
        SymbolMap symbolMap = SymbolMap.createSymbolMap(newCols, projected);
        Map<Expression, ElementSymbol> inverse = symbolMap.inserseMapping();

        //TODO: the util logic should handle multiple groups
        for (GroupSymbol group : indNode.getGroups()) {
            FrameUtil.convertFrame(joinNode, group, newGroups, inverse, metadata);
        }

        //add the source a new group for the join
        indNode.addAsParent(source);

        //convert the lower plan into a subplan
        //it needs to be rooted by a project - a view isn't really needed
        indNode.removeFromParent();
        project.addFirstChild(indNode);
        //run the remaining rules against the subplan
        RuleStack ruleCopy = rules.clone();
        RuleChooseDependent ruleChooseDependent = new RuleChooseDependent();
        ruleChooseDependent.traditionalOnly = true;
        ruleCopy.push(ruleChooseDependent);

        if (indNode.getType() == NodeConstants.Types.ACCESS) {
            PlanNode root = RuleRaiseAccess.raiseAccessNode(project, indNode, metadata, capabilitiesFinder, true, null, context);
            if (root != project) {
                project = root;
            }
        }
        //fully plan the sub-plan with the remaining rules
        project = rules.getPlanner().executeRules(ruleCopy, project);
        source.setProperty(Info.SYMBOL_MAP, symbolMap);
        source.setProperty(Info.SUB_PLAN, project);
        return true;
    }

    /**
     * @param independentExpressions
     * @param dependentExpressions
     * @param makeDep
     * @return
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @since 4.3
     */
    public static PlanNode getDependentCriteriaNode(String id, List<Expression> independentExpressions,
                                           List<Expression> dependentExpressions, PlanNode indNode, QueryMetadataInterface metadata, DependentCostAnalysis dca, Boolean bound, MakeDep makeDep) throws QueryMetadataException, TeiidComponentException {

        Float cardinality = null;

        List<DependentSetCriteria.AttributeComparison> expressions = new ArrayList<DependentSetCriteria.AttributeComparison>(dependentExpressions.size());

        for (int i = 0; i < dependentExpressions.size(); i++) {
            Expression depExpr = dependentExpressions.get(i);
            Expression indExpr = independentExpressions.get(i);

            DependentSetCriteria.AttributeComparison comp = new DependentSetCriteria.AttributeComparison();
            if (dca != null && dca.expectedNdv[i] != null) {
                if (dca.expectedNdv[i] > 4*dca.maxNdv[i]) {
                    continue; //not necessary to use
                }
                comp.ndv = dca.expectedNdv[i];
                comp.maxNdv = dca.maxNdv[i];
            } else {
                Collection<ElementSymbol> elems = ElementCollectorVisitor.getElements(indExpr, true);
                if (cardinality == null) {
                    cardinality = NewCalculateCostUtil.computeCostForTree(indNode, metadata);
                }
                comp.ndv = NewCalculateCostUtil.getNDVEstimate(indNode, metadata, cardinality, elems, true);
                if (bound) {
                    if (dca != null) {
                        comp.maxNdv = Math.max(comp.ndv * 4, dca.expectedCardinality * 2);
                    } else {
                        comp.maxNdv = Math.max(UNKNOWN_INDEPENDENT_CARDINALITY, comp.ndv * 4);
                    }
                }
            }
            comp.ind = indExpr;
            comp.dep = SymbolMap.getExpression(depExpr);
            expressions.add(comp);
        }

        PlanNode result = createDependentSetNode(id, expressions);
        if (makeDep != null) {
            DependentSetCriteria dsc = (DependentSetCriteria)result.getProperty(Info.SELECT_CRITERIA);
            dsc.setMakeDepOptions(makeDep);
        }
        return result;
    }

    static PlanNode createDependentSetNode(String id, List<DependentSetCriteria.AttributeComparison> expressions) {
        DependentSetCriteria crit = createDependentSetCriteria(id, expressions);
        if (crit == null) {
            return null;
        }
        return RelationalPlanner.createSelectNode(crit, false);
    }

    static DependentSetCriteria createDependentSetCriteria(String id, List<DependentSetCriteria.AttributeComparison> expressions) {
        if (expressions.isEmpty()) {
            return null;
        }

        Expression indEx = null;
        Expression depEx = null;
        float maxNdv = NewCalculateCostUtil.UNKNOWN_VALUE;
        float ndv = NewCalculateCostUtil.UNKNOWN_VALUE;
        if (expressions.size() == 1) {
            AttributeComparison attributeComparison = expressions.get(0);
            indEx = attributeComparison.ind;
            depEx = attributeComparison.dep;
            maxNdv = attributeComparison.maxNdv;
            ndv = attributeComparison.ndv;
        } else {
            List<Expression> indExprs = new ArrayList<Expression>(expressions.size());
            List<Expression> depExprs = new ArrayList<Expression>(expressions.size());
            boolean unknown = false;
            for (DependentSetCriteria.AttributeComparison comp : expressions) {
                indExprs.add(comp.ind);
                depExprs.add(comp.dep);
                if (comp.ndv == NewCalculateCostUtil.UNKNOWN_VALUE) {
                    ndv = NewCalculateCostUtil.UNKNOWN_VALUE;
                    maxNdv = NewCalculateCostUtil.UNKNOWN_VALUE;
                    unknown = true;
                } else if (!unknown) {
                    ndv = Math.max(ndv, comp.ndv);
                    maxNdv = Math.max(maxNdv, comp.maxNdv);
                }
            }
            //TODO: detect a base type
            indEx = new Array(DefaultDataClasses.OBJECT, indExprs);
            depEx = new Array(DefaultDataClasses.OBJECT, depExprs);
        }

        DependentSetCriteria crit = new DependentSetCriteria(depEx, id);
        crit.setValueExpression(indEx);
        crit.setAttributes(expressions);
        crit.setMaxNdv(maxNdv);
        crit.setNdv(ndv);
        return crit;
    }

    public String toString() {
        return "ChooseDependent"; //$NON-NLS-1$
    }

}
