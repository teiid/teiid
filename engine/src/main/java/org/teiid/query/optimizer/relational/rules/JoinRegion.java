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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;


/**
 *  A join region is a set of cross and inner joins whose ordering is completely interchangeable.
 *
 *  It can be conceptually thought of as:
 *     Criteria node some combination of groups A, B, C
 *     Criteria node some combination of groups A, B, C
 *     ...
 *     Join
 *       JoinSourceA
 *       JoinSourceB
 *       JoinSourceC
 *
 *  A full binary join tree is then constructed out of this join region such that all of the
 *  criteria is pushed to its lowest point.
 *
 */
class JoinRegion {

    private JoinRegion left;

    private PlanNode joinRoot;

    public static final int UNKNOWN_TUPLE_EST = 100000;

    private LinkedHashMap<PlanNode, PlanNode> dependentJoinSourceNodes = new LinkedHashMap<PlanNode, PlanNode>();
    private LinkedHashMap<PlanNode, PlanNode> joinSourceNodes = new LinkedHashMap<PlanNode, PlanNode>();

    private List<PlanNode> dependentCritieraNodes = new ArrayList<PlanNode>();
    private List<PlanNode> criteriaNodes = new ArrayList<PlanNode>();

    private List<Collection<AccessPattern>> unsatisfiedAccessPatterns = new LinkedList<Collection<AccessPattern>>();
    private boolean containsNestedTable;

    private Map<ElementSymbol, Set<Collection<GroupSymbol>>> dependentCriteriaElements;
    private Map<PlanNode, Set<PlanNode>> critieriaToSourceMap;

    private HashMap<List<Object>, Float> depCache;

    public PlanNode getJoinRoot() {
        return joinRoot;
    }

    public void setContainsNestedTable(boolean containsNestedTable) {
        this.containsNestedTable = containsNestedTable;
    }

    public boolean containsNestedTable() {
        return containsNestedTable;
    }

    public List<Collection<AccessPattern>> getUnsatisfiedAccessPatterns() {
        return unsatisfiedAccessPatterns;
    }

    public Map<PlanNode, PlanNode> getJoinSourceNodes() {
        return joinSourceNodes;
    }

    public Map<PlanNode, PlanNode> getDependentJoinSourceNodes() {
        return dependentJoinSourceNodes;
    }

    public List<PlanNode> getCriteriaNodes() {
        return criteriaNodes;
    }

    public List<PlanNode> getDependentCriteriaNodes() {
        return dependentCritieraNodes;
    }

    public Map<ElementSymbol, Set<Collection<GroupSymbol>>> getDependentCriteriaElements() {
        return this.dependentCriteriaElements;
    }

    public Map<PlanNode, Set<PlanNode>> getCritieriaToSourceMap() {
        return this.critieriaToSourceMap;
    }

    public void addJoinSourceNode(PlanNode sourceNode) {
        PlanNode root = sourceNode;
        while (root.getParent() != null && root.getParent().getType() == NodeConstants.Types.SELECT) {
            root = root.getParent();
        }
        if (sourceNode.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
            Collection<AccessPattern> aps = (Collection<AccessPattern>)sourceNode.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
            unsatisfiedAccessPatterns.add(aps);
            dependentJoinSourceNodes.put(sourceNode, root);
        } else {
            joinSourceNodes.put(sourceNode, root);
        }

        if (joinRoot == null) {
            joinRoot = root;
        }
    }

    public void addParentCriteria(PlanNode sourceNode) {
        PlanNode parent = sourceNode.getParent();
        while (parent != null && parent.getType() == NodeConstants.Types.SELECT) {
            criteriaNodes.add(parent);
            sourceNode = parent;
            parent = parent.getParent();
        }
        if (joinRoot == null) {
            joinRoot = sourceNode;
        }
    }

    public void addJoinCriteriaList(List<? extends Criteria> joinCriteria) {
        if (joinCriteria == null || joinCriteria.isEmpty()) {
            return;
        }
        for (Criteria crit : joinCriteria) {
            criteriaNodes.add(RelationalPlanner.createSelectNode(crit, false));
        }
    }

    /**
     * This will rebuild the join tree starting at the join root.
     *
     * A left linear tree will be constructed out of the ordering of the
     * join sources.
     *
     * Criteria nodes are simply placed at the top of the join region in order
     * to be pushed by rule PushSelectSriteria.
     *
     */
    public void reconstructJoinRegoin() {
        LinkedHashMap<PlanNode, PlanNode> combined = new LinkedHashMap<PlanNode, PlanNode>(joinSourceNodes);
        combined.putAll(dependentJoinSourceNodes);

        PlanNode root = null;

        if (combined.size() < 2) {
            root = combined.values().iterator().next();
        } else {
            root = RulePlanJoins.createJoinNode();

            for (Map.Entry<PlanNode, PlanNode> entry : combined.entrySet()) {
                PlanNode joinSourceRoot = entry.getValue();
                if (root.getChildCount() == 2) {
                    PlanNode parentJoin = RulePlanJoins.createJoinNode();
                    parentJoin.addFirstChild(root);
                    parentJoin.addGroups(root.getGroups());
                    root = parentJoin;
                }
                root.addLastChild(joinSourceRoot);
                root.addGroups(entry.getKey().getGroups());
            }
        }
        LinkedList<PlanNode> criteria = new LinkedList<PlanNode>(dependentCritieraNodes);
        criteria.addAll(criteriaNodes);

        PlanNode parent = this.joinRoot.getParent();

        boolean isLeftChild = parent.getFirstChild() == this.joinRoot;

        parent.removeChild(joinRoot);

        for (PlanNode critNode : criteria) {
            critNode.removeFromParent();
            critNode.removeAllChildren();
            critNode.addFirstChild(root);
            root = critNode;
            critNode.removeProperty(NodeConstants.Info.IS_COPIED);
            critNode.removeProperty(NodeConstants.Info.EST_CARDINALITY);
        }

        if (isLeftChild) {
            parent.addFirstChild(root);
        } else {
            parent.addLastChild(root);
        }
        this.joinRoot = root;
    }

    /**
     * Will provide an estimate of cost by summing the estimated tuples flowing through
     * each intermediate join.
     *
     * @param joinOrder
     * @param metadata
     * @return
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws QueryPlannerException
     */
    public double scoreRegion(Object[] joinOrder, int startIndex, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context, boolean partial) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        List<Map.Entry<PlanNode, PlanNode>> joinSourceEntries = new ArrayList<Map.Entry<PlanNode, PlanNode>>(joinSourceNodes.entrySet());
        double totalIntermediatCost = 0;
        double cost = 1;

        HashSet<PlanNode> criteria = new HashSet<PlanNode>(this.criteriaNodes);
        HashSet<GroupSymbol> groups = new HashSet<GroupSymbol>(this.joinSourceNodes.size());
        HashSet<GroupSymbol> rightGroups = new HashSet<GroupSymbol>();
        List<Expression> leftExpressions = new ArrayList<Expression>();
        List<Expression> rightExpressions = new ArrayList<Expression>();
        HashSet<Criteria> nonEquiJoinCriteria = new HashSet<Criteria>();

        //only calculate up to the second to last as the last is not an intermediate result
        for (int i = 0; i < joinOrder.length - (partial?0:1); i++) {
            boolean hasUnknown = false;
            boolean shouldFilter = true;
            Integer source = (Integer)joinOrder[i];

            Map.Entry<PlanNode, PlanNode> entry = joinSourceEntries.get(source.intValue());
            PlanNode joinSourceRoot = entry.getValue();

            if (i >= startIndex) {
                //check to make sure that this group ordering satisfies the access patterns
                if (!this.unsatisfiedAccessPatterns.isEmpty() || this.containsNestedTable) {
                    PlanNode joinSource = entry.getKey();

                    Collection<GroupSymbol> requiredGroups = (Collection<GroupSymbol>)joinSource.getProperty(NodeConstants.Info.REQUIRED_ACCESS_PATTERN_GROUPS);

                    if (requiredGroups != null && !groups.containsAll(requiredGroups)) {
                        return Double.MAX_VALUE;
                    }
                }
            }

            rightGroups.clear();
            rightGroups.addAll(groups);
            groups.addAll(joinSourceRoot.getGroups());

            if (startIndex > 0 && i < startIndex) {
                continue;
            }

            float sourceCost = joinSourceRoot.getCardinality();

            List<PlanNode> applicableCriteria = null;

            CompoundCriteria cc = null;

            if (!criteria.isEmpty() && i > 0) {
                applicableCriteria = getJoinCriteriaForGroups(groups, criteria, false);
                if (applicableCriteria != null && !applicableCriteria.isEmpty()) {
                    cc = new CompoundCriteria();
                    for (PlanNode planNode : applicableCriteria) {
                        cc.addCriteria((Criteria) planNode.getProperty(NodeConstants.Info.SELECT_CRITERIA));
                    }
                }
            }

            boolean isInd = joinSourceRoot.hasProperty(Info.MAKE_IND);

            if (sourceCost == NewCalculateCostUtil.UNKNOWN_VALUE) {
                sourceCost = UNKNOWN_TUPLE_EST;

                hasUnknown = true;
                if (cc != null) {
                    shouldFilter = false;
                    sourceCost = (float)cost;
                    criteria.removeAll(applicableCriteria);
                    if (NewCalculateCostUtil.usesKey(cc, metadata) || (i >= 1 && joinSourceRoot.hasProperty(Info.MAKE_DEP) && !joinSourceRoot.hasBooleanProperty(Info.MAKE_NOT_DEP))) {
                        sourceCost = Math.min(UNKNOWN_TUPLE_EST, sourceCost * Math.min(NewCalculateCostUtil.UNKNOWN_JOIN_SCALING, sourceCost));
                    } else {
                        sourceCost = Math.min(UNKNOWN_TUPLE_EST, sourceCost * NewCalculateCostUtil.UNKNOWN_JOIN_SCALING * 8);
                    }
                }
            } else if (Double.isInfinite(sourceCost) || Double.isNaN(sourceCost)) {
                return Double.MAX_VALUE;
            } else if (i == 1 && applicableCriteria != null && !applicableCriteria.isEmpty()) {
                List<Object> key = Arrays.asList(joinOrder[0], joinOrder[1]);
                Float depJoinCost = null;
                if (depCache != null && depCache.containsKey(key)) {
                    depJoinCost = depCache.get(key);
                } else {
                    Integer indIndex = (Integer)joinOrder[0];
                    Map.Entry<PlanNode, PlanNode> indEntry = joinSourceEntries.get(indIndex.intValue());
                    PlanNode possibleInd = indEntry.getValue();

                    depJoinCost = getDepJoinCost(metadata, capFinder, context, possibleInd, applicableCriteria, joinSourceRoot);
                    if (depCache == null) {
                        depCache = new HashMap<List<Object>, Float>();
                    }
                    depCache.put(key, depJoinCost);
                }
                if (depJoinCost != null) {
                    sourceCost = depJoinCost;
                }
            }

            if (i > 0 && (applicableCriteria == null || applicableCriteria.isEmpty()) && (hasUnknown || isInd)) {
                if (isInd && !getJoinCriteriaForGroups(joinSourceRoot.getGroups(), criteria, true).isEmpty()) {
                    //TODO: it would be better to deeply cost / plan the various dependent scenarios
                    //rather than the check here and above for an initial dependent join
                    return Double.MAX_VALUE;
                }
                sourceCost *= 100; //penalty
            }

            double rightCost = cost;
            cost *= sourceCost;

            if (cc != null && applicableCriteria != null && shouldFilter) {
                //filter based upon notion of join
                leftExpressions.clear();
                rightExpressions.clear();
                nonEquiJoinCriteria.clear();

                Collection<GroupSymbol> leftGroups = joinSourceRoot.getGroups();

                RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, leftExpressions, rightExpressions, cc.getCriteria(), nonEquiJoinCriteria);

                if (!leftExpressions.isEmpty()) {
                    float leftNdv = NewCalculateCostUtil.getNDVEstimate(joinSourceRoot, metadata, sourceCost, leftExpressions, null);
                    float rightNdv = NewCalculateCostUtil.UNKNOWN_VALUE;

                    if (leftNdv != NewCalculateCostUtil.UNKNOWN_VALUE) {
                        Set<GroupSymbol> usedRight = GroupsUsedByElementsVisitor.getGroups(rightExpressions);
                        for (int j = 0; j < i; j++) {
                            Entry<PlanNode, PlanNode> previousEntry = joinSourceEntries.get((int) joinOrder[j]);
                            if (previousEntry.getValue().getGroups().containsAll(usedRight)) {
                                rightNdv = NewCalculateCostUtil.getNDVEstimate(previousEntry.getValue(), metadata, sourceCost, rightExpressions, null);
                                break;
                            }
                        }
                    }

                    if (leftNdv != NewCalculateCostUtil.UNKNOWN_VALUE && rightNdv != NewCalculateCostUtil.UNKNOWN_VALUE) {
                        cost = (sourceCost / leftNdv) * (rightCost / rightNdv) * Math.min(leftNdv, rightNdv);
                    } else {
                        //check for a key
                        //just use the default logic
                        nonEquiJoinCriteria.clear();
                    }
                } else {
                    //just use the default logic
                    nonEquiJoinCriteria.clear();
                }

                for (PlanNode criteriaNode : applicableCriteria) {
                    Criteria crit = (Criteria) criteriaNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                    if (!nonEquiJoinCriteria.contains(crit)) {
                        continue;
                    }
                    float filter = ((Float)criteriaNode.getProperty(NodeConstants.Info.EST_SELECTIVITY)).floatValue();

                    cost *= filter;
                }

                criteria.removeAll(applicableCriteria);
            }
            totalIntermediatCost += cost;
        }

        return totalIntermediatCost;
    }

    private Float getDepJoinCost(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, CommandContext context,
            PlanNode indNode, List<PlanNode> applicableCriteria,
            PlanNode depNode) throws QueryMetadataException,
            TeiidComponentException, QueryPlannerException {
        if (depNode.hasBooleanProperty(Info.MAKE_NOT_DEP)) {
            return null;
        }

        float indCost = indNode.getCardinality();

        if (indCost == NewCalculateCostUtil.UNKNOWN_VALUE) {
            return null;
        }

        List<Criteria> crits = new ArrayList<Criteria>(applicableCriteria.size());
        for (PlanNode planNode : applicableCriteria) {
            crits.add((Criteria) planNode.getProperty(NodeConstants.Info.SELECT_CRITERIA));
        }
        List<Expression> leftExpressions = new LinkedList<Expression>();
        List<Expression> rightExpressions = new LinkedList<Expression>();
        RuleChooseJoinStrategy.separateCriteria(indNode.getGroups(), depNode.getGroups(), leftExpressions, rightExpressions, crits, new LinkedList<Criteria>());
        if (leftExpressions.isEmpty()) {
            return null;
        }
        return NewCalculateCostUtil.computeCostForDepJoin(indNode, depNode, leftExpressions, rightExpressions, metadata, capFinder, context).expectedCardinality;
    }

    /**
     *  Returns true if every element in an unsatisfied access pattern can be satisfied by the current join criteria
     *  This does not necessarily mean that a join tree will be successfully created
     */
    public boolean isSatisfiable() {
        for (Collection<AccessPattern> accessPatterns : getUnsatisfiedAccessPatterns()) {
            boolean matchedAll = false;
            for (AccessPattern ap : accessPatterns) {
                if (dependentCriteriaElements.keySet().containsAll(ap.getUnsatisfied())) {
                    matchedAll = true;
                    break;
                }
            }
            if (!matchedAll) {
                return false;
            }
        }

        return true;
    }

    public void initializeCostingInformation(QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        for (PlanNode node : joinSourceNodes.values()) {
            NewCalculateCostUtil.computeCostForTree(node, metadata);
        }

        estimateCriteriaSelectivity(metadata);
    }

    /**
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private void estimateCriteriaSelectivity(QueryMetadataInterface metadata) throws QueryMetadataException,
                                                                             TeiidComponentException {
        for (PlanNode node : criteriaNodes) {
            Criteria crit = (Criteria)node.getProperty(NodeConstants.Info.SELECT_CRITERIA);

            float[] baseCosts = new float[] {100, 10000, 1000000};

            float filterValue = 0;

            for (int j = 0; j < baseCosts.length; j++) {
                float filter = NewCalculateCostUtil.recursiveEstimateCostOfCriteria(baseCosts[j], node, crit, metadata);

                filterValue += filter/baseCosts[j];
            }

            filterValue /= baseCosts.length;

            node.setProperty(NodeConstants.Info.EST_SELECTIVITY, new Float(filterValue));
        }
    }

    /**
     *  Initializes information on the joinRegion about dependency information, etc.
     *
     *  TODO: assumptions are made here about how dependent criteria must look that are a little restrictive
     */
    public void initializeJoinInformation() {
        critieriaToSourceMap = new HashMap<PlanNode, Set<PlanNode>>();

        LinkedList<PlanNode> crits = new LinkedList<PlanNode>(criteriaNodes);
        crits.addAll(dependentCritieraNodes);

        LinkedHashMap<PlanNode, PlanNode> source = new LinkedHashMap<PlanNode, PlanNode>(joinSourceNodes);
        source.putAll(dependentJoinSourceNodes);

        for (PlanNode critNode : crits) {
            for (GroupSymbol group : critNode.getGroups()) {
                for (PlanNode node : source.keySet()) {
                    if (node.getGroups().contains(group)) {
                        Set<PlanNode> sources = critieriaToSourceMap.get(critNode);
                        if (sources == null) {
                            sources = new HashSet<PlanNode>();
                            critieriaToSourceMap.put(critNode, sources);
                        }
                        sources.add(node);
                        break;
                    }
                }
            }
        }

        if (unsatisfiedAccessPatterns.isEmpty()) {
            return;
        }

        Map<GroupSymbol, PlanNode> dependentGroupToSourceMap = new HashMap<GroupSymbol, PlanNode>();

        for (PlanNode node : dependentJoinSourceNodes.keySet()) {
            for (GroupSymbol symbol : node.getGroups()) {
                dependentGroupToSourceMap.put(symbol, node);
            }
        }

        for (Iterator<PlanNode> i = getCriteriaNodes().iterator(); i.hasNext();) {
            PlanNode node = i.next();

            for (GroupSymbol symbol : node.getGroups()) {
                if (dependentGroupToSourceMap.containsKey(symbol)) {
                    i.remove();
                    dependentCritieraNodes.add(node);
                    break;
                }
            }
        }

        dependentCriteriaElements = new HashMap<ElementSymbol, Set<Collection<GroupSymbol>>>();

        for (PlanNode critNode : dependentCritieraNodes) {
            Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            if(!(crit instanceof CompareCriteria)) {
                continue;
            }
            CompareCriteria compCrit = (CompareCriteria) crit;
            if(compCrit.getOperator() != CompareCriteria.EQ) {
                continue;
            }
            CompareCriteria compareCriteria = (CompareCriteria)crit;
            //this may be a proper dependent join criteria
            Collection<ElementSymbol>[] critElements = new Collection[2];
            critElements[0] = ElementCollectorVisitor.getElements(compareCriteria.getLeftExpression(), true);
            if (critElements[0].isEmpty()) {
                continue;
            }
            critElements[1] = ElementCollectorVisitor.getElements(compareCriteria.getRightExpression(), true);
            if (critElements[1].isEmpty()) {
                continue;
            }
            for (int expr = 0; expr < critElements.length; expr++) {
                //simplifying assumption that there will be a single element on the dependent side
                if (critElements[expr].size() != 1) {
                    continue;
                }
                ElementSymbol elem = critElements[expr].iterator().next();
                if (!dependentGroupToSourceMap.containsKey(elem.getGroupSymbol())) {
                    continue;
                }
                //this is also a simplifying assumption.  don't consider criteria that can't be pushed
                if (containsFunctionsThatCannotBePushed(expr==0?compareCriteria.getRightExpression():compareCriteria.getLeftExpression())) {
                    continue;
                }
                Set<Collection<GroupSymbol>> independentGroups = dependentCriteriaElements.get(elem);
                if (independentGroups == null) {
                    independentGroups = new HashSet<Collection<GroupSymbol>>();
                    dependentCriteriaElements.put(elem, independentGroups);
                }
                //set the other side as independent elements
                independentGroups.add(GroupsUsedByElementsVisitor.getGroups(critElements[(expr+1)%2]));
            }
        }
    }

    /**
     * Returns true if the expression is, or contains, any functions that cannot be pushed
     * down to the source
     * @param expression
     * @return
     * @since 4.2
     */
    private static boolean containsFunctionsThatCannotBePushed(Expression expression) {
        Iterator functions = FunctionCollectorVisitor.getFunctions(expression, true).iterator();
        while (functions.hasNext()) {
            Function function = (Function)functions.next();
            if (function.getFunctionDescriptor().getPushdown() == PushDown.CANNOT_PUSHDOWN) {
                return true;
            }
        }
        return false;
    }

    //TODO: this should be better than a linear search
    protected List<PlanNode> getJoinCriteriaForGroups(Set<GroupSymbol> groups, Collection<PlanNode> nodes, boolean any) {
        List<PlanNode> result = new LinkedList<PlanNode>();

        for (PlanNode critNode : nodes) {
            if (any?!Collections.disjoint(groups, critNode.getGroups()):groups.containsAll(critNode.getGroups())) {
                Criteria crit = (Criteria) critNode.getProperty(Info.SELECT_CRITERIA);
                if (crit instanceof CompareCriteria && ((CompareCriteria) crit).isOptional()) {
                    continue;
                }
                result.add(critNode);
            }
        }

        return result;
    }

    public void changeJoinOrder(Object[] joinOrder) {
        List<Map.Entry<PlanNode, PlanNode>> joinSourceEntries = new ArrayList<Map.Entry<PlanNode, PlanNode>>(joinSourceNodes.entrySet());

        for (int i = 0; i < joinOrder.length; i++) {
            Integer source = (Integer)joinOrder[i];

            Map.Entry<PlanNode, PlanNode> entry = joinSourceEntries.get(source.intValue());

            this.joinSourceNodes.remove(entry.getKey());
            this.joinSourceNodes.put(entry.getKey(), entry.getValue());
        }
    }

    public void setLeft(JoinRegion left) {
        this.left = left;
    }

    public JoinRegion getLeft() {
        return left;
    }

}