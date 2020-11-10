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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
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
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.DependentSetCriteria.AttributeComparison;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.symbol.WindowSpecification;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


public final class RulePushSelectCriteria implements OptimizerRule {

    private List<PlanNode> createdNodes;

    public List<PlanNode> getCreatedNodes() {
        return createdNodes;
    }

    public void setCreatedNodes(List<PlanNode> createdNodes) {
        this.createdNodes = createdNodes;
    }

    /**
     * Execute the rule as described in the class comments.
     * @param plan Incoming query plan, may be modified during method and may be returned from method
     * @param metadata Metadata source
     * @param rules Rules from optimizer rule stack, may be manipulated during method
     * @return Updated query plan if rule fired, else original query plan
     */
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        // Initialize movedNode to true so the loop will start
        boolean movedAnyNode = true;

        // Create set of nodes that no longer need to be considered
        Set<PlanNode> deadNodes = new HashSet<PlanNode>();

        // Loop while criteria nodes are still being moved
        while(movedAnyNode) {

            // Reset flag to false for this iteration
            movedAnyNode = false;

            // Find criteria nodes that could be pushed
            List<PlanNode> critNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SELECT);
            Collections.reverse(critNodes);
            for (PlanNode critNode : critNodes) {
                boolean isPhantom = critNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM);
                boolean isCopied = critNode.hasBooleanProperty(NodeConstants.Info.IS_COPIED);
                boolean isPushed = critNode.hasBooleanProperty(NodeConstants.Info.IS_PUSHED);
                if (isPhantom || isCopied || isPushed || deadNodes.contains(critNode)) {
                    continue;
                }

                PlanNode sourceNode = findOriginatingNode(metadata, capFinder, critNode, analysisRecord);

                if(sourceNode == null) {
                    deadNodes.add(critNode);
                    continue;
                }
                boolean moved = pushTowardOriginatingNode(sourceNode, critNode, metadata, capFinder);

                if(critNode.hasBooleanProperty(Info.IS_PUSHED) || (critNode.getGroups().isEmpty() && critNode.getSubqueryContainers().isEmpty()) || !atBoundary(critNode, sourceNode)) {
                    deadNodes.add(critNode);
                    movedAnyNode |= moved;
                    continue;
                }

                switch (sourceNode.getType()) {
                    case NodeConstants.Types.SOURCE:
                    {
                        Boolean acrossFrame = pushAcrossFrame(sourceNode, critNode, metadata, capFinder, context);
                        if (acrossFrame != null) {
                            moved = acrossFrame;
                        } else {
                            movedAnyNode = true; //new nodes created
                        }
                        break;
                    }
                    case NodeConstants.Types.JOIN:
                    {
                        //pushing below a join is not necessary under an access node
                        if (NodeEditor.findParent(critNode, NodeConstants.Types.ACCESS) == null && critNode.getSubqueryContainers().isEmpty()) {
                            moved = handleJoinCriteria(sourceNode, critNode, metadata);
                        }
                        break;
                    }
                    case NodeConstants.Types.GROUP:
                    {
                        moved = pushAcrossGroupBy(sourceNode, critNode, metadata, true, capFinder);
                    }
                }

                if (!moved) {
                    deadNodes.add(critNode);
                } else {
                    movedAnyNode = true;
                }
            }
        }

        return plan;
    }

    boolean pushAcrossGroupBy(PlanNode sourceNode,
            PlanNode critNode, QueryMetadataInterface metadata, boolean inPlan, CapabilitiesFinder capFinder)
            throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        if (critNode.hasBooleanProperty(NodeConstants.Info.IS_HAVING)) {
            return false;
        }
        if (critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
            PlanNode accessNode = NodeEditor.findParent(critNode, NodeConstants.Types.ACCESS);
            if (accessNode != null) {
                List<Expression> cols = (List<Expression>) sourceNode.getProperty(Info.GROUP_COLS);
                if (cols != null) {
                    boolean hasExpression = false;
                    boolean hasLiteral = false;
                    for (final Iterator<Expression> iterator = cols.iterator(); iterator.hasNext();) {
                        Expression ex = iterator.next();
                        hasExpression |= !(ex instanceof ElementSymbol);
                        hasLiteral |= EvaluatableVisitor.willBecomeConstant(ex, true);
                    }
                    if (hasLiteral || (hasExpression && !CapabilitiesUtil.supports(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder))) {
                        markDependent(critNode, accessNode, metadata, capFinder);
                        critNode.setProperty(Info.IS_HAVING, true);
                        return false;
                    }
                }
            }
        }
        if (sourceNode.hasBooleanProperty(Info.ROLLUP)) {
            //there is a pre/post affect.  we can push, but then there is
            //null filtering that may need to occur
            //the more complicated approach would be to determine the effective null tests,
            //but instead we'll just clone the node
            if (inPlan) {
                PlanNode copy = copyNode(critNode);
                critNode.setProperty(Info.IS_PUSHED, true);
                critNode.setProperty(Info.IS_HAVING, true);
                critNode.getFirstChild().addAsParent(copy);
                critNode = copy;
            }
        }
        boolean moved = false;
        SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        FrameUtil.convertNode(critNode, null, null, symbolMap.asMap(), metadata, true);
        if (inPlan) {
            NodeEditor.removeChildNode(critNode.getParent(), critNode);
            sourceNode.getFirstChild().addAsParent(critNode);
        }
        moved = true;
        if (critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
            PlanNode accessNode = NodeEditor.findParent(critNode, NodeConstants.Types.ACCESS);
            if (accessNode != null) {
                markDependent(critNode, accessNode, metadata, capFinder);
                moved = false; //terminal position
            }
        }
        return moved;
    }

    private PlanNode findOriginatingNode(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, PlanNode critNode, AnalysisRecord record)
            throws TeiidComponentException, QueryMetadataException {
        if (critNode.getGroups().isEmpty()) {
            //check to see if pushing may impact cardinality
            PlanNode groupNode = NodeEditor.findNodePreOrder(critNode, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE);
            if (groupNode != null && !groupNode.hasCollectionProperty(NodeConstants.Info.GROUP_COLS)) {
                return groupNode;
            }

            Object modelId = getSubqueryModelId(metadata, capFinder, critNode, record);
            if (modelId != null) {
                for (PlanNode node : NodeEditor.findAllNodes(critNode, NodeConstants.Types.SOURCE)) {
                    GroupSymbol group = node.getGroups().iterator().next();
                    Object srcModelID = metadata.getModelID(group.getMetadataID());
                    if(CapabilitiesUtil.isSameConnector(srcModelID, modelId, metadata, capFinder)) {
                        return node;
                    }
                }
            }
        }
        return FrameUtil.findOriginatingNode(critNode, critNode.getGroups());
    }

    private Object getSubqueryModelId(QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, PlanNode critNode, AnalysisRecord record)
            throws TeiidComponentException, QueryMetadataException {
        Object modelId = null;
        for (SubqueryContainer subqueryContainer : critNode.getSubqueryContainers()) {
            Object validId = CriteriaCapabilityValidatorVisitor.validateSubqueryPushdown(subqueryContainer, null, metadata, capFinder, record);
            if (validId == null) {
                return null;
            }
            if (modelId == null) {
                modelId = validId;
            } else if (!CapabilitiesUtil.isSameConnector(modelId, validId, metadata, capFinder)) {
                return null;
            }
        }
        return modelId;
    }

    /**
     * Handles multi-group criteria originating at the given joinNode
     *
     * @param joinNode
     * @param critNode
     * @return
     */
    private boolean handleJoinCriteria(PlanNode joinNode, PlanNode critNode, QueryMetadataInterface metadata) {
        JoinType jt = (JoinType)joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);

        if (jt == JoinType.JOIN_CROSS || jt == JoinType.JOIN_INNER) {
            return moveCriteriaIntoOnClause(critNode, joinNode);
        }
        JoinType optimized = JoinUtil.optimizeJoinType(critNode, joinNode, metadata, true);

        if (optimized == JoinType.JOIN_INNER) {
            moveCriteriaIntoOnClause(critNode, joinNode);
            return true; //return true since the join type has changed
        }
        return false;
    }

    /**
     * @param critNode
     * @param joinNode
     */
    private boolean moveCriteriaIntoOnClause(PlanNode critNode,
                                          PlanNode joinNode) {
        NodeEditor.removeChildNode(critNode.getParent(), critNode);

        List joinCriteria = (List)joinNode.getProperty(NodeConstants.Info.JOIN_CRITERIA);
        Criteria criteria = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);

        //since the parser uses EMPTY_LIST, check for size 0 also
        if (joinCriteria == null || joinCriteria.size() == 0) {
            joinCriteria = new LinkedList();
            joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
        }

        if (joinCriteria.contains(criteria)) {
            return false;
        }
        boolean moved = false;
        if(critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
            if (criteria instanceof DependentSetCriteria) {
                DependentSetCriteria dsc = (DependentSetCriteria)criteria;
                if (dsc.hasMultipleAttributes()) {
                    //split the array based upon the join children.
                    List<DependentSetCriteria.AttributeComparison> joinExprs = new ArrayList<DependentSetCriteria.AttributeComparison>();
                    List<DependentSetCriteria.AttributeComparison> leftExprs = new ArrayList<DependentSetCriteria.AttributeComparison>();
                    List<DependentSetCriteria.AttributeComparison> rightExprs = new ArrayList<DependentSetCriteria.AttributeComparison>();
                    PlanNode leftJoinSource = FrameUtil.findJoinSourceNode(joinNode.getFirstChild());
                    PlanNode rightJoinSource = FrameUtil.findJoinSourceNode(joinNode.getLastChild());
                    for (int i = 0; i < dsc.getAttributes().size(); i++) {
                        DependentSetCriteria.AttributeComparison comp = dsc.getAttributes().get(i);
                        Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(comp.dep);
                        if (leftJoinSource.getGroups().containsAll(groups)) {
                            leftExprs.add(comp);
                        } else if (rightJoinSource.getGroups().containsAll(groups)){
                            rightExprs.add(comp);
                        } else {
                            joinExprs.add(comp);
                        }
                    }
                    criteria = RuleChooseDependent.createDependentSetCriteria(dsc.getContextSymbol(), joinExprs);
                    PlanNode left = RuleChooseDependent.createDependentSetNode(dsc.getContextSymbol(), leftExprs);
                    if (left != null) {
                        moved = true;
                        joinNode.getFirstChild().addAsParent(left);
                    }
                    PlanNode right = RuleChooseDependent.createDependentSetNode(dsc.getContextSymbol(), rightExprs);
                    if (right != null) {
                        moved = true;
                        joinNode.getLastChild().addAsParent(right);
                    }
                }
            }
            if (criteria != null) {
                joinNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
            }
        }
        if (criteria != null) {
            joinCriteria.add(criteria);
        }

        if (!joinCriteria.isEmpty() && joinNode.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_CROSS) {
            joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
        }
        return moved;
    }

    /**
     *
     * @param critNode
     * @param metadata
     * @param capFinder
     * @throws QueryPlannerException
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    boolean pushTowardOriginatingNode(PlanNode sourceNode, PlanNode critNode, final QueryMetadataInterface metadata, final CapabilitiesFinder capFinder)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        boolean groupSelects = createdNodes == null && sourceNode.getParent().getType() == NodeConstants.Types.SELECT && sourceNode.getChildCount() == 0;

        //to keep a stable criteria ordering, move the sourceNode to the top of the criteria chain
        while (sourceNode.getParent().getType() == NodeConstants.Types.SELECT) {
            sourceNode = sourceNode.getParent();
            if (sourceNode == critNode) {
                return false;
            }
        }

        // See how far we can move it towards the SOURCE node
        final PlanNode destination = examinePath(critNode, sourceNode, metadata, capFinder);
        boolean result = false;
        if (createdNodes == null & destination.getType() == NodeConstants.Types.ACCESS && isDependentFinalDestination(critNode, destination)) {
            Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            if (isMultiAttributeDependentSet(crit)) {
                result = splitSet(critNode, new DependentNodeTest() {

                    @Override
                    public boolean isValid(PlanNode copyNode) throws QueryMetadataException,
                            QueryPlannerException, TeiidComponentException {
                        return RuleRaiseAccess.canRaiseOverSelect(destination, metadata, capFinder, copyNode, null);
                    }
                }, (DependentSetCriteria)crit, destination);
            }
        }

        NodeEditor.removeChildNode(critNode.getParent(), critNode);
        destination.addAsParent(critNode);
        if (groupSelects && destination == sourceNode && !critNode.hasBooleanProperty(Info.IS_TEMPORARY) && !destination.hasBooleanProperty(Info.IS_TEMPORARY)) {
            //Help with the detection of composite keys in pushed criteria
            RuleMergeCriteria.mergeChain(critNode, metadata);
        }

        return result;
    }

    private interface DependentNodeTest {
        boolean isValid(PlanNode copyNode) throws QueryMetadataException, QueryPlannerException, TeiidComponentException;
    }

    private boolean splitSet(PlanNode critNode, DependentNodeTest test, DependentSetCriteria dscOrig, PlanNode destination)
            throws QueryMetadataException, TeiidComponentException,
            QueryPlannerException {
        boolean result = false;
        List<DependentSetCriteria> dscList = splitDependentSetCriteria(dscOrig, false, null);
        List<DependentSetCriteria.AttributeComparison> pushable = new ArrayList<AttributeComparison>();
        List<DependentSetCriteria.AttributeComparison> nonPushable = new ArrayList<AttributeComparison>();
        for (DependentSetCriteria dsc : dscList) {
            PlanNode copyNode = copyNode(critNode);
            setCriteria(dsc, copyNode);
            if (test.isValid(copyNode)) {
                pushable.add(dsc.getAttributes().get(0));
           } else {
                nonPushable.add(dsc.getAttributes().get(0));
            }
        }
        if (!pushable.isEmpty()) {
            result = true; //signal that we should run again
            if (nonPushable.isEmpty()) {
                throw new AssertionError("should not be completely pushed"); //$NON-NLS-1$
            }
            setCriteria(RuleChooseDependent.createDependentSetCriteria(dscOrig.getContextSymbol(), nonPushable), critNode);
            PlanNode copyNode = copyNode(critNode);
            setCriteria(RuleChooseDependent.createDependentSetCriteria(dscOrig.getContextSymbol(), pushable), copyNode);
            destination.addAsParent(copyNode); //it should be pushed in the next run
        }
        return result;
    }

    private void setCriteria(DependentSetCriteria dsc, PlanNode copyNode) {
        copyNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, dsc);
        copyNode.getGroups().clear();
        copyNode.addGroups(GroupsUsedByElementsVisitor.getGroups(dsc));
    }

    /**
     * Examine the path from crit node to source node to determine how far down a node
     * can be pushed.
     * @return destinationChild
     */
    PlanNode examinePath(PlanNode critNode, PlanNode sourceNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
        throws QueryPlannerException, TeiidComponentException {

        // Walk from source node up to critNode to build list of intervening nodes
        Stack<PlanNode> path = new Stack<PlanNode>();
        PlanNode currentNode = sourceNode.getParent();
        while(currentNode != critNode) {
            path.push(currentNode);
            currentNode = currentNode.getParent();
        }

        // Examine path in reverse order (by popping stack)
        while(! path.empty()) {
            currentNode = path.pop();

            // Look for situations where we don't allow SELECT to be pushed
            switch (currentNode.getType()) {
            case NodeConstants.Types.ACCESS:
                try {
                    if (!RuleRaiseAccess.canRaiseOverSelect(currentNode, metadata, capFinder, critNode, null)) {
                        return currentNode;
                    }
                    if (!RuleRaiseAccess.checkConformedSubqueries(currentNode, critNode, this.createdNodes == null)) {
                        return currentNode;
                    }
                    if (this.createdNodes == null) {
                        satisfyConditions(critNode, currentNode, metadata);
                    }

                    if (isDependentFinalDestination(critNode, currentNode)) {
                        //once a dependent crit node is pushed, don't bother pushing it further into the command
                        //dependent access node will use this as an assumption for where dependent sets can appear in the command
                        markDependent(critNode,currentNode, metadata, capFinder);
                        return currentNode.getFirstChild();
                    }
                } catch(QueryMetadataException e) {
                     throw new QueryPlannerException(QueryPlugin.Event.TEIID30267, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30267, currentNode.getGroups()));
                }
                break;
            case NodeConstants.Types.JOIN:
                //pushing below a join is not necessary under an access node
                if (this.createdNodes == null && NodeEditor.findParent(currentNode, NodeConstants.Types.ACCESS) != null) {
                    return currentNode;
                }

                // Check whether this criteria is on the inner side of an outer join.
                // If so, can't push past the join
                JoinType jt = JoinUtil.getJoinTypePreventingCriteriaOptimization(currentNode, critNode);

                if(jt != null) {
                    //if we successfully optimized then this should no longer inhibit the criteria from being pushed
                    //since the criteria must then be on the outer side of an outer join or on either side of an inner join

                    JoinType optimized = JoinUtil.optimizeJoinType(critNode, currentNode, metadata, this.createdNodes == null);

                    if (optimized == null || optimized == JoinType.JOIN_FULL_OUTER) {
                        return currentNode;
                    }
                }

                if (this.createdNodes == null) {
                    satisfyConditions(critNode, currentNode, metadata);
                }
                break;
            default:
                if (FrameUtil.isOrderedOrStrictLimit(currentNode)) {
                    return currentNode;
                }
            }
        }
        return sourceNode;
    }

    private boolean isMultiAttributeDependentSet(Criteria crit) {
        return crit instanceof DependentSetCriteria && ((DependentSetCriteria)crit).hasMultipleAttributes();
    }

    private boolean isDependentFinalDestination(PlanNode critNode,
            PlanNode currentNode) {
        return critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)
                && NodeEditor.findNodePreOrder(currentNode.getFirstChild(), NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE) == null;
    }

    private void markDependent(PlanNode critNode, PlanNode accessNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException {
        //once a dependent crit node is pushed, don't bother pushing it further into the command
        //dependent access node will use this as an assumption for where dependent sets can appear in the command
        critNode.setProperty(NodeConstants.Info.IS_PUSHED, Boolean.TRUE);
        if (createdNodes != null) {
            return; //this is during a planning run and should not cause additional side-effects
        }
        accessNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
        Criteria crit = (Criteria) critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        if (isMultiAttributeDependentSet(crit)) {
            //split the criteria as needed
            List<DependentSetCriteria> crits = splitDependentSetCriteria((DependentSetCriteria) crit, CapabilitiesUtil.supports(Capability.ARRAY_TYPE, RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder), metadata);
            critNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, new CompoundCriteria(crits));
        }

        Collection<ElementSymbol> elements = null;
        for (PlanNode joinNode : NodeEditor.findAllNodes(accessNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE)) {
            List<Criteria> joinCriteria = (List<Criteria>) joinNode.getProperty(Info.JOIN_CRITERIA);
            if (joinCriteria == null) {
                continue;
            }
            for (Criteria joinPredicate : joinCriteria) {
                if (!(joinPredicate instanceof CompareCriteria)) {
                    continue;
                }
                CompareCriteria cc = (CompareCriteria)joinPredicate;
                if (!cc.isOptional()) {
                    continue;
                }
                if (elements == null) {
                    elements = ElementCollectorVisitor.getElements((LanguageObject)critNode.getProperty(Info.SELECT_CRITERIA), true);
                }
                if (!Collections.disjoint(elements, ElementCollectorVisitor.getElements(cc, false))) {
                    cc.setOptional(false);
                }
            }
        }
    }

    private List<DependentSetCriteria> splitDependentSetCriteria(DependentSetCriteria dsc, boolean supportsArray, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        List<AttributeComparison> attributes = dsc.getAttributes();
        List<DependentSetCriteria> crits = new ArrayList<DependentSetCriteria>(attributes.size());

        Map<Object, List<AttributeComparison>> splits = new LinkedHashMap<Object, List<AttributeComparison>>();

        for (int i = 0; i < attributes.size(); i++) {
            Object key = null;
            DependentSetCriteria.AttributeComparison comp = attributes.get(i);
            if (supportsArray && (comp.dep instanceof ElementSymbol)) {
                ElementSymbol es = (ElementSymbol)comp.dep;
                GroupSymbol group = es.getGroupSymbol();
                if (!metadata.isVirtualGroup(group.getMetadataID())) {
                    key = group;
                }
                //TODO: we could try to determine further if this is allowable
                //for now since we are pushing as far as we can, this is a good indication,
                //that it will need split
            }
            if (key == null) {
                key = splits.size();
            }
            List<AttributeComparison> comps = splits.get(key);
            if (comps == null) {
                comps = new ArrayList<DependentSetCriteria.AttributeComparison>(2);
                splits.put(key, comps);
            }
            comps.add(comp);
        }

        for (List<AttributeComparison> comps : splits.values()) {
            DependentSetCriteria crit = RuleChooseDependent.createDependentSetCriteria(dsc.getContextSymbol(), comps);
            crit.setMakeDepOptions(dsc.getMakeDepOptions());
            crits.add(crit);
        }
        return crits;
    }

    Boolean pushAcrossFrame(PlanNode sourceNode, PlanNode critNode, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        //ensure that the criteria can be pushed further
        if (sourceNode.getChildCount() == 1 && FrameUtil.isOrderedOrStrictLimit(sourceNode.getFirstChild())) {
            return false;
        }

        //check to see if this is a move across a union
        if (sourceNode.getChildCount() > 0) {
            PlanNode child = sourceNode.getFirstChild();
            child = FrameUtil.findOriginatingNode(child, child.getGroups());
            if (child != null && child.getType() == NodeConstants.Types.SET_OP) {
                //only allow criteria without subqueires - node cloning doesn't allow for the proper creation of
                //multiple nodes with the same subqueries
                if (child == sourceNode.getFirstChild() && critNode.getSubqueryContainers().isEmpty()) {
                    return pushAcrossSetOp(critNode, child, metadata, capFinder, context);
                }
                //this could be an access node in the middle of the source and set op,
                //it is an odd case that is not supported for now
                return false;
            }
        }

        // See if we can move it towards the SOURCE node
        return moveNodeAcrossFrame(critNode, sourceNode, metadata);
    }

    /**
     * All nodes between critNode and sourceNode must be SELECT nodes.
     */
    boolean atBoundary(PlanNode critNode, PlanNode sourceNode) {
        // Walk from source node to critNode to check each intervening node
        PlanNode currentNode = sourceNode.getParent();
        while(currentNode != critNode) {
            if(currentNode.getType() != NodeConstants.Types.SELECT) {
                return false;
            }

            currentNode = currentNode.getParent();
        }

        return true;
    }

    Boolean moveNodeAcrossFrame(PlanNode critNode, PlanNode sourceNode, final QueryMetadataInterface metadata)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

          // Check that sourceNode has a child to push across
        if(sourceNode.getChildCount() == 0) {
            return false;
        }

        final PlanNode projectNode = NodeEditor.findNodePreOrder(sourceNode.getFirstChild(), NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);
        if(FrameUtil.isProcedure(projectNode)) {
            return false;
        }

        final SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);

        final GroupSymbol sourceGroup = sourceNode.getGroups().iterator().next();
        if (!placeConvertedSelectNode(critNode, sourceGroup, projectNode, symbolMap, metadata)) {
            if (createdNodes == null) {
                Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
                if (isMultiAttributeDependentSet(crit) && splitSet(critNode, new DependentNodeTest() {

                            @Override
                            public boolean isValid(PlanNode copyNode) throws QueryMetadataException,
                                    QueryPlannerException, TeiidComponentException {
                                return createConvertedSelectNode(copyNode, sourceGroup, projectNode, symbolMap, metadata) != null;
                            }
                        }, (DependentSetCriteria)crit, sourceNode)) {
                    return null;
                }
            }
            return false;
        }

        if (createdNodes == null) {
            satisfyConditions(critNode, sourceNode, metadata);
        }

        // Mark critNode as a "phantom"
        critNode.setProperty(NodeConstants.Info.IS_PHANTOM, Boolean.TRUE);

        return true;
    }

    /**
     * @param critNode
     * @param sourceNode
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    static void satisfyConditions(PlanNode critNode,
                                       PlanNode sourceNode, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        List aps = (List)sourceNode.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
        Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);

        if (sourceNode.hasBooleanProperty(Info.IS_MULTI_SOURCE) && crit instanceof CompareCriteria) {
            CompareCriteria cc = (CompareCriteria)crit;
            if (cc.getLeftExpression() instanceof ElementSymbol && cc.getRightExpression() instanceof Constant) {
                ElementSymbol es = (ElementSymbol)cc.getLeftExpression();
                if (metadata.isMultiSourceElement(es.getMetadataID())) {
                    sourceNode.setProperty(Info.IS_MULTI_SOURCE, false);
                    sourceNode.setProperty(Info.SOURCE_NAME, ((Constant)cc.getRightExpression()).getValue());
                }
            }
        }

        if (aps == null) {
            return;
        }

        Collection<ElementSymbol> elements = getElementsIncriteria(crit);

        boolean removeAps = satisfyAccessPatterns(aps, elements);
        if (removeAps) {
            sourceNode.removeProperty(NodeConstants.Info.ACCESS_PATTERNS);
            return;
        }

        Collections.sort(aps);
    }

    static Collection<ElementSymbol> getElementsIncriteria(Criteria crit) {
        Collection<ElementSymbol> elements = new HashSet<ElementSymbol>();
        boolean first = true;
        if(crit instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) crit;
            for(Criteria subCrit : compCrit.getCriteria()) {
                if(compCrit.getOperator() == CompoundCriteria.AND || first) {
                    first = false;
                    elements.addAll(getElementsIncriteria(subCrit));
                } else {
                    elements.retainAll(getElementsIncriteria(subCrit));
                }
            }
        } else {
            elements.addAll(ElementCollectorVisitor.getElements(crit, true));
        }
        return elements;
    }

    /**
     * @param aps
     * @param elements
     * @return
     */
    static boolean satisfyAccessPatterns(List<AccessPattern> aps, Collection<ElementSymbol> elements) {
        for (AccessPattern ap : aps) {
            ap.getUnsatisfied().removeAll(elements);
            if (ap.getUnsatisfied().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    PlanNode copyNode(PlanNode critNode) {
        // Create new copy node
        PlanNode copyNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);

        // Copy criteria
        Criteria crit = (Criteria) critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        Criteria copyCrit = (Criteria) crit.clone();
        copyNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, copyCrit);
        copyNode.addGroups(critNode.getGroups());
        if(critNode.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
            copyNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
        }
        if (critNode.hasBooleanProperty(NodeConstants.Info.IS_TEMPORARY)) {
            copyNode.setProperty(NodeConstants.Info.IS_TEMPORARY, Boolean.TRUE);
        }
        if (createdNodes != null) {
            createdNodes.add(copyNode);
        }
        return copyNode;
    }

    boolean pushAcrossSetOp(PlanNode critNode, PlanNode setOp, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        // Find source node above union and grab the symbol map
        PlanNode sourceNode = NodeEditor.findParent(setOp, NodeConstants.Types.SOURCE);
        GroupSymbol virtualGroup = sourceNode.getGroups().iterator().next();
        if (createdNodes == null) {
            satisfyConditions(critNode, sourceNode, metadata);
        }

        SymbolMap symbolMap = (SymbolMap) sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        SymbolMap childMap = symbolMap;

        // Move criteria to first child of union - names are the same, so no symbol mapping
        LinkedList<PlanNode> unionChildren = new LinkedList<PlanNode>();
        collectUnionChildren(setOp, unionChildren);

        int movedCount = 0;

        for (PlanNode planNode : unionChildren) {
              // Find first project node
            PlanNode projectNode = NodeEditor.findNodePreOrder(planNode, NodeConstants.Types.PROJECT);

            if (childMap == null) {
                childMap = SymbolMap.createSymbolMap(symbolMap.getKeys(), (List) projectNode.getProperty(NodeConstants.Info.PROJECT_COLS));
            }

            //we cannot simply move the node in the case where placing above or below the access would be invalid
            boolean handleSetOp = false;
            PlanNode accessNode = NodeEditor.findNodePreOrder(planNode, NodeConstants.Types.ACCESS, NodeConstants.Types.PROJECT);
            if (accessNode != null
                    && NodeEditor.findParent(projectNode, NodeConstants.Types.SET_OP, NodeConstants.Types.ACCESS) != null) {
                handleSetOp = true;
            }

            // Move the node
            if(placeConvertedSelectNode(critNode, virtualGroup, projectNode, childMap, metadata)) {
                if (handleSetOp) {
                    PlanNode newSelect = projectNode.getFirstChild();
                    Criteria crit = (Criteria) newSelect.getProperty(NodeConstants.Info.SELECT_CRITERIA);

                    if (createdNodes != null || (crit != QueryRewriter.UNKNOWN_CRITERIA && crit != QueryRewriter.FALSE_CRITERIA)) {
                        //remove if it's not going to cause the tree to prune
                        projectNode.replaceChild(newSelect, newSelect.getFirstChild());
                    }

                    Object modelID = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);

                    if(newSelect.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)
                            && context != null && CapabilitiesUtil.supportsInlineView(modelID, metadata, capFinder)
                            && CriteriaCapabilityValidatorVisitor.canPushLanguageObject(crit, modelID, metadata, capFinder, null) ) {

                        List<Expression> old = (List<Expression>) projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);

                        //create a project node based upon the created group and add it as the parent of the select
                        PlanNode project = RelationalPlanner.createProjectNode(LanguageObject.Util.deepClone(old, Expression.class));
                        accessNode.getFirstChild().addAsParent(project);

                        PlanNode newSourceNode = RuleDecomposeJoin.rebuild(new GroupSymbol("intermediate"), null, project.getFirstChild(), metadata, context, projectNode); //$NON-NLS-1$
                        newSourceNode.setProperty(NodeConstants.Info.INLINE_VIEW, true);

                        accessNode.addGroups(newSourceNode.getGroups());

                        accessNode.setProperty(Info.EST_COL_STATS, null);

                        childMap = SymbolMap.createSymbolMap(symbolMap.getKeys(), (List) project.getProperty(NodeConstants.Info.PROJECT_COLS));

                        if (!placeConvertedSelectNode(critNode, virtualGroup, project, childMap, metadata)) {
                            //sanity check, this should always be a valid placement
                            throw new AssertionError();
                        }
                    } else {
                        //TODO: see if the predicate should be duplicated for each branch under the access node
                        //or an inline view could be used similar to the above
                        if (createdNodes != null) {
                            createdNodes.remove(newSelect);
                        }
                        childMap = null;
                        continue;
                    }
                }
                movedCount++;
            }

            childMap = null; //create a new symbol map for the other children
        }

        //TODO - the logic here could be made more intelligent about EXCEPT and INTERSECT.
        if(movedCount == unionChildren.size()) {
            critNode.setProperty(NodeConstants.Info.IS_PHANTOM, Boolean.TRUE);
            return true;
        }
        //otherwise mark it as pushed so we don't consider it again
        critNode.setProperty(NodeConstants.Info.IS_PUSHED, Boolean.TRUE);
        //if any moved, then we need to continue
        return movedCount != 0;
    }

    static void collectUnionChildren(PlanNode unionNode, List<PlanNode> unionChildren) {
        for (PlanNode child : unionNode.getChildren()) {
            if(child.getType() == NodeConstants.Types.SET_OP) {
                collectUnionChildren(child, unionChildren);
            } else {
                unionChildren.add(child);
            }
        }
    }

    private boolean placeConvertedSelectNode(PlanNode critNode,
                                   GroupSymbol sourceGroup,
                                   PlanNode projectNode,
                                   SymbolMap symbolMap,
                                   QueryMetadataInterface metadata) throws QueryPlannerException {
        PlanNode copyNode = createConvertedSelectNode(critNode, sourceGroup,
                projectNode, symbolMap, metadata);
        if (copyNode == null) {
            return false;
        }

        PlanNode intermediateParent = NodeEditor.findParent(projectNode, NodeConstants.Types.ACCESS, NodeConstants.Types.SOURCE | NodeConstants.Types.SET_OP);
        if (intermediateParent != null) {
            intermediateParent.addAsParent(copyNode);
        } else {
            projectNode.getFirstChild().addAsParent(copyNode);
        }
        return true;
    }

    private PlanNode createConvertedSelectNode(PlanNode critNode,
            GroupSymbol sourceGroup, PlanNode projectNode, SymbolMap symbolMap,
            QueryMetadataInterface metadata) throws QueryPlannerException {
        // If projectNode has children, then it is from a SELECT without a FROM and the criteria should not be pushed
        if(projectNode.getChildCount() == 0) {
            return null;
        }

        Criteria crit = (Criteria) critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        Collection<ElementSymbol> cols = ElementCollectorVisitor.getElements(crit, true);

        if (projectNode.hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)) {
            //we can push iff the predicate is against partitioning columns in all projected window functions
            Set<WindowFunction> windowFunctions = RuleAssignOutputElements.getWindowFunctions((List<Expression>) projectNode.getProperty(Info.PROJECT_COLS));
            for (WindowFunction windowFunction : windowFunctions) {
                WindowSpecification spec = windowFunction.getWindowSpecification();
                if (spec.getPartition() == null) {
                    return null;
                }
                for (ElementSymbol col : cols) {
                    if (!spec.getPartition().contains(symbolMap.getMappedExpression(col))) {
                        return null;
                    }
                }
            }
        }


        Boolean conversionResult = checkConversion(symbolMap, cols);

        if (conversionResult == Boolean.FALSE) {
            return null; //not convertable
        }

        if (!critNode.getSubqueryContainers().isEmpty()
                && checkConversion(symbolMap, critNode.getCorrelatedReferenceElements()) != null) {
            return null; //not convertable, or has an aggregate for a correlated reference
        }

        PlanNode copyNode = copyNode(critNode);

        if (conversionResult == Boolean.TRUE) {
            copyNode.setProperty(NodeConstants.Info.IS_HAVING, Boolean.TRUE);
        }

        FrameUtil.convertNode(copyNode, sourceGroup, null, symbolMap.asMap(), metadata, true);

        //any proc relational criteria that is not input criteria should stay above the source
        if (sourceGroup.isProcedure() && !copyNode.getGroups().isEmpty()) {
            if (this.createdNodes != null) {
                this.createdNodes.remove(this.createdNodes.size() - 1);
            }
            return null;
        }

        if (critNode.hasBooleanProperty(Info.IS_DEPENDENT_SET)) {
            Criteria dep = (Criteria)copyNode.getProperty(Info.SELECT_CRITERIA);
            if (dep instanceof DependentSetCriteria) {
                DependentSetCriteria dsc = (DependentSetCriteria)dep;
                if (dsc.getExpression() instanceof Constant) {
                    return null; //corner case, the expression is just a constant
                                 //TODO: we should check if the dependent join can be reversed or undone
                }
            }
        }

        return copyNode;
    }

    private Boolean checkConversion(SymbolMap symbolMap,
            Collection<ElementSymbol> elements) {
        Boolean result = null;

        for (ElementSymbol element : elements) {
            Expression converted = symbolMap.getMappedExpression(element);

            if(converted == null) {
                return false;
            }

            Collection<SubqueryContainer<?>> scalarSubqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(converted);
            if (!scalarSubqueries.isEmpty()){
                return false;
            }

            if (!ElementCollectorVisitor.getAggregates(converted, false).isEmpty()) {
                result = Boolean.TRUE;
            }

        }
        return result;
    }

    public String toString() {
        return "PushSelectCriteria"; //$NON-NLS-1$
    }

}
