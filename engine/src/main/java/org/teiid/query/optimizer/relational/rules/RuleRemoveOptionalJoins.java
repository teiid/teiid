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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Removes optional join nodes if elements originating from that join are not used in the
 * top level project symbols.
 */
public class RuleRemoveOptionalJoins implements
                                    OptimizerRule {

    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {
        List<PlanNode> projectNodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.PROJECT);
        HashSet<PlanNode> skipNodes = new HashSet<PlanNode>();
        for (PlanNode projectNode : projectNodes) {
            if (projectNode.getChildCount() == 0 || projectNode.getProperty(NodeConstants.Info.INTO_GROUP) != null) {
                continue;
            }
            PlanNode groupNode = NodeEditor.findNodePreOrder(projectNode, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN);
            if (groupNode != null) {
                projectNode = groupNode;
            }
            Set<GroupSymbol> requiredForOptional = getRequiredGroupSymbols(projectNode.getFirstChild());
            boolean done = false;
            while (!done) {
                done = true;
                List<PlanNode> joinNodes = NodeEditor.findAllNodes(projectNode, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
                for (PlanNode planNode : joinNodes) {
                    if (skipNodes.contains(planNode)) {
                        continue;
                    }
                    if (!planNode.getExportedCorrelatedReferences().isEmpty()) {
                        skipNodes.add(planNode);
                        continue;
                    }
                    Set<GroupSymbol> required = getRequiredGroupSymbols(planNode);

                    List<PlanNode> removed = removeJoin(required, requiredForOptional, planNode, planNode.getFirstChild(), analysisRecord, metadata);
                    if (removed != null) {
                        skipNodes.addAll(removed);
                        done = false;
                        continue;
                    }
                    removed = removeJoin(required, requiredForOptional, planNode, planNode.getLastChild(), analysisRecord, metadata);
                    if (removed != null) {
                        skipNodes.addAll(removed);
                        done = false;
                    }
                }
            }
        }
        return plan;
    }

    private Set<GroupSymbol> getRequiredGroupSymbols(PlanNode planNode) {
        return GroupsUsedByElementsVisitor.getGroups((Collection<? extends LanguageObject>)planNode.getProperty(NodeConstants.Info.OUTPUT_COLS));
    }

    /**
     * remove the optional node if possible
     * @throws QueryPlannerException
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    private List<PlanNode> removeJoin(Set<GroupSymbol> required, Set<GroupSymbol> requiredForOptional, PlanNode joinNode, PlanNode optionalNode, AnalysisRecord record, QueryMetadataInterface metadata) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        boolean correctFrame = false;
        boolean isOptional = optionalNode.hasBooleanProperty(NodeConstants.Info.IS_OPTIONAL);
        if (isOptional) {
            required = requiredForOptional;
            correctFrame = true;
        }
        if (!Collections.disjoint(optionalNode.getGroups(), required)) {
            return null;
        }
        if (isOptional) {
            //prevent bridge table removal
            HashSet<GroupSymbol> joinGroups = new HashSet<GroupSymbol>();
            PlanNode parentNode = joinNode;
            while (parentNode.getType() != NodeConstants.Types.PROJECT) {
                PlanNode current = parentNode;
                parentNode = parentNode.getParent();
                if (current.getType() != NodeConstants.Types.SELECT && current.getType() != NodeConstants.Types.JOIN) {
                    continue;
                }
                Set<GroupSymbol> currentGroups = current.getGroups();
                if (current.getType() == NodeConstants.Types.JOIN) {
                    currentGroups = GroupsUsedByElementsVisitor.getGroups((List<Criteria>)current.getProperty(NodeConstants.Info.JOIN_CRITERIA));
                }
                if (!Collections.disjoint(currentGroups, optionalNode.getGroups()) && !optionalNode.getGroups().containsAll(currentGroups)) {
                    //we're performing a join
                    boolean wasEmpty = joinGroups.isEmpty();
                    boolean modified = joinGroups.addAll(current.getGroups());
                    if (!wasEmpty && modified) {
                        return null;
                    }
                }
            }
        }
        JoinType jt = (JoinType)joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);

        boolean usesKey = false;
        boolean isRight = optionalNode == joinNode.getLastChild();

        if (!isOptional && (jt == JoinType.JOIN_INNER || (jt == JoinType.JOIN_LEFT_OUTER && isRight))) {
            usesKey = isOptionalUsingKey(joinNode, optionalNode, metadata, isRight, jt == JoinType.JOIN_INNER);
        }

        if (!isOptional && !usesKey &&
                (jt != JoinType.JOIN_LEFT_OUTER || !isRight || useNonDistinctRows(joinNode.getParent()))) {
            return null;
        }
        // remove the parent node and move the sibling node upward
        PlanNode parentNode = joinNode.getParent();
        joinNode.removeChild(optionalNode);
        joinNode.getFirstChild().setProperty(NodeConstants.Info.OUTPUT_COLS, joinNode.getProperty(NodeConstants.Info.OUTPUT_COLS));
        NodeEditor.removeChildNode(parentNode, joinNode);
        joinNode.recordDebugAnnotation((isOptional?"node was marked as optional ":"node will not affect the results"), null, "Removing join node", record, null); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        while (parentNode.getType() != NodeConstants.Types.PROJECT) {
            PlanNode current = parentNode;
            parentNode = parentNode.getParent();
            if (correctFrame) {
                if (current.getType() == NodeConstants.Types.SELECT) {
                    if (!Collections.disjoint(current.getGroups(), optionalNode.getGroups())) {
                        current.getFirstChild().setProperty(NodeConstants.Info.OUTPUT_COLS, current.getProperty(NodeConstants.Info.OUTPUT_COLS));
                        NodeEditor.removeChildNode(parentNode, current);
                    }
                } else if (current.getType() == NodeConstants.Types.JOIN) {
                    if (!Collections.disjoint(current.getGroups(), optionalNode.getGroups())) {
                        List<Criteria> crits = (List<Criteria>) current.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                        if (crits != null && !crits.isEmpty()) {
                            for (Iterator<Criteria> iterator = crits.iterator(); iterator.hasNext();) {
                                Criteria criteria = iterator.next();
                                if (!Collections.disjoint(GroupsUsedByElementsVisitor.getGroups(criteria), optionalNode.getGroups())) {
                                    iterator.remove();
                                }
                            }
                            if (crits.isEmpty()) {
                                JoinType joinType = (JoinType) current.getProperty(NodeConstants.Info.JOIN_TYPE);
                                if (joinType == JoinType.JOIN_INNER) {
                                    current.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
                                }
                            }
                        }
                    }
                }
            } else if (current.getType() != NodeConstants.Types.JOIN) {
                break;
            }
            if (current.getType() == NodeConstants.Types.JOIN) {
                current.getGroups().removeAll(optionalNode.getGroups());
            }
        }

        return NodeEditor.findAllNodes(optionalNode, NodeConstants.Types.JOIN);
    }

    private boolean isOptionalUsingKey(PlanNode joinNode,
            PlanNode optionalNode, QueryMetadataInterface metadata, boolean isRight, boolean inner) throws
            TeiidComponentException, QueryMetadataException {
        //TODO: check if key preserved to allow for more than just a single group as optional
        //for now we just look for a single group
        if (optionalNode.getGroups().size() != 1) {
            return false;
        }
        PlanNode left = isRight?joinNode.getFirstChild():joinNode.getLastChild();
        LinkedList<Expression> leftExpressions = new LinkedList<Expression>();
        LinkedList<Expression> rightExpressions = new LinkedList<Expression>();
        LinkedList<Criteria> nonEquiJoinCriteria = new LinkedList<Criteria>();
        RuleChooseJoinStrategy.separateCriteria(left.getGroups(), optionalNode.getGroups(), leftExpressions, rightExpressions, (List<Criteria>)joinNode.getProperty(NodeConstants.Info.JOIN_CRITERIA), nonEquiJoinCriteria);
        if (!nonEquiJoinCriteria.isEmpty()) {
            for (Criteria crit : nonEquiJoinCriteria) {
                if (!Collections.disjoint(GroupsUsedByElementsVisitor.getGroups(crit), optionalNode.getGroups())) {
                    return false; //additional predicates still need to be applied, or ignored via hint
                }
            }
        }
        ArrayList<Object> leftIds = new ArrayList<Object>(leftExpressions.size());
        ArrayList<Object> rightIds = new ArrayList<Object>(rightExpressions.size());
        List<ElementSymbol> nullableToFilter = new ArrayList<>(1);
        for (Expression expr : leftExpressions) {
            if (expr instanceof ElementSymbol) {
                ElementSymbol col = (ElementSymbol) expr;
                Object id = col.getMetadataID();
                leftIds.add(id);
                if (inner) {
                    //TODO: could deeply check whether the value can be null
                    //the most complex case being marked in the metadata as non-null,
                    //but having a nested outer join with this column coming
                    //from the inner side
                    nullableToFilter.add(col);
                }
            }
        }
        for (Expression expr : rightExpressions) {
            if (expr instanceof ElementSymbol) {
                rightIds.add(((ElementSymbol) expr).getMetadataID());
            } else {
                return false; //only allow a key join
            }
        }

        outer: for (GroupSymbol group : left.getGroups()) {
            Collection fks = metadata.getForeignKeysInGroup(group.getMetadataID());
            for (Object fk : fks) {
                List fkColumns = metadata.getElementIDsInKey(fk);
                if (!leftIds.containsAll(fkColumns)) {
                    continue;
                }
                Object pk = metadata.getPrimaryKeyIDForForeignKeyID(fk);
                List pkColumns = metadata.getElementIDsInKey(pk);
                if ((rightIds.size() != pkColumns.size()) || !rightIds.containsAll(pkColumns)) {
                    continue;
                }
                for (ElementSymbol e : nullableToFilter) {
                    IsNullCriteria inc = new IsNullCriteria(e);
                    inc.setNegated(true);
                    joinNode.addAsParent(RelationalPlanner.createSelectNode(inc, false));
                }
                if (!isRight) {
                    //match up to the replacement logic below
                    JoinUtil.swapJoinChildren(joinNode);
                }
                return true;
            }
        }
        return false;
    }

    static boolean useNonDistinctRows(PlanNode parent) {
        while (parent != null) {
            if (parent.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL)) {
                return false;
            }
            switch (parent.getType()) {
                case NodeConstants.Types.DUP_REMOVE: {
                    return false;
                }
                case NodeConstants.Types.SET_OP: {
                    if (!parent.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
                        return false;
                    }
                    break;
                }
                case NodeConstants.Types.GROUP: {
                    Set<AggregateSymbol> aggs = RulePushAggregates.collectAggregates(parent);
                    return AggregateSymbol.areAggregatesCardinalityDependent(aggs);
                }
                case NodeConstants.Types.TUPLE_LIMIT: {
                    if (FrameUtil.isOrderedOrStrictLimit(parent)) {
                        return true;
                    }
                    break;
                }
                case NodeConstants.Types.PROJECT: {
                    List<Expression> projectCols = (List<Expression>)parent.getProperty(NodeConstants.Info.PROJECT_COLS);
                    for (Expression ex : projectCols) {
                        if (FunctionCollectorVisitor.isNonDeterministic(ex) || !ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(ex).isEmpty()) {
                            return true;
                        }
                    }
                    break;
                }
            }
            parent = parent.getParent();
        }
        return true;
    }

    public String toString() {
        return "RemoveOptionalJoins"; //$NON-NLS-1$
    }

}
