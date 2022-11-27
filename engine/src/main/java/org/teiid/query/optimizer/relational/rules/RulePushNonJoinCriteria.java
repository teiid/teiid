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
import java.util.Iterator;
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
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Pushes on criteria out of the on clause if possible.
 *
 * If the join no longer contains criteria, it will be changed into a cross join.
 *
 * Upon a successful push, RulePushSelectCriteria will be run again.
 */
public final class RulePushNonJoinCriteria implements OptimizerRule {

    private boolean firstRun = true;

    public RulePushNonJoinCriteria(boolean firstRun) {
        this.firstRun = firstRun;
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

        boolean treeChanged = false;
        boolean removeCopiedFlag = false;
        boolean pushRuleRaiseNull = false;

        for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN)) {
            List criteria = (List)node.getProperty(NodeConstants.Info.JOIN_CRITERIA);

            JoinType joinType = (JoinType)node.getProperty(NodeConstants.Info.JOIN_TYPE);

            //criteria cannot be pushed out of a full outer join clause
            if (joinType == JoinType.JOIN_FULL_OUTER || joinType == JoinType.JOIN_CROSS) {
                continue;
            }

            Iterator crits = criteria.iterator();
            while (crits.hasNext()) {
                Criteria crit = (Criteria)crits.next();

                //special case handling for true/false criteria
                if (crit.equals(QueryRewriter.FALSE_CRITERIA) || crit.equals(QueryRewriter.UNKNOWN_CRITERIA)) {
                    if (joinType == JoinType.JOIN_INNER) {
                        FrameUtil.replaceWithNullNode(node);
                    } else {
                        //must be a left or right outer join, replace the inner side with null
                        FrameUtil.replaceWithNullNode(JoinUtil.getInnerSideJoinNodes(node)[0]);
                        removeCopiedFlag = true;
                    }
                    //since a null node has been created, raise it to its highest point
                    pushRuleRaiseNull = true;
                    treeChanged = true;
                    break;
                } else if (crit.equals(QueryRewriter.TRUE_CRITERIA)) {
                    crits.remove();
                    break;
                }

                if (pushCriteria(node, crit, crits, metadata)) {
                    treeChanged = true;
                }
            }

            //degrade the join if there is no criteria left
            if (criteria.isEmpty() && joinType == JoinType.JOIN_INNER) {
                node.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
                treeChanged = true;
            }

            if (removeCopiedFlag) {
                //allow the criteria above the join to be eligible for pushing and copying
                PlanNode parent = node.getParent();
                while (parent != null && parent.getType() == NodeConstants.Types.SELECT) {
                    parent.setProperty(NodeConstants.Info.IS_COPIED, Boolean.FALSE);
                    parent = parent.getParent();
                }
            }
        }

        if (treeChanged) {
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }

        if (pushRuleRaiseNull) {
            rules.push(RuleConstants.RAISE_NULL);
        }

        return plan;
    }

    /**
     * True if the criteria is pushed.
     *
     * It's possible to push to the inner side of the join if the new criteria node
     * originates there
     *
     * @param joinNode
     * @param tgtCrit
     * @param metadata
     * @return
     */
    private boolean pushCriteria(PlanNode joinNode,
                                  Criteria tgtCrit, Iterator iter, QueryMetadataInterface metadata) {
        PlanNode newCritNode = RelationalPlanner.createSelectNode(tgtCrit, false);

        Set<GroupSymbol> groups = newCritNode.getGroups();

        PlanNode[] innerJoinNodes = JoinUtil.getInnerSideJoinNodes(joinNode);

        boolean pushed = false;

        for (int i = 0; i < innerJoinNodes.length; i++) {
            if (FrameUtil.findOriginatingNode(innerJoinNodes[i], groups) != null) {
                if (pushed) {
                    //create a new copy since the old one has been used
                    newCritNode = RelationalPlanner.createSelectNode(tgtCrit, false);
                }
                innerJoinNodes[i].addAsParent(newCritNode);
                pushed = true;
            }
        }

        if (pushed) {
            iter.remove();
        } else if (firstRun && tgtCrit instanceof CompareCriteria) {
            CompareCriteria crit = (CompareCriteria) tgtCrit;

            Expression leftExpr = crit.getLeftExpression();
            Expression rightExpr = crit.getRightExpression();

            for (int i = 0; i < innerJoinNodes.length; i++) {
                PlanNode node = FrameUtil.findJoinSourceNode(innerJoinNodes[i]);

                boolean outer = false;
                for (PlanNode child : NodeEditor.findAllNodes(node, NodeConstants.Types.JOIN)) {
                    if (((JoinType)child.getProperty(Info.JOIN_TYPE)).isOuter()) {
                        outer = true;
                        break;
                    }
                }

                if (!outer) {
                    continue;
                }

                Set<GroupSymbol> leftExprGroups = GroupsUsedByElementsVisitor.getGroups(leftExpr);
                Set<GroupSymbol> rightExprGroups = GroupsUsedByElementsVisitor.getGroups(rightExpr);

                ArrayList<ElementSymbol> notNull = new ArrayList<ElementSymbol>(2);

                if (node.getGroups().containsAll(leftExprGroups)) {
                    collectNotNull(leftExpr, notNull);
                } else if (node.getGroups().containsAll(rightExprGroups)) {
                    collectNotNull(rightExpr, notNull);
                }

                if (!notNull.isEmpty()) {
                    pushed = true;
                    for (ElementSymbol es : notNull) {
                        IsNullCriteria inc = new IsNullCriteria(es);
                        inc.setNegated(true);
                        PlanNode notNullCrit = RelationalPlanner.createSelectNode(inc, false);
                        notNullCrit.setProperty(NodeConstants.Info.IS_TEMPORARY, true);
                        innerJoinNodes[i].addAsParent(notNullCrit);
                    }
                }
            }
        }

        return pushed;
    }

    private void collectNotNull(Expression leftExpr,
            ArrayList<ElementSymbol> notNull) {
        if (leftExpr instanceof ElementSymbol) {
            notNull.add((ElementSymbol)leftExpr);
        } else if (leftExpr instanceof Function) {
            Function f = (Function)leftExpr;
            if (!f.getFunctionDescriptor().isNullDependent()) {
                for (Expression arg : f.getArgs()) {
                    collectNotNull(arg, notNull);
                }
            }
        }
    }

    public String toString() {
        return "PushNonJoinCriteria"; //$NON-NLS-1$
    }

}
