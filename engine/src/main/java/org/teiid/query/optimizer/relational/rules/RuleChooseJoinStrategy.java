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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Marks join as a candidate merge join if conditions are met
 */
public class RuleChooseJoinStrategy implements OptimizerRule {

    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryMetadataException, TeiidComponentException {

        for (PlanNode joinNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS)) {
            chooseJoinStrategy(joinNode, metadata);
        }

        return plan;
    }

    /**
     * Determines whether this node should be converted to a merge join node
     * @param joinNode The join node
     * @param metadata The metadata
     */
    static void chooseJoinStrategy(PlanNode joinNode, QueryMetadataInterface metadata) {
        // Check that join is an inner join
        JoinType jtype = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
        if(jtype.equals(JoinType.JOIN_CROSS)) {
            return;
        }

        PlanNode leftChild = joinNode.getFirstChild();
        leftChild = FrameUtil.findJoinSourceNode(leftChild);
        if (leftChild == null) {
            return;
        }
        PlanNode rightChild = joinNode.getLastChild();
        rightChild = FrameUtil.findJoinSourceNode(rightChild);
        if (rightChild == null) {
            return;
        }

        Collection<GroupSymbol> leftGroups = leftChild.getGroups();
        Collection<GroupSymbol> rightGroups = rightChild.getGroups();

        List<Expression> leftExpressions = new ArrayList<Expression>();
        List<Expression> rightExpressions = new ArrayList<Expression>();

        // Check that join criteria are all equality criteria and that there are elements from
        // no more than one group on each side
        List<Criteria> crits = (List<Criteria>) joinNode.getProperty(NodeConstants.Info.JOIN_CRITERIA);

        filterOptionalCriteria(crits, true);

        if (crits.isEmpty() && jtype == JoinType.JOIN_INNER) {
            joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
            return;
        }

        List<Criteria> nonEquiJoinCriteria = new ArrayList<Criteria>();

        separateCriteria(leftGroups, rightGroups, leftExpressions, rightExpressions, crits, nonEquiJoinCriteria);
        if (!leftExpressions.isEmpty()) {
            joinNode.setProperty(NodeConstants.Info.LEFT_EXPRESSIONS, createExpressionSymbols(leftExpressions));
            joinNode.setProperty(NodeConstants.Info.RIGHT_EXPRESSIONS, createExpressionSymbols(rightExpressions));

            //make use of the one side criteria
            joinNode.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.MERGE);
            joinNode.setProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA, nonEquiJoinCriteria);
        } else if (nonEquiJoinCriteria.isEmpty()) {
            joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, nonEquiJoinCriteria);
            if (joinNode.getProperty(NodeConstants.Info.JOIN_TYPE) == JoinType.JOIN_INNER) {
                joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
            }
        }
    }

    public static void separateCriteria(Collection<GroupSymbol> leftGroups,
            Collection<GroupSymbol> rightGroups,
            List<Expression> leftExpressions,
            List<Expression> rightExpressions, List<Criteria> crits,
            Collection<Criteria> nonEquiJoinCriteria) {
        for (Criteria theCrit : crits) {
            Set<GroupSymbol> critGroups =GroupsUsedByElementsVisitor.getGroups(theCrit);

            if (leftGroups.containsAll(critGroups) || rightGroups.containsAll(critGroups)) {
                nonEquiJoinCriteria.add(theCrit);
                continue;
            }

            if(! (theCrit instanceof CompareCriteria)) {
                nonEquiJoinCriteria.add(theCrit);
                continue;
            }

            CompareCriteria crit = (CompareCriteria) theCrit;
            if(crit.getOperator() != CompareCriteria.EQ) {
                nonEquiJoinCriteria.add(theCrit);
                continue;
            }

            Expression leftExpr = crit.getLeftExpression();
            Expression rightExpr = crit.getRightExpression();

            Set<GroupSymbol> leftExprGroups = GroupsUsedByElementsVisitor.getGroups(leftExpr);
            Set<GroupSymbol> rightExprGroups = GroupsUsedByElementsVisitor.getGroups(rightExpr);

            if (leftGroups.isEmpty() || rightGroups.isEmpty()) {
                nonEquiJoinCriteria.add(theCrit);
            }else if(leftGroups.containsAll(leftExprGroups) && rightGroups.containsAll(rightExprGroups)) {
                leftExpressions.add(leftExpr);
                rightExpressions.add(rightExpr);
            } else if (rightGroups.containsAll(leftExprGroups) && leftGroups.containsAll(rightExprGroups)) {
                leftExpressions.add(rightExpr);
                rightExpressions.add(leftExpr);
            } else {
                nonEquiJoinCriteria.add(theCrit);
            }
        }
    }

    public static List<Expression> createExpressionSymbols(List<? extends Expression> expressions) {
        List<Expression> result = new ArrayList<Expression>();
        for (Expression expression : expressions) {
            if (expression instanceof Symbol) {
                result.add(expression);
                continue;
            }
            result.add(new ExpressionSymbol("expr", expression)); //$NON-NLS-1$
        }
        return result;
    }

    static void filterOptionalCriteria(List<Criteria> crits, boolean all) {
        for (Iterator<Criteria> iter = crits.iterator(); iter.hasNext();) {
            Criteria crit = iter.next();
            if (crit instanceof CompareCriteria) {
                CompareCriteria cc = (CompareCriteria) crit;
                if (Boolean.TRUE.equals(cc.getIsOptional()) || (all && cc.isOptional())) {
                    iter.remove();
                }
            }
        }
    }

    public String toString() {
        return "ChooseJoinStrategy"; //$NON-NLS-1$
    }

}