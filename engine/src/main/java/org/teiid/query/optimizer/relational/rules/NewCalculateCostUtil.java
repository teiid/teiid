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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Like.MatchMode;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;

/**
 * @since 4.3
 */
public class NewCalculateCostUtil {

    public static final int UNKNOWN_JOIN_SCALING = 20;
    public static final float UNKNOWN_VALUE = -1;

    // the following variables are used to hold cost estimates (roughly in milliseconds)
    private final static float compareTime = .0001f; //TODO: a better estimate would be based upon the number of conjuncts
    private final static float readTime = .001f; //TODO: should come from the connector
    private final static float procNewRequestTime = 1; //TODO: should come from the connector

    enum Stat {
        NDV,
        NDV_HIGH,
        NNV
    }

    public static class DependentCostAnalysis {
        Float[] maxNdv;
        Float[] expectedNdv;
        Float expectedCardinality;
    }

    @SuppressWarnings("serial")
    static class ColStats extends LinkedHashMap<Expression, float[]> {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('{');

            int j = 0;
            for (Iterator<Entry<Expression, float[]>> i = this.entrySet().iterator(); i.hasNext();) {
                Entry<Expression, float[]> e = i.next();
                sb.append(e.getKey());
                sb.append('=');
                sb.append(Arrays.toString(e.getValue()));
                j++;
                if (i.hasNext()) {
                    sb.append(", "); //$NON-NLS-1$
                    if (j > 3) {
                        sb.append("..."); //$NON-NLS-1$
                        break;
                    }
                }
            }
            return sb.append('}').toString();
        }

        public float[] put(Expression key, float[] value) {
            return super.put(SymbolMap.getExpression(key), value);
        }

        public float[] get(Expression key) {
            return super.get(SymbolMap.getExpression(key));
        }
    }

    /**
     * Calculate cost of a node and all children, recursively from the bottom up.
     * @param node
     * @param metadata
     * @return Cost computed at the passed node
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    static float computeCostForTree(PlanNode node, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        updateCardinality(node, metadata);

        return node.getCardinality();
    }

    static boolean updateCardinality(PlanNode node, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        Float cost = (Float) node.getProperty(NodeConstants.Info.EST_CARDINALITY);

        // check if already computed
        boolean updated = false;
        for (PlanNode child : node.getChildren()) {
            updated |= updateCardinality(child, metadata);
        }
        if(cost == null || updated) {
            computeNodeCost(node, metadata);
            return true;
        }
        return false;
    }

    /**
     * This method attempts to estimate a cost for each type of node.
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private static void computeNodeCost(PlanNode node, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        switch(node.getType()) {
            case NodeConstants.Types.SOURCE:
                estimateSourceNodeCost(node, metadata);
                break;

            case NodeConstants.Types.SELECT:
                estimateSelectNodeCost(node, metadata);
                break;

            case NodeConstants.Types.JOIN:
                estimateJoinNodeCost(node, metadata);
                break;

            case NodeConstants.Types.DUP_REMOVE:
                estimateNodeCost(node, FrameUtil.findTopCols(node), metadata);
                break;

            case NodeConstants.Types.GROUP:
                if (!node.hasCollectionProperty(NodeConstants.Info.GROUP_COLS)) {
                    setCardinalityEstimate(node, 1f, true, metadata);
                } else {
                    estimateNodeCost(node, (List)node.getProperty(NodeConstants.Info.GROUP_COLS), metadata);
                }
                break;
            case NodeConstants.Types.ACCESS:
            case NodeConstants.Types.SORT:
            {
                //Simply record the cost of the only child
                PlanNode child = node.getFirstChild();
                Float childCost = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
                setCardinalityEstimate(node, childCost, true, metadata);
                break;
            }
            case NodeConstants.Types.NULL:
                setCardinalityEstimate(node, 0f, true, metadata);
                break;

            case NodeConstants.Types.PROJECT:
            {
                Float childCost = null;
                //Simply record the cost of the only child
                if (node.getChildCount() != 0) {
                    PlanNode child = node.getFirstChild();
                    childCost = (Float)child.getProperty(NodeConstants.Info.EST_CARDINALITY);
                } else {
                    childCost = 1f;
                }
                setCardinalityEstimate(node, childCost, true, metadata);
                break;
            }
            case NodeConstants.Types.SET_OP:
            {
                estimateSetOpCost(node, metadata);
                break;
            }
            case NodeConstants.Types.TUPLE_LIMIT:
            {
                PlanNode child = node.getFirstChild();
                float childCost = child.getCardinality();

                Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
                Float cost = childCost;

                if (childCost != UNKNOWN_VALUE && offset instanceof Constant) {
                    float offsetCost = childCost - ((Number)((Constant)offset).getValue()).floatValue();
                    cost = new Float((offsetCost < 0) ? 0 : offsetCost);
                }

                Expression limit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
                if (limit instanceof Constant) {
                    float limitCost = ((Number)((Constant)limit).getValue()).floatValue();
                    if (cost.floatValue() != UNKNOWN_VALUE) {
                        cost = new Float(Math.min(limitCost, cost.floatValue()));
                    } else {
                        cost = new Float(limitCost);
                    }
                }
                setCardinalityEstimate(node, cost, true, metadata);
                break;
            }
        }
    }

    private static void estimateSetOpCost(PlanNode node,
            QueryMetadataInterface metadata) throws QueryMetadataException,
            TeiidComponentException {
        float cost = 0;

        SetQuery.Operation op = (SetQuery.Operation)node.getProperty(NodeConstants.Info.SET_OPERATION);

        float leftCost = (Float)node.getFirstChild().getProperty(NodeConstants.Info.EST_CARDINALITY);
        float rightCost = (Float)node.getLastChild().getProperty(NodeConstants.Info.EST_CARDINALITY);

        if (!node.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
            leftCost = getDistinctEstimate(node.getFirstChild(), metadata, leftCost);
            rightCost = getDistinctEstimate(node.getLastChild(), metadata, rightCost);
        }

        cost = getCombinedSetEstimate(op, leftCost, rightCost, !node.hasBooleanProperty(NodeConstants.Info.USE_ALL));

        setCardinalityEstimate(node, new Float(cost), true, metadata);
    }

    private static float getCombinedSetEstimate(SetQuery.Operation op, float leftCost, float rightCost, boolean distinct) {
        float cost;
        cost = leftCost;

        switch (op) {
        case EXCEPT:
            if (leftCost != UNKNOWN_VALUE && rightCost != UNKNOWN_VALUE) {
                cost = Math.max(1, leftCost - .5f * rightCost);
            }
            break;
        case INTERSECT:
            if (rightCost != UNKNOWN_VALUE) {
                if (leftCost != UNKNOWN_VALUE) {
                    cost = .5f * Math.min(leftCost, rightCost);
                } else {
                    cost = rightCost;
                }
            }
            break;
        default: //union
            if (leftCost != UNKNOWN_VALUE && rightCost != UNKNOWN_VALUE) {
                if (distinct) {
                    cost = Math.max(leftCost, rightCost) + .5f * Math.min(leftCost, rightCost);
                } else {
                    cost = rightCost + leftCost;
                }
            }
            break;
        }
        return cost;
    }

    private static float getDistinctEstimate(PlanNode node,
            QueryMetadataInterface metadata, float cost)
            throws QueryMetadataException, TeiidComponentException {
        PlanNode projectNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.PROJECT);
        float result = cost;
        if (projectNode != null) {
            result = getNDVEstimate(node.getParent(), metadata, cost, (List)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS), false);
            if (result == UNKNOWN_VALUE) {
                if (cost == UNKNOWN_VALUE) {
                    return UNKNOWN_VALUE;
                }
                return cost/2;
            }
        }
        return result;
    }
    private static void setCardinalityEstimate(PlanNode node, Float bestEstimate, boolean setColEstimates, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        setCardinalityEstimate(node, bestEstimate, setColEstimates, metadata, 1, 1);
    }

    private static void setCardinalityEstimate(PlanNode node, Float bestEstimate, boolean setColEstimates, QueryMetadataInterface metadata, float leftPercent, float rightPercent) throws QueryMetadataException, TeiidComponentException {
        if (bestEstimate == null){
            bestEstimate = Float.valueOf(UNKNOWN_VALUE);
        }
        Float lastEstimate = (Float)node.setProperty(NodeConstants.Info.EST_CARDINALITY, bestEstimate);
        if (node.getParent() != null && (lastEstimate == null || !lastEstimate.equals(bestEstimate))) {
            node.getParent().setProperty(Info.EST_CARDINALITY, null);
        }
        if (setColEstimates) {
            setColStatEstimates(node, bestEstimate, metadata, leftPercent, rightPercent);
            ColStats stats = (ColStats) node.getProperty(Info.EST_COL_STATS);
            if (stats != null && node.getType() == NodeConstants.Types.SELECT) {
                Criteria predicateCriteria = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);

                if(predicateCriteria instanceof CompareCriteria) {
                    CompareCriteria compCrit = (CompareCriteria) predicateCriteria;


                    Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(compCrit.getLeftExpression(), true);
                    if (elements.size() == 1 && EvaluatableVisitor.willBecomeConstant(compCrit.getRightExpression())) {
                        float[] val = stats.get(elements.iterator().next());
                        if (val != null) {
                            val[Stat.NNV.ordinal()] = 0;
                            switch (compCrit.getOperator()) {
                            case CompareCriteria.EQ:
                                val[Stat.NDV.ordinal()] = 1;
                                val[Stat.NDV_HIGH.ordinal()] = 1;
                                break;
                            }
                        }
                    }
                } else if(predicateCriteria instanceof SetCriteria) {
                    SetCriteria setCriteria = (SetCriteria) predicateCriteria;

                    Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(setCriteria.getExpression(), true);
                    if (elements.size() == 1 && !setCriteria.isNegated()) {
                        float[] val = stats.get(elements.iterator().next());
                        if (val != null) {
                            val[Stat.NNV.ordinal()] = 0;
                            val[Stat.NDV.ordinal()] = (val[Stat.NDV.ordinal()] == UNKNOWN_VALUE?setCriteria.getNumberOfValues():Math.min(setCriteria.getNumberOfValues(), val[Stat.NDV.ordinal()]));
                            val[Stat.NDV_HIGH.ordinal()] = (val[Stat.NDV_HIGH.ordinal()] == UNKNOWN_VALUE?setCriteria.getNumberOfValues():Math.min(setCriteria.getNumberOfValues(), val[Stat.NDV_HIGH.ordinal()]));
                        }
                    }
                } else if(predicateCriteria instanceof IsNullCriteria) {
                    IsNullCriteria isNullCriteria = (IsNullCriteria)predicateCriteria;

                    Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(isNullCriteria.getExpression(), true);
                    if (elements.size() == 1 && isNullCriteria.isNegated()) {
                        float[] val = stats.get(elements.iterator().next());
                        if (val != null) {
                            val[Stat.NDV.ordinal()] = 0;
                            val[Stat.NDV_HIGH.ordinal()] = 0;
                        }
                    }
                }
            }
        }
    }

    /**
     * Method estimateJoinNodeCost.
     * @param node
     * @param metadata
     */
    private static void estimateJoinNodeCost(PlanNode node, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        Iterator<PlanNode> children = node.getChildren().iterator();
        PlanNode child1 = children.next();
        float childCost1 = child1.getCardinality();
        PlanNode child2 = children.next();
        float childCost2 = child2.getCardinality();

        if (childCost1 == UNKNOWN_VALUE || childCost2 == UNKNOWN_VALUE) {
            setCardinalityEstimate(node, null, true, metadata);
            return;
        }

        JoinType joinType = (JoinType)node.getProperty(NodeConstants.Info.JOIN_TYPE);
        List joinCriteria = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);

        float baseCost = childCost1 * childCost2;
        float leftPercent = 1;
        float rightPercent = 1;
        if (joinCriteria != null && !joinCriteria.isEmpty()) {
            List<Expression> leftExpressions = null;
            List<Expression> rightExpressions = null;
            List<Criteria> nonEquiJoinCriteria = null;
            if (!node.hasCollectionProperty(NodeConstants.Info.LEFT_EXPRESSIONS)) {
                Collection<GroupSymbol> leftGroups = child1.getGroups();
                Collection<GroupSymbol> rightGroups = child2.getGroups();

                leftExpressions = new ArrayList<Expression>();
                rightExpressions = new ArrayList<Expression>();
                nonEquiJoinCriteria = new ArrayList<Criteria>();

                RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, leftExpressions, rightExpressions, joinCriteria, nonEquiJoinCriteria);
            } else {
                leftExpressions = (List<Expression>) node.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS);
                rightExpressions = (List<Expression>) node.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS);
                nonEquiJoinCriteria = (List<Criteria>) node.getProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA);
            }

            float leftNdv = getNDVEstimate(child1, metadata, childCost1, leftExpressions, false);
            float rightNdv = getNDVEstimate(child2, metadata, childCost2, rightExpressions, false);

            float leftNdv1 = getNDVEstimate(child1, metadata, childCost1, leftExpressions, true);
            float rightNdv1 = getNDVEstimate(child2, metadata, childCost2, rightExpressions, true);

            if (leftNdv == UNKNOWN_VALUE) {
                leftNdv = leftNdv1;
            }
            if (rightNdv == UNKNOWN_VALUE) {
                rightNdv = rightNdv1;
            }

            if (leftNdv != UNKNOWN_VALUE && rightNdv != UNKNOWN_VALUE) {
                //Compensate for estimates by assuming a 1-many relationship
                if (leftNdv1/leftNdv > 2*rightNdv1/rightNdv && leftNdv > rightNdv) {
                    rightNdv = rightNdv1;
                } else {
                    leftNdv = leftNdv1;
                }
                baseCost = (childCost1 / leftNdv) * (childCost2 / rightNdv) * Math.min(leftNdv, rightNdv);
                leftPercent = Math.min(leftNdv, rightNdv) / leftNdv;
                rightPercent = Math.min(leftNdv, rightNdv) / rightNdv;
            } else {
                nonEquiJoinCriteria = joinCriteria;
            }

            if (nonEquiJoinCriteria != null && !nonEquiJoinCriteria.isEmpty()) {
                Criteria crit = Criteria.combineCriteria(nonEquiJoinCriteria);
                //TODO: we may be able to get a fairly accurate join estimate if the
                //unknown side is being joined with a key
                baseCost = recursiveEstimateCostOfCriteria(baseCost, node, crit, metadata);
            }
        }

        Float cost = null;
        if (JoinType.JOIN_CROSS.equals(joinType) || JoinType.JOIN_INNER.equals(joinType)){
            cost = baseCost;
        } else if (JoinType.JOIN_FULL_OUTER.equals(joinType)) {
            cost = Math.max((childCost1+childCost2),baseCost);
        } else if (JoinType.JOIN_LEFT_OUTER.equals(joinType)) {
            cost = Math.max(childCost1,baseCost);
        } else if (JoinType.JOIN_SEMI.equals(joinType) || JoinType.JOIN_ANTI_SEMI.equals(joinType)) {
            cost = Math.min(childCost1, baseCost);
        }

        setCardinalityEstimate(node, cost, true, metadata, leftPercent, rightPercent);
    }

    /**
     * Estimate the cost of a selection.  This is not easy to do without information
     * about the value count for each relation attribute.
     * @param metadata
     */
    private static void estimateSelectNodeCost(PlanNode node, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        PlanNode child = node.getFirstChild();
        float childCost = child.getCardinality();

        //Get list of conjuncts
        Criteria selectCriteria = (Criteria)node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        float newCost = recursiveEstimateCostOfCriteria(childCost, node, selectCriteria, metadata);
        setCardinalityEstimate(node, newCost, true, metadata);
    }

    private static void setColStatEstimates(PlanNode node, float cardinality, QueryMetadataInterface metadata, float leftPercent, float rightPercent) throws QueryMetadataException, TeiidComponentException {
        ColStats colStats = null;
        ColStats colStatsOther = null;
        float childCardinality = UNKNOWN_VALUE;
        if (node.getChildCount() > 0) {
            childCardinality = node.getFirstChild().getCardinality();
            colStats = (ColStats) node.getFirstChild().getProperty(Info.EST_COL_STATS);
        }
        float otherChildCardinality = UNKNOWN_VALUE;
        List<? extends Expression> outputColsOther = null;
        if (node.getChildCount() > 1) {
            otherChildCardinality = node.getLastChild().getCardinality();
            colStatsOther = (ColStats) node.getLastChild().getProperty(Info.EST_COL_STATS);
            outputColsOther = getOutputCols(node.getLastChild(), metadata);
        }
        SetQuery.Operation setOp = (Operation) node.getProperty(Info.SET_OPERATION);
        List<? extends Expression> outputCols = getOutputCols(node, metadata);
        ColStats newColStats = new ColStats();
        for (int i = 0; i < outputCols.size(); i++) {
            Expression expr = outputCols.get(i);
            float[] newStats = new float[3];
            Arrays.fill(newStats, UNKNOWN_VALUE);
            Expression rawExpr = SymbolMap.getExpression(expr);
            if (rawExpr instanceof Function) {
                Function function = (Function)rawExpr;
                if (function.getArgs().length == 0 && function.getFunctionDescriptor().getDeterministic() != Determinism.NONDETERMINISTIC) {
                    newStats[Stat.NDV.ordinal()] = 1;
                    newStats[Stat.NDV_HIGH.ordinal()] = 1;
                    newStats[Stat.NNV.ordinal()] = 0;
                }
            } else if (rawExpr instanceof Constant || rawExpr instanceof Reference) {
                newStats[Stat.NDV.ordinal()] = 1;
                newStats[Stat.NDV_HIGH.ordinal()] = 1;
                newStats[Stat.NNV.ordinal()] = 0;
                if (rawExpr instanceof Constant) {
                    if (((Constant)rawExpr).getValue() == null) {
                        newStats[Stat.NDV.ordinal()] = 0;
                        newStats[Stat.NDV_HIGH.ordinal()] = 0;
                        newStats[Stat.NNV.ordinal()] = 1;
                    }
                }
                newColStats.put(expr, newStats);
                continue;
            }
            if (childCardinality == UNKNOWN_VALUE && ((setOp == null && colStats == null) || (setOp != null && (colStats == null || colStatsOther == null)))) {
                //base case - cannot determine, just assume unique rows
                newStats[Stat.NDV.ordinal()] = cardinality;
                newStats[Stat.NDV_HIGH.ordinal()] = cardinality;
                newStats[Stat.NNV.ordinal()] = 0;
            } else if (setOp != null) {
                //set op
                float[] stats = colStats.get(expr);
                float[] statsOther = colStatsOther.get(outputColsOther.get(i));
                newStats[Stat.NDV.ordinal()] = Math.min(cardinality==UNKNOWN_VALUE?Float.MAX_VALUE:cardinality, getCombinedSetEstimate(setOp, stats[Stat.NDV.ordinal()], statsOther[Stat.NDV.ordinal()], true));
                newStats[Stat.NDV_HIGH.ordinal()] = Math.min(cardinality==UNKNOWN_VALUE?Float.MAX_VALUE:cardinality, getCombinedSetEstimate(setOp, stats[Stat.NDV_HIGH.ordinal()], statsOther[Stat.NDV_HIGH.ordinal()], true));
                newStats[Stat.NNV.ordinal()] = Math.min(cardinality==UNKNOWN_VALUE?Float.MAX_VALUE:cardinality, getCombinedSetEstimate(setOp, stats[Stat.NNV.ordinal()], statsOther[Stat.NNV.ordinal()], !node.hasBooleanProperty(NodeConstants.Info.USE_ALL)));
            } else {
                //all other cases - join is the only multi-node case here
                float[] stats = null;
                float origCardinality = childCardinality;
                boolean left = true;
                if (colStats != null) {
                    stats = colStats.get(expr);
                }
                if (stats == null && colStatsOther != null) {
                    origCardinality = otherChildCardinality;
                    stats = colStatsOther.get(expr);
                    left = false;
                }
                origCardinality = Math.max(1, origCardinality);
                if (stats == null) {
                    if (node.getType() == NodeConstants.Types.PROJECT) {
                        Collection<Expression> elems = new HashSet<Expression>();
                        ElementCollectorVisitor.getElements(expr, elems);
                        newStats[Stat.NDV.ordinal()] = getStat(Stat.NDV, elems, node, childCardinality, metadata);
                        newStats[Stat.NDV_HIGH.ordinal()] = getStat(Stat.NDV_HIGH, elems, node, childCardinality, metadata);
                        newStats[Stat.NNV.ordinal()] = getStat(Stat.NNV, elems, node, childCardinality, metadata);
                    } else {
                        //TODO: use a better estimate for new aggs
                        if (node.hasProperty(Info.GROUP_COLS) && cardinality != UNKNOWN_VALUE) {
                            newStats[Stat.NDV.ordinal()] = cardinality / 3;
                            newStats[Stat.NDV_HIGH.ordinal()] = cardinality / 3;
                        } else {
                            newStats[Stat.NDV.ordinal()] = cardinality;
                            newStats[Stat.NDV_HIGH.ordinal()] = cardinality;
                        }
                        newStats[Stat.NNV.ordinal()] = UNKNOWN_VALUE;
                    }
                } else {
                    if (node.getType() == NodeConstants.Types.DUP_REMOVE || node.getType() == NodeConstants.Types.GROUP || node.getType() == NodeConstants.Types.PROJECT || node.getType() == NodeConstants.Types.ACCESS) {
                        //don't scale down
                        newStats[Stat.NDV.ordinal()] = Math.min(cardinality==UNKNOWN_VALUE?Float.MAX_VALUE:cardinality, stats[Stat.NDV.ordinal()]);
                        newStats[Stat.NDV_HIGH.ordinal()] = Math.min(cardinality==UNKNOWN_VALUE?Float.MAX_VALUE:cardinality, stats[Stat.NDV_HIGH.ordinal()]);
                    } else if (stats[Stat.NDV.ordinal()] != UNKNOWN_VALUE && cardinality!=UNKNOWN_VALUE) {
                        if (stats[Stat.NDV.ordinal()] == stats[Stat.NDV_HIGH.ordinal()]) {
                            newStats[Stat.NDV.ordinal()] = stats[Stat.NDV.ordinal()]*Math.min(left?leftPercent:rightPercent, cardinality/origCardinality);
                            newStats[Stat.NDV_HIGH.ordinal()] = newStats[Stat.NDV.ordinal()];
                        } else {
                            newStats[Stat.NDV.ordinal()] = (float) Math.min(stats[Stat.NDV.ordinal()], Math.sqrt(cardinality));
                            newStats[Stat.NDV_HIGH.ordinal()] = Math.max(newStats[Stat.NDV.ordinal()], stats[Stat.NDV_HIGH.ordinal()]*Math.min(left?leftPercent:rightPercent, cardinality/origCardinality));
                        }
                        newStats[Stat.NDV.ordinal()] = Math.max(1, newStats[Stat.NDV.ordinal()]);
                        newStats[Stat.NDV_HIGH.ordinal()] = Math.max(1, newStats[Stat.NDV_HIGH.ordinal()]);
                    }
                    if (stats[Stat.NNV.ordinal()] != UNKNOWN_VALUE) {
                        //TODO: this is an under estimate for the inner side of outer joins
                        newStats[Stat.NNV.ordinal()] = stats[Stat.NNV.ordinal()]*Math.min(1, (cardinality==UNKNOWN_VALUE?origCardinality:cardinality)/origCardinality);
                        newStats[Stat.NNV.ordinal()] = Math.max(0, newStats[Stat.NNV.ordinal()]);
                    }
                }
            }
            newColStats.put(expr, newStats);
        }
        node.setProperty(Info.EST_COL_STATS, newColStats);
    }

    /**
     * For a source node, the cost is basically the cardinality of the source
     * (if it is known).
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private static void estimateSourceNodeCost(PlanNode node, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        float cost = UNKNOWN_VALUE;
        if(node.getChildCount() > 0) {
            SymbolMap references = (SymbolMap)node.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
            //only cost non-correlated TODO: a better estimate for correlated
            if (references == null) {
                PlanNode child = node.getFirstChild();
                cost = child.getCardinality();
                SymbolMap symbolMap = (SymbolMap)node.getProperty(NodeConstants.Info.SYMBOL_MAP);
                if (symbolMap != null) {
                    ColStats colStats = (ColStats) child.getProperty(Info.EST_COL_STATS);
                    if (colStats != null) {
                        List<? extends Expression> outputCols = getOutputCols(node, metadata);
                        ColStats newColStats = new ColStats();
                        for (Expression expr : outputCols) {
                            if (!(expr instanceof ElementSymbol)) {
                                continue;
                            }
                            ElementSymbol es = (ElementSymbol)expr;
                            Expression ex = symbolMap.getMappedExpression(es);
                            float[] value = colStats.get(ex);
                            if (value == null) {
                                Collection<ElementSymbol> elems =ElementCollectorVisitor.getElements(ex, true);
                                value = new float[3];
                                value[Stat.NDV.ordinal()] = getStat(Stat.NDV, elems, node, cost, metadata);
                                value[Stat.NDV_HIGH.ordinal()] = getStat(Stat.NDV_HIGH, elems, node, cost, metadata);
                                value[Stat.NNV.ordinal()] = getStat(Stat.NNV, elems, node, cost, metadata);
                            }
                            newColStats.put(es, value);
                        }
                        node.setProperty(Info.EST_COL_STATS, newColStats);
                    } else {
                        colStats = createColStats(node, metadata, cost);
                        node.setProperty(Info.EST_COL_STATS, colStats);
                    }
                }
            }
        }else {
            GroupSymbol group = node.getGroups().iterator().next();
            float cardinality = metadata.getCardinality(group.getMetadataID());
            if (cardinality <= QueryMetadataInterface.UNKNOWN_CARDINALITY){
                if (group.isTempTable() && metadata.getModelID(group.getMetadataID()) == TempMetadataAdapter.TEMP_MODEL) {
                    //this should be with-in the scope of a procedure or an undefined size common table
                    //
                    //the typical assumption is that this should drive other joins, thus assume
                    //a relatively small number of rows.  This is a relatively safe assumption
                    //as we do not need parallel processing with the temp fetch and the
                    //dependent join backoff should prevent unacceptable performance
                    //
                    //another strategy (that is generally applicable) is to delay the full affect of dependent join planning
                    //until the size is known - however that is somewhat complicated with the current WITH logic
                    //as the table is loaded on demand
                    cardinality = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
                } else {
                    cardinality = UNKNOWN_VALUE;
                }
            }
            cost = cardinality;
            if (!node.hasProperty(Info.ATOMIC_REQUEST)) {
                ColStats colStats = createColStats(node, metadata, cost);
                node.setProperty(Info.EST_COL_STATS, colStats);
            }
        }

        setCardinalityEstimate(node, new Float(cost), false, metadata);
    }

    private static ColStats createColStats(PlanNode node,
            QueryMetadataInterface metadata, float cardinality)
            throws QueryMetadataException, TeiidComponentException {
        ColStats colStats = new ColStats();
        List<? extends Expression> outputCols = getOutputCols(node, metadata);
        for (Expression expr : outputCols) {
            if (!(expr instanceof ElementSymbol)) {
                continue;
            }
            ElementSymbol es = (ElementSymbol)expr;
            float[] vals = new float[3];
            float ndv = metadata.getDistinctValues(es.getMetadataID());
            float nnv = metadata.getNullValues(es.getMetadataID());
            float ndv_high = ndv;
            if (cardinality != UNKNOWN_VALUE) {
                if (ndv == UNKNOWN_VALUE) {
                    if (usesKey(node, Arrays.asList(expr), metadata)) {
                        ndv = cardinality;
                        nnv = 0;
                    } else {
                        //follow the foreign keys
                        Collection<?> fks = metadata.getForeignKeysInGroup(es.getGroupSymbol().getMetadataID());
                        for (Object fk : fks) {
                            List<?> fkColumns = metadata.getElementIDsInKey(fk);
                            if (fkColumns.size() == 1 && fkColumns.get(0).equals(es.getMetadataID())) {
                                Object pk = metadata.getPrimaryKeyIDForForeignKeyID(fk);
                                List<?> pkColumns = metadata.getElementIDsInKey(pk);
                                if (pkColumns.size() == 1) {
                                    float distinctValues = metadata.getDistinctValues(pkColumns.get(0));
                                    ndv = Math.min(cardinality, distinctValues);
                                }
                                if (ndv == UNKNOWN_VALUE) {
                                    float cardinality2 = metadata.getCardinality(metadata.getGroupIDForElementID(pkColumns.get(0)));
                                    if (cardinality2 != UNKNOWN_VALUE) {
                                        ndv = Math.min(cardinality, cardinality2);
                                    }
                                }
                            }
                        }
                    }
                    if (ndv == UNKNOWN_VALUE) {
                        ndv = (float)Math.ceil(Math.pow(cardinality, .5));
                        ndv_high = cardinality / 2;
                    } else {
                        ndv_high = ndv;
                    }
                }
                float groupCardinality = metadata.getCardinality(es.getGroupSymbol().getMetadataID());
                if (groupCardinality != UNKNOWN_VALUE && groupCardinality > cardinality) {
                    if (ndv != UNKNOWN_VALUE) {
                        ndv *= cardinality / Math.max(1, groupCardinality);
                        ndv = Math.max(ndv, 1);
                        ndv_high = Math.min(ndv_high, groupCardinality);
                    }
                    if (nnv != UNKNOWN_VALUE) {
                        nnv *= cardinality / Math.max(1, groupCardinality);
                        nnv = Math.max(nnv, 1);
                    }
                }
            }
            if (es.getType() == DataTypeManager.DefaultDataClasses.BOOLEAN) {
                ndv = (ndv == UNKNOWN_VALUE?2:Math.min(ndv, 2));
                ndv_high = (ndv_high == UNKNOWN_VALUE?2:Math.min(ndv_high, 2));
            } else if (es.getType() == DataTypeManager.DefaultDataClasses.BYTE) {
                ndv = (ndv == UNKNOWN_VALUE?2:Math.min(ndv, 256));
                ndv_high = (ndv_high == UNKNOWN_VALUE?2:Math.min(ndv_high, 256));
            }
            vals[Stat.NDV.ordinal()] = ndv;
            vals[Stat.NNV.ordinal()] = nnv;
            vals[Stat.NDV_HIGH.ordinal()] = ndv_high;
            colStats.put(es, vals);
        }
        return colStats;
    }

    static List<? extends Expression> getOutputCols(PlanNode node,
            QueryMetadataInterface metadata) throws QueryMetadataException,
            TeiidComponentException {
        List<Expression> outputCols =(List<Expression>)node.getProperty(Info.OUTPUT_COLS);
        if (outputCols != null) {
            return outputCols;
        }
        PlanNode projectNode = NodeEditor.findNodePreOrder(node,
                NodeConstants.Types.PROJECT | NodeConstants.Types.GROUP
                | NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN
                | NodeConstants.Types.NULL);
        if (projectNode != null) {
            node = projectNode;
        }

        if (node.getType() == NodeConstants.Types.PROJECT) {
            return (List<? extends Expression>) node.getProperty(NodeConstants.Info.PROJECT_COLS);
        } else if (node.getType() == NodeConstants.Types.GROUP) {
            SymbolMap map = (SymbolMap)node.getProperty(Info.SYMBOL_MAP);
            return map.getKeys();
        }
        LinkedList<ElementSymbol> elements = new LinkedList<ElementSymbol>();
        for (GroupSymbol group : node.getGroups()) {
            elements.addAll(ResolverUtil.resolveElementsInGroup(group, metadata));
        }
        return elements;
    }

    /**
     * For a Group or Dup Removal node, the cost is basically the smaller of the largest NDV of the
     * selected columns and cost of the child node (if it is known).
     * @param node
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private static void estimateNodeCost(PlanNode node, List expressions, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        PlanNode child = node.getFirstChild();
        float childCost = child.getCardinality();

        if(childCost == UNKNOWN_VALUE) {
            setCardinalityEstimate(node, null, true, metadata);
            return;
        }

        float cardinality = getNDVEstimate(node, metadata, childCost, expressions, true);
        setCardinalityEstimate(node, cardinality, true, metadata);
    }

    /**
     * Get an estimate directly from the stat.  Values will be combined based upon combining percentages
     */
    static float getStat(Stat stat, Collection<? extends Expression> elems, PlanNode node,
            float cardinality, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        float result = 1;
        int branch = 0;
        boolean branchFound = false;
        if (elems.size() > 1 && !(elems instanceof Set)) {
            elems = new HashSet<Expression>(elems);
        }
        if (cardinality != UNKNOWN_VALUE) {
            cardinality = Math.max(1, cardinality);
        }
        for (Expression expression : elems) {
            ColStats colStats = (ColStats) node.getProperty(Info.EST_COL_STATS);
            if (node.getChildCount() == 0 && colStats == null) {
                colStats = createColStats(node, metadata, cardinality);
            } else if (colStats == null) {
                for (int i = branch; i < node.getChildCount(); i++) {
                    PlanNode child = node.getChildren().get(i);
                    colStats = (ColStats) child.getProperty(Info.EST_COL_STATS);
                    if (colStats == null) {
                        continue;
                    }
                    float[] stats = colStats.get(expression);
                    if (stats != null) {
                        if (node.getType() == NodeConstants.Types.SET_OP) {
                            branch = i;
                            branchFound = true;
                        }
                        break;
                    }
                    colStats = null;
                    if (branchFound) {
                        break;
                    }
                }
            }
            if (colStats == null) {
                return UNKNOWN_VALUE;
            }
            float[] stats = colStats.get(expression);
            if (stats == null || stats[stat.ordinal()] == UNKNOWN_VALUE) {
                if (stat == Stat.NDV) {
                    //use type information
                    if (expression.getType() == DataTypeManager.DefaultDataClasses.BOOLEAN) {
                        if (elems.size() == 1) {
                            result = 2;
                        } else if (cardinality != UNKNOWN_VALUE) {
                            result *= (Math.max(0, cardinality - 2))/cardinality;
                        } else {
                            result *= 2;
                        }
                        continue;
                    }
                    if (expression.getType() == DataTypeManager.DefaultDataClasses.BYTE) {
                        if (elems.size() == 1) {
                            result = 256;
                        } else if (cardinality != UNKNOWN_VALUE) {
                            result *= (Math.max(0, cardinality - 256))/cardinality;
                        } else {
                            result *= 256;
                        }
                        continue;
                    }
                }
                return UNKNOWN_VALUE;
            }
            if (elems.size() == 1) {
                result = stats[stat.ordinal()];
            } else if (cardinality != UNKNOWN_VALUE) {
                result *= (Math.max(0, cardinality - stats[stat.ordinal()]))/cardinality;
            } else {
                result *= stats[stat.ordinal()];
            }
        }
        if (cardinality == UNKNOWN_VALUE) {
            return result;
        }
        if (elems.size() > 1) {
            result = (1 - result) * cardinality;
        }
        return Math.min(result, cardinality);
    }

    static float recursiveEstimateCostOfCriteria(float childCost, PlanNode currentNode, Criteria crit, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        float cost = childCost;
        if(crit instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) crit;
            if (compCrit.getOperator() == CompoundCriteria.OR) {
                cost = 0;
            }
            if (isSingleTable(currentNode) && usesKey(compCrit, metadata)) {
                return 1;
            }
            for (Criteria critPart : compCrit.getCriteria()) {
                float nextCost = recursiveEstimateCostOfCriteria(childCost, currentNode, critPart, metadata);

                if(compCrit.getOperator() == CompoundCriteria.AND) {
                    if (nextCost == UNKNOWN_VALUE) {
                        continue;
                    }
                    if (childCost != UNKNOWN_VALUE) {
                        cost = (Math.min(cost, nextCost) + (cost * nextCost/childCost))/2;
                    } else {
                        if (cost == UNKNOWN_VALUE) {
                            cost = nextCost;
                        } else {
                            cost = Math.min(cost, nextCost);
                        }
                    }
                    if (cost <= 1) {
                        return 1;
                    }
                } else {
                    if (nextCost == UNKNOWN_VALUE) {
                        return childCost;
                    }
                    //this assumes that all disjuncts are completely disjoint
                    cost += nextCost;
                    if (childCost != UNKNOWN_VALUE) {
                        cost = Math.min(cost, childCost);
                    }
                }
            }
            if (cost == UNKNOWN_VALUE) {
                return childCost;
            }
        } else if(crit instanceof NotCriteria){
            if (childCost == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
            float nextCost = recursiveEstimateCostOfCriteria(childCost, currentNode, ((NotCriteria)crit).getCriteria(), metadata);
            if (nextCost == UNKNOWN_VALUE){
                return childCost;
            }
            cost -= nextCost;
        } else {
            cost = estimatePredicateCost(childCost, currentNode, crit, metadata);

            if (cost == UNKNOWN_VALUE) {
                return childCost;
            }
        }

        cost = Math.max(cost, 1);

        return cost;
    }

    /**
     * This method is a helper to examine whether a compound criteria covers
     * a compound key.  A "valid" criteria is
     * 1) a predicate criteria
     *   1a) not negated
     *   1b) with an equality operator if it is a compare criteria
     * b) or a compound criteria containing valid criteria and an "AND" operator
     * @param criteria
     * @param elements Collection to collect ElementSymbols in
     * @since 4.2
     */
    private static void collectElementsOfValidCriteria(Criteria criteria, Collection<ElementSymbol> elements) {

        if(criteria instanceof CompoundCriteria) {
            CompoundCriteria compCrit = (CompoundCriteria) criteria;
            Iterator<Criteria> iter = compCrit.getCriteria().iterator();
            boolean first = true;
            Collection<ElementSymbol> savedElements = elements;
            if(compCrit.getOperator() == CompoundCriteria.OR) {
                elements = new HashSet<ElementSymbol>();
            }
            while(iter.hasNext()) {
                if(compCrit.getOperator() == CompoundCriteria.AND || first) {
                    collectElementsOfValidCriteria(iter.next(), elements);
                    first = false;
                } else {
                    HashSet<ElementSymbol> other = new HashSet<ElementSymbol>();
                    collectElementsOfValidCriteria(iter.next(), other);
                    elements.retainAll(other);
                }
            }
            if (compCrit.getOperator() == CompoundCriteria.OR) {
                savedElements.addAll(elements);
            }
        } else if(criteria instanceof CompareCriteria) {
            CompareCriteria compCrit = (CompareCriteria)criteria;
            if (compCrit.getOperator() == CompareCriteria.EQ){
                ElementCollectorVisitor.getElements(compCrit, elements);
            }
        } else if(criteria instanceof MatchCriteria) {
            MatchCriteria matchCriteria = (MatchCriteria)criteria;
            if (!matchCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(matchCriteria, elements);
            }

        } else if(criteria instanceof AbstractSetCriteria) {
            AbstractSetCriteria setCriteria = (AbstractSetCriteria)criteria;
            if (!setCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(setCriteria.getExpression(), elements);
            }
        } else if(criteria instanceof IsNullCriteria) {
            IsNullCriteria isNullCriteria = (IsNullCriteria)criteria;
            if (!isNullCriteria.isNegated()) {
                ElementCollectorVisitor.getElements(isNullCriteria.getExpression(), elements);
            }
        }
    }

    /**
     * @param childCost
     * @param predicateCriteria
     * @param metadata
     * @return
     * @since 4.3
     */
    private static float estimatePredicateCost(float childCost, PlanNode currentNode, Criteria predicateCriteria, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        float cost = childCost;

        boolean isNegatedPredicateCriteria = false;
        if(predicateCriteria instanceof CompareCriteria) {
            CompareCriteria compCrit = (CompareCriteria) predicateCriteria;

            if (compCrit.isOptional()) {
                return childCost;
            }

            if (compCrit.getOperator() == CompareCriteria.EQ || compCrit.getOperator() == CompareCriteria.NE){
                if (childCost == UNKNOWN_VALUE) {
                    if (isSingleTable(currentNode)
                            && EvaluatableVisitor.willBecomeConstant(compCrit.getRightExpression())
                            && usesKey(compCrit, metadata)) {
                        return 1;
                    }
                    return UNKNOWN_VALUE;
                }
                float ndv = getPredicateNDV(compCrit.getLeftExpression(), currentNode, childCost, metadata);
                if (!EvaluatableVisitor.willBecomeConstant(compCrit.getRightExpression())) {
                    float ndv1 = getPredicateNDV(compCrit.getRightExpression(), currentNode, childCost, metadata);
                    ndv = (float) Math.sqrt(ndv * ndv1);
                }
                if (ndv > Math.sqrt(childCost)) {
                    //for larger ndv we want a smoother estimate
                    cost = (float) Math.sqrt(childCost - ndv);
                } else {
                    cost = childCost/ndv;
                }
                if (compCrit.getOperator() == CompareCriteria.NE) {
                    isNegatedPredicateCriteria = true;
                }
            } else { //GE, LE, GT, LT
                cost = getCostForComparison(childCost, metadata, compCrit);
            }
        } else if(predicateCriteria instanceof MatchCriteria) {
            if (childCost == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
            MatchCriteria matchCriteria = (MatchCriteria)predicateCriteria;

            float ndv = getPredicateNDV(matchCriteria.getLeftExpression(), currentNode, childCost, metadata);

            cost = estimateMatchCost(childCost, ndv, matchCriteria);

            isNegatedPredicateCriteria = matchCriteria.isNegated();

        } else if(predicateCriteria instanceof SetCriteria) {
            SetCriteria setCriteria = (SetCriteria) predicateCriteria;

            if (childCost == UNKNOWN_VALUE) {
                if (isSingleTable(currentNode) && usesKey(setCriteria, metadata)) {
                    return setCriteria.getNumberOfValues();
                }
                return UNKNOWN_VALUE;
            }

            float ndv = getPredicateNDV(setCriteria.getExpression(), currentNode, childCost, metadata);

            if (ndv > Math.sqrt(childCost)) {
                cost = (float) Math.sqrt(Math.max(1, childCost - ndv)) * setCriteria.getNumberOfValues();
            } else {
                cost = childCost * setCriteria.getNumberOfValues() / ndv;
            }

            isNegatedPredicateCriteria = setCriteria.isNegated();

        } else if(predicateCriteria instanceof SubquerySetCriteria) {
            if (childCost == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
            SubquerySetCriteria setCriteria = (SubquerySetCriteria) predicateCriteria;

            // TODO - use inner ProcessorPlan cardinality estimates
            // to determine the estimated number of values
            cost = childCost / 3;

            isNegatedPredicateCriteria = setCriteria.isNegated();

        } else if(predicateCriteria instanceof IsNullCriteria) {
            Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(predicateCriteria, true);

            IsNullCriteria isNullCriteria = (IsNullCriteria)predicateCriteria;

            float nnv = getStat(Stat.NNV, elements, currentNode, childCost, metadata);
            if (nnv == UNKNOWN_VALUE) {
                if (childCost == UNKNOWN_VALUE) {
                    return UNKNOWN_VALUE;
                }
                float ndv = getPredicateNDV(isNullCriteria.getExpression(), currentNode, childCost, metadata);
                cost = (childCost - ndv)/8;
            } else {
                if (childCost == UNKNOWN_VALUE) {
                    if (!isNullCriteria.isNegated()) {
                        return nnv;
                    }
                    return UNKNOWN_VALUE;
                }
                cost = nnv;
            }

            isNegatedPredicateCriteria = isNullCriteria.isNegated();
        } else if (predicateCriteria instanceof DependentSetCriteria) {
            if (childCost == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
            DependentSetCriteria dsc = (DependentSetCriteria)predicateCriteria;

            if (dsc.getNdv() == UNKNOWN_VALUE) {
                return childCost / 3;
            }

            float ndv = getPredicateNDV(dsc.getExpression(), currentNode, childCost, metadata);

            cost = childCost * dsc.getNdv() / Math.max(1, ndv);
        }

        if (cost == UNKNOWN_VALUE) {
            return UNKNOWN_VALUE;
        }

        if (cost > childCost) {
            cost = childCost;
        }

        if (isNegatedPredicateCriteria) {
            // estimate for NOT in the predicate
            cost = (cost != UNKNOWN_VALUE)
                    ? Math.max( childCost - cost, 1)
                    : UNKNOWN_VALUE;
        }

        return cost;
    }

    private static float getPredicateNDV(LanguageObject object, PlanNode currentNode, float childCost, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(object, true);

        Collection<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(elements);
        boolean multiGroup = groups.size() > 1;

        float ndv = getStat(Stat.NDV, elements, currentNode, childCost, metadata);

        if (ndv == UNKNOWN_VALUE) {
            boolean usesKey = usesKey(elements, metadata);
            if (multiGroup) {
                if (usesKey) {
                    ndv = (float)Math.ceil(Math.sqrt(childCost));
                } else {
                    ndv = (float)Math.ceil(Math.sqrt(childCost)/4);
                }
            } else if (usesKey) {
                ndv = childCost;
            } else {
                ndv = (float)Math.ceil(Math.sqrt(childCost)/2);
            }
            ndv = Math.max(ndv, 1);
        }

        return ndv;
    }

    /**
     * TODO: does not check for escape char
     * or if it will contain single match chars
     */
    private static float estimateMatchCost(float childCost,
                                           float ndv,
                                           MatchCriteria criteria) {
        Expression matchExpression = criteria.getRightExpression();
        if(matchExpression instanceof Constant && ((Constant)matchExpression).getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
            String compareValue = (String) ((Constant)matchExpression).getValue();
            if(criteria.getMode() != MatchMode.REGEX && criteria.getEscapeChar() == MatchCriteria.NULL_ESCAPE_CHAR
                    && compareValue != null && compareValue.indexOf('%') < 0) {
                return (childCost / 2) * (1 / 3f  + 1 / ndv); //without knowing length constraints we'll make an average guess
            }
        } else if (EvaluatableVisitor.willBecomeConstant(criteria.getLeftExpression())) {
            if (ndv > Math.sqrt(childCost)) {
                return (float) Math.sqrt(childCost - ndv);
            }
            return childCost/ndv;
        }
        return childCost / 3;
    }

    private static float getCostForComparison(float childCost,
                                              QueryMetadataInterface metadata,
                                              CompareCriteria compCrit) throws TeiidComponentException,
                                                                       QueryMetadataException {
        if (!(compCrit.getLeftExpression() instanceof ElementSymbol) || !(compCrit.getRightExpression() instanceof Constant)) {
            return childCost/3;
        }
        ElementSymbol element = (ElementSymbol)compCrit.getLeftExpression();
        Class<?> dataType = compCrit.getRightExpression().getType();

        String max = (String)metadata.getMaximumValue(element.getMetadataID());
        String min = (String)metadata.getMinimumValue(element.getMetadataID());
        if(max == null || min == null) {
            return childCost/3;
        }
        float cost = childCost;
        try{
            float maxValue = 0;
            float minValue = 0;

            Constant value = (Constant)compCrit.getRightExpression();
            float compareValue = 0;
            // Case 6257 - handling added for time and date.  If the max/min values are not
            // in the expected format, NumberFormatException is thrown and reverts to default costing.
            if(dataType.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
                compareValue = ((Timestamp)value.getValue()).getTime();
                maxValue = Timestamp.valueOf(max).getTime();
                minValue = Timestamp.valueOf(min).getTime();
            } else if(dataType.equals(DataTypeManager.DefaultDataClasses.TIME)) {
                compareValue = ((Time)value.getValue()).getTime();
                maxValue = Time.valueOf(max).getTime();
                minValue = Time.valueOf(min).getTime();
            // (For date, our costing sets the max and min values using timestamp format)
            } else if(dataType.equals(DataTypeManager.DefaultDataClasses.DATE)) {
                compareValue = ((Date)value.getValue()).getTime();
                maxValue = Timestamp.valueOf(max).getTime();
                minValue = Timestamp.valueOf(min).getTime();
            } else {
                if(!Number.class.isAssignableFrom(dataType)) {
                    return childCost/3;
                }
                compareValue = ((Number)value.getValue()).floatValue();
                maxValue = Integer.parseInt(max);
                minValue = Integer.parseInt(min);
            }
            float range = Math.max(maxValue - minValue, 1);

            float costMultiple = 1;

            if(compCrit.getOperator() == CompareCriteria.GT || compCrit.getOperator() == CompareCriteria.GE) {
                costMultiple = (maxValue - compareValue)/range;
                if (compareValue < 0 && maxValue < 0) {
                    costMultiple = (1 - costMultiple);
                }
            } else if(compCrit.getOperator() == CompareCriteria.LT || compCrit.getOperator() == CompareCriteria.LE) {
                costMultiple = (compareValue - minValue)/range;
                if (compareValue < 0 && minValue < 0) {
                    costMultiple = (1 - costMultiple);
                }
            }
            if (costMultiple > 1) {
                costMultiple = 1;
            } else if (costMultiple < 0) {
                costMultiple = 0;
            }
            cost = childCost * costMultiple;
        } catch(IllegalArgumentException e) {
            LogManager.logWarning(LogConstants.CTX_QUERY_PLANNER, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30029));
            // If we were unable to parse the timestamp we will revert to the divide by three estimate
            cost = childCost/3;
        }
        return cost;
    }

    static boolean usesKey(PlanNode planNode, Collection<? extends Expression> allElements, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        //TODO: key preserved joins should be marked
        return isSingleTable(planNode)
        && usesKey(allElements, metadata);
    }

    static boolean isSingleTable(PlanNode planNode) {
        return planNode.getChildCount() == 0 || NodeEditor.findAllNodes(planNode, NodeConstants.Types.SOURCE, NodeConstants.Types.JOIN | NodeConstants.Types.SET_OP).size() == 1;
    }

    public static boolean usesKey(Criteria crit, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        HashSet<ElementSymbol> elements = new HashSet<ElementSymbol>();
        collectElementsOfValidCriteria(crit, elements);
        return usesKey(elements, metadata);
    }

    /**
     * TODO: this uses key check is not really accurate, it doesn't take into consideration where
     * we are in the plan.
     * if a key column is used after a non 1-1 join or a union all, then it may be non-unique.
     */
    public static boolean usesKey(Collection<? extends Expression> allElements, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {
        return usesKey(allElements, null, metadata, true);
    }

    public static boolean usesKey(Collection<? extends Expression> allElements, Set<GroupSymbol> groups, QueryMetadataInterface metadata, boolean unique)
    throws QueryMetadataException, TeiidComponentException {
        return getKeyUsed(allElements, groups, metadata, unique) != null;
    }

    public static Object getKeyUsed(Collection<? extends Expression> allElements, Set<GroupSymbol> groups, QueryMetadataInterface metadata, Boolean unique)
    throws QueryMetadataException, TeiidComponentException {

        if(allElements == null || allElements.size() == 0) {
            return null;
        }

        // Sort elements into groups
        Map<GroupSymbol, List<Object>> groupMap = new HashMap<GroupSymbol, List<Object>>();
        for (Expression ses : allElements) {
            Expression ex = SymbolMap.getExpression(ses);
            if (!(ex instanceof ElementSymbol)) {
                continue; //TODO: function based indexes are possible, but we don't have the metadata
            }
            ElementSymbol element = (ElementSymbol)ex;
            GroupSymbol group = element.getGroupSymbol();
            if (groups != null && !groups.contains(group)) {
                continue;
            }
            List<Object> elements = groupMap.get(group);
            if(elements == null) {
                elements = new ArrayList<Object>();
                groupMap.put(group, elements);
            }
            elements.add(element.getMetadataID());
        }

        // Walk through each group
        for (Map.Entry<GroupSymbol, List<Object>> entry : groupMap.entrySet()) {
            GroupSymbol group = entry.getKey();
            List<Object> elements = entry.getValue();

            // Look up keys
            Collection keys = null;
            if ((unique != null && unique) || unique == null) {
                keys = metadata.getUniqueKeysInGroup(group.getMetadataID());
            }
            if ((unique != null && !unique) || unique == null) {
                if (keys != null) {
                    keys = new ArrayList<Object>(keys);
                } else {
                    keys = new ArrayList<Object>(2);
                }
                keys.addAll(metadata.getIndexesInGroup(group.getMetadataID()));
            }

            if(keys != null && keys.size() > 0) {
                // For each key, get key elements
                for (Object key : keys) {
                    List keyElements = metadata.getElementIDsInKey(key);
                    if(elements.containsAll(keyElements)) {
                        // Used all elements of the key
                        return key;
                    }
                }
            }
        }

        return null;
    }

    private static float safeLog(float x) {
        return (float)Math.max(1, Math.log(x));
    }

    /**
     * Computes the cost of a Dependent Join
     *
     * The worst possible cost will arise from a high independent ndv (many dependent sets) and a low dependent ndv (possibly many matches per set)
     *
     * This logic uses the same assumption as criteria in that ndv is used as a divisor of cardinality.
     * @throws QueryPlannerException
     *
     */
    public static DependentCostAnalysis computeCostForDepJoin(PlanNode joinNode, boolean leftIndependent, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context)
        throws TeiidComponentException, QueryMetadataException, QueryPlannerException {

        PlanNode independentNode = leftIndependent?joinNode.getFirstChild():joinNode.getLastChild();
        PlanNode dependentNode = leftIndependent?joinNode.getLastChild():joinNode.getFirstChild();

        List independentExpressions = (List)(leftIndependent?joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS));
        List dependentExpressions = (List)(leftIndependent?joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS):joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS));

        return computeCostForDepJoin(independentNode, dependentNode,
                independentExpressions, dependentExpressions, metadata,
                capFinder, context);
    }

    public static DependentCostAnalysis computeCostForDepJoin(PlanNode independentNode,
            PlanNode dependentNode, List independentExpressions,
            List dependentExpressions, QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, CommandContext context)
            throws QueryMetadataException, TeiidComponentException, QueryPlannerException {

        float independentCardinality = computeCostForTree(independentNode, metadata);
        float dependentCardinality = computeCostForTree(dependentNode, metadata);

        DependentCostAnalysis dca = new DependentCostAnalysis();
        dca.maxNdv = new Float[independentExpressions.size()];
        dca.expectedNdv = new Float[independentExpressions.size()];

        if (independentCardinality == UNKNOWN_VALUE || dependentCardinality == UNKNOWN_VALUE) {
            return dca; //no cost information to be determined
        }

        float processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
        if(context != null) {
            processorBatchSize = context.getProcessorBatchSize();
        }

        RulePushSelectCriteria rpsc = new RulePushSelectCriteria();
        rpsc.setCreatedNodes(new LinkedList<PlanNode>());

        for (int i = 0; i < independentExpressions.size(); i++) {
            Expression indExpr = (Expression)independentExpressions.get(i);
            Collection<ElementSymbol> indElements = ElementCollectorVisitor.getElements(indExpr, true);
            //use a mid estimate
            float indSymbolNDV = getNDVEstimate(independentNode, metadata, independentCardinality, indElements, true);
            boolean unknownNDV = false;
            if (indSymbolNDV == UNKNOWN_VALUE) {
                indSymbolNDV = independentCardinality;
            }
            Expression depExpr = (Expression)dependentExpressions.get(i);

            LinkedList<Expression> depExpressions = new LinkedList<Expression>();
            LinkedList<PlanNode> targets = determineTargets(dependentNode,
                    metadata, capFinder, rpsc, depExpr, depExpressions);

            Iterator<Expression> exprIter = depExpressions.iterator();
            for (Iterator<PlanNode> targetIter = targets.iterator(); targetIter.hasNext();) {
                PlanNode target = targetIter.next();
                Expression targerDepExpr = exprIter.next();
                boolean isAccess = target.getType() == NodeConstants.Types.ACCESS;
                PlanNode accessNode = isAccess?target:NodeEditor.findParent(target, NodeConstants.Types.ACCESS);

                float setCriteriaBatchSize = indSymbolNDV;

                if (accessNode != null) {
                    setCriteriaBatchSize = CapabilitiesUtil.getMaxInCriteriaSize(RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder);
                    if (setCriteriaBatchSize < 1) {
                        setCriteriaBatchSize = indSymbolNDV;
                    } else {
                        int numberOfSets = CapabilitiesUtil.getMaxDependentPredicates(RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder);
                        if (numberOfSets > 0) {
                            setCriteriaBatchSize *= Math.max(1, numberOfSets /dependentExpressions.size()); //scale down to be conservative
                        }
                    }
                } else if (indSymbolNDV > processorBatchSize) {
                    //don't bother making a virtual join dependent if they are likely to be large
                    //TODO: what operations are performed between origNode and dependentNode
                    //TODO: we should be using a tree structure rather than just a value iterator
                    continue;
                }
                Collection<ElementSymbol> depElems = ElementCollectorVisitor.getElements(targerDepExpr, true);
                if (!isAccess) {
                    while (target.getParent().getType() == NodeConstants.Types.SELECT) {
                        target = target.getParent();
                    }
                } //TODO: we can be more particluar about what criteria we're considering
                float depTargetCardinality = computeCostForTree(target, metadata);
                if (depTargetCardinality == UNKNOWN_VALUE) {
                    continue;
                }
                //use a high/low estimate
                float depSymbolNDV = getNDVEstimate(target, metadata, depTargetCardinality, depElems, isAccess?true:null);
                boolean usesKey = usesKey(target, depElems, metadata);
                if (depSymbolNDV == UNKNOWN_VALUE) {
                    //should not come here - as we should have an estimate from above
                    if (!usesKey) {
                        //make an educated guess that this is a fk
                        float indSymbolOrigNDV = indSymbolNDV;
                        float indCardinalityOrig = independentCardinality;
                        //TODO: we should probably dig deeper than this
                        PlanNode indOrigNode = FrameUtil.findOriginatingNode(independentNode, GroupsUsedByElementsVisitor.getGroups(indElements));
                        if (indOrigNode != null) {
                            indCardinalityOrig = computeCostForTree(indOrigNode, metadata);
                            indSymbolOrigNDV = getStat(Stat.NDV, indElements, indOrigNode, indCardinalityOrig, metadata);
                            if (indSymbolOrigNDV == UNKNOWN_VALUE) {
                                indSymbolOrigNDV = indCardinalityOrig * indSymbolNDV / independentCardinality;
                            }
                        }
                        depSymbolNDV = Math.max((float)Math.pow(depTargetCardinality, .75), Math.min(indSymbolOrigNDV, depTargetCardinality));
                        unknownNDV = true;
                    } else {
                        depSymbolNDV = depTargetCardinality;
                    }
                }
                boolean usesIndex = accessNode != null && usesKey;
                if (!usesKey && accessNode != null && target.getType() == NodeConstants.Types.SOURCE && target.getChildCount() == 0) {
                    usesIndex = usesKey(depElems, target.getGroups(), metadata, false);
                }
                float[] estimates = estimateCost(accessNode, setCriteriaBatchSize, usesIndex, depTargetCardinality, indSymbolNDV, dependentCardinality, depSymbolNDV, independentCardinality);
                if (estimates[1] < 0) {
                    if (dca.expectedCardinality == null) {
                        dca.expectedCardinality = estimates[0];
                    } else {
                        dca.expectedCardinality = Math.min(dca.expectedCardinality, estimates[0]);
                    }
                }
                //don't use the ndv if it is unknown or significantly smaller than the high estimate
                if (unknownNDV || depSymbolNDV > 2 * getNDVEstimate(target, metadata, depTargetCardinality, depElems, null)) {
                    continue;
                }
                dca.expectedNdv[i] = indSymbolNDV;
                if (indSymbolNDV >= depSymbolNDV) {
                    dca.maxNdv[i] = indSymbolNDV;
                    continue;
                }
                //use a quick "binary" search to find the max ndv
                float min = 0;
                float max = Math.min(Integer.MAX_VALUE, depSymbolNDV);
                float tempNdv = indSymbolNDV;
                for (int j = 0; j < 10; j++) {
                    if (estimates[1] > 1) {
                        max = tempNdv;
                        tempNdv = (tempNdv + min)/2;
                    } else if (estimates[1] < 0) {
                        min = tempNdv;
                        //we assume that values should be closer to the min side
                        tempNdv = Math.min(tempNdv * 8 + 1, (tempNdv + max)/2);
                    } else {
                        break;
                    }
                    estimates = estimateCost(accessNode, setCriteriaBatchSize, usesIndex, depTargetCardinality, tempNdv, dependentCardinality, depSymbolNDV, independentCardinality);
                }
                dca.maxNdv[i] = tempNdv;
            }
        }
        return dca;
    }

    private static float[] estimateCost(PlanNode accessNode, float setCriteriaBatchSize, boolean usesIndex, float depTargetCardinality,
            float indSymbolNDV, float dependentCardinality, float depSymbolNDV, float independentCardinality) {
        float scalingFactor = 1;
        if (indSymbolNDV < independentCardinality && depTargetCardinality > 4*independentCardinality && indSymbolNDV > Math.sqrt(depSymbolNDV)) {
            //when the dep ndv is relatively small, we need a better estimate of the reduction in dependent tuples
            scalingFactor = (float) (indSymbolNDV / Math.sqrt(depSymbolNDV*depTargetCardinality));
        } else {
            scalingFactor = indSymbolNDV/depSymbolNDV;
        }
        float dependentAccessCardinality = Math.min(depTargetCardinality, depTargetCardinality * scalingFactor);
        float scaledCardinality = Math.min(dependentCardinality, dependentCardinality * scalingFactor);
        float numberComparisons = (usesIndex?safeLog(depTargetCardinality):depTargetCardinality) * (usesIndex?indSymbolNDV:safeLog(indSymbolNDV));
        numberComparisons += independentCardinality * safeLog(indSymbolNDV);
        float newDependentQueries = accessNode == null?0:(float)Math.ceil(indSymbolNDV / setCriteriaBatchSize);

        float relativeCost = newDependentQueries*procNewRequestTime;
        float relativeComparisonCost = (numberComparisons - safeLog(scaledCardinality) /*no longer needed by the join*/
            /*sort cost reduction, however it's always true if its on the source and using an index
              TODO: there are other cost reductions, which we could get by checking the other parent nodes */
            + (scaledCardinality*safeLog(scaledCardinality) - dependentCardinality*safeLog(dependentCardinality)))
            * compareTime;
        float relativeReadCost = (dependentAccessCardinality - depTargetCardinality)*readTime; //cardinality reductions
        return new float[] {scaledCardinality, relativeCost + relativeComparisonCost + relativeReadCost};
    }

    /**
     * For now we only consider a single target. In the future we may consider multiple.
     */
    private static LinkedList<PlanNode> determineTargets(
            PlanNode dependentNode, QueryMetadataInterface metadata,
            CapabilitiesFinder capFinder, RulePushSelectCriteria rpsc,
            Expression depExpr, LinkedList<Expression> depExpressions)
            throws QueryPlannerException, TeiidComponentException {
        LinkedList<PlanNode> targets = new LinkedList<PlanNode>();
        LinkedList<PlanNode> critNodes = new LinkedList<PlanNode>();
        PlanNode select = RelationalPlanner.createSelectNode(new DependentSetCriteria(depExpr, null), false);
        // mark as not a dependent set so that the flag doesn't inadvertently change anything
        select.setProperty(Info.IS_DEPENDENT_SET, false);
        critNodes.add(select);
        LinkedList<PlanNode> initialTargets = new LinkedList<PlanNode>();
        initialTargets.add(dependentNode);
        while (!critNodes.isEmpty()) {
            PlanNode critNode = critNodes.remove();
            PlanNode initial = initialTargets.remove();
            if (critNode.getGroups().isEmpty()) {
                //TODO: we need to project constants up through a plan to avoid this case
                continue;
            }
            PlanNode sourceNode = FrameUtil.findOriginatingNode(initial, critNode.getGroups());
            if (sourceNode == null) {
                continue;
            }
            PlanNode target = sourceNode;
            PlanNode accessNode = NodeEditor.findParent(target, NodeConstants.Types.ACCESS);
            if (accessNode != null) {
                if (accessNode.hasBooleanProperty(Info.MAKE_NOT_DEP)) {
                    targets.clear();
                    break;
                }
                targets.add(accessNode);
                DependentSetCriteria dsc = (DependentSetCriteria)critNode.getProperty(Info.SELECT_CRITERIA);
                depExpressions.add(dsc.getExpression());
                List<PlanNode> sources = NodeEditor.findAllNodes(target, NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE);
                if (sources.size() == 1) {
                    PlanNode source = sources.get(0);
                    if (source.getChildCount() == 0) {
                        continue;
                    }
                }
            }
            if (initial != sourceNode) {
                //pretend the criteria starts at the initial location
                //either above or below depending upon the node type
                if (initial.getChildCount() > 1) {
                    initial.addAsParent(critNode);
                } else {
                    initial.getFirstChild().addAsParent(critNode);
                }
                target = rpsc.examinePath(critNode, sourceNode, metadata, capFinder);
                critNode.getParent().replaceChild(critNode, critNode.getFirstChild());
            }
            if (target != sourceNode || (sourceNode.getType() == NodeConstants.Types.SOURCE && sourceNode.getChildCount() == 0)) {
                if (target.hasBooleanProperty(Info.MAKE_NOT_DEP)) {
                    targets.clear();
                    break;
                }
                targets.add(target);
                DependentSetCriteria dsc = (DependentSetCriteria)critNode.getProperty(Info.SELECT_CRITERIA);
                depExpressions.add(dsc.getExpression());
                continue;
            }
            switch (sourceNode.getType()) {
            case NodeConstants.Types.SOURCE: {
                PlanNode child = sourceNode.getFirstChild();
                child = FrameUtil.findOriginatingNode(child, child.getGroups());
                if (child != null && child.getType() == NodeConstants.Types.SET_OP) {
                    if (target.hasBooleanProperty(Info.MAKE_NOT_DEP)) {
                        targets.clear();
                        break;
                    }
                    targets.add(target);
                    DependentSetCriteria dsc = (DependentSetCriteria)critNode.getProperty(Info.SELECT_CRITERIA);
                    depExpressions.add(dsc.getExpression());
                    //TODO: we may need better handling for set op situations
                    //for now the strategy is to mark both the union root and the children as targets
                    //continue;
                }
                if (!rpsc.pushAcrossFrame(sourceNode, critNode, metadata, capFinder, null)) {
                    if (target.hasBooleanProperty(Info.MAKE_NOT_DEP)) {
                        targets.clear();
                        break;
                    }
                    targets.add(target);
                    DependentSetCriteria dsc = (DependentSetCriteria)critNode.getProperty(Info.SELECT_CRITERIA);
                    depExpressions.add(dsc.getExpression());
                }
                List<PlanNode> createdNodes = rpsc.getCreatedNodes();
                for (PlanNode planNode : createdNodes) {
                    critNodes.add(planNode);
                    initialTargets.add(planNode.getFirstChild());
                    NodeEditor.removeChildNode(planNode.getParent(), planNode);
                }
                rpsc.getCreatedNodes().clear();
                break;
            }
            case NodeConstants.Types.GROUP: {
                if (rpsc.pushAcrossGroupBy(sourceNode, critNode, metadata, false, capFinder)) {
                    critNodes.add(critNode);
                    initialTargets.add(sourceNode.getFirstChild());
                }
                break;
            }
            }
            //the source must be a null or project node, which we don't care about
        }
        return targets;
    }

    /**
     *
     * @param indNode
     * @param metadata
     * @param cardinality
     * @param elems
     * @param useCardinalityIfUnknown - false is a low estimate, null uses a middle estimate, and true uses a high estimate
     * @return
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    static float getNDVEstimate(PlanNode indNode,
            QueryMetadataInterface metadata, float cardinality,
            Collection<? extends Expression> elems, Boolean useCardinalityIfUnknown) throws QueryMetadataException,
            TeiidComponentException {
        if (elems == null || elems.isEmpty()) {
            return cardinality;
        }
        float ndv = getStat(Stat.NDV, elems, indNode, cardinality, metadata);
        //if we're using cardinality, then we want to include the high estimate
        if (ndv != UNKNOWN_VALUE && (useCardinalityIfUnknown == null || useCardinalityIfUnknown)) {
            float ndv_high = getStat(Stat.NDV_HIGH, elems, indNode, cardinality, metadata);
            if (ndv_high != UNKNOWN_VALUE) {
                if (useCardinalityIfUnknown == null) {
                    ndv = (float) Math.sqrt(ndv * ndv_high);
                } else {
                    ndv = (ndv + ndv_high)/2;
                }
            }
        }
        //special handling if cardinality has been set, but not ndv
        if (ndv == UNKNOWN_VALUE && (useCardinalityIfUnknown == null || useCardinalityIfUnknown)) {
            Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(elems);
            PlanNode source = FrameUtil.findOriginatingNode(indNode, groups);
            if (source != null) {
                ndv = getStat(Stat.NDV, elems, source, source.getCardinality(), metadata);
                if (ndv == UNKNOWN_VALUE) {
                    if (useCardinalityIfUnknown != null || source.getChildCount() == 0) {
                        ndv = source.getCardinality();
                    }
                    if (ndv != UNKNOWN_VALUE && !usesKey(source, elems, metadata)) {
                        ndv/=2; //guess that it's non-unique
                    }
                }
                if (ndv != UNKNOWN_VALUE) {
                    while (source != indNode) {
                        source = source.getParent();
                        float parentCardinality = source.getCardinality();
                        if (parentCardinality != UNKNOWN_VALUE && parentCardinality < ndv) {
                            ndv = parentCardinality;
                        }
                    }
                }
            }
        }
        if (ndv == UNKNOWN_VALUE) {
            if (cardinality == UNKNOWN_VALUE) {
                return UNKNOWN_VALUE;
            }
            if (usesKey(indNode, elems, metadata)) {
                ndv = cardinality;
            } else if (useCardinalityIfUnknown != null && useCardinalityIfUnknown) {
                ndv = cardinality/2;
            } else {
                return UNKNOWN_VALUE;
            }
        }
        if (cardinality != UNKNOWN_VALUE && cardinality < ndv) {
            ndv = cardinality;
        }
        return Math.max(1, ndv);
    }

}
