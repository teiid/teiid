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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.teiid.core.TeiidException;
import org.teiid.core.util.Assertion;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;


/**
 * <p>
 * Utility methods for query planning related to joins.
 * <p>
 * In some cases, a query plan can be made more optimal via a few possible
 * criteria/join optimizations.
 *
 */
public class JoinUtil {

    /**
     * Can't instantiate
     */
    private JoinUtil() {
        super();
    }

    /**
     * Will attempt to optimize the join type based upon the criteria provided.
     *
     * Returns the new join type if one is found, otherwise null
     *
     * An outer join can be optimized if criteria that is not dependent upon null values
     * is applied on the inner side of the join.
     *
     * @param critNode
     * @param joinNode
     * @return
     */
    static final JoinType optimizeJoinType(PlanNode critNode, PlanNode joinNode, QueryMetadataInterface metadata, boolean modifyJoin) {
        if (critNode.getGroups().isEmpty() || !joinNode.getGroups().containsAll(critNode.getGroups()) || joinNode.hasBooleanProperty(Info.PRESERVE)) {
            return null;
        }

        JoinType joinType = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);

        if (!joinType.isOuter()) {
            return null;
        }

        PlanNode left = joinNode.getFirstChild();
        left = FrameUtil.findJoinSourceNode(left);
        PlanNode right = joinNode.getLastChild();
        right = FrameUtil.findJoinSourceNode(right);

        Collection<GroupSymbol> outerGroups = left.getGroups();
        Collection<GroupSymbol> innerGroups = right.getGroups();
        if (joinType == JoinType.JOIN_RIGHT_OUTER) {
            outerGroups = innerGroups;
            innerGroups = left.getGroups();
        }

        //sanity check
        if ((joinType == JoinType.JOIN_LEFT_OUTER || joinType == JoinType.JOIN_RIGHT_OUTER)
                        && outerGroups.containsAll(critNode.getGroups())) {
            return null;
        }

        Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);

        boolean isNullDepdendent = isNullDependent(metadata, innerGroups, crit);

        JoinType result = JoinType.JOIN_INNER;

        if (joinType == JoinType.JOIN_LEFT_OUTER || joinType == JoinType.JOIN_RIGHT_OUTER) {
            if (isNullDepdendent) {
                return null;
            }
        } else {
            boolean isNullDepdendentOther = isNullDependent(metadata, outerGroups, crit);

            if (isNullDepdendent && isNullDepdendentOther) {
                return null;
            }

            if (isNullDepdendent && !isNullDepdendentOther) {
                result =  JoinType.JOIN_LEFT_OUTER;
            } else if (!isNullDepdendent && isNullDepdendentOther) {
                if (modifyJoin) {
                    JoinUtil.swapJoinChildren(joinNode);
                    result = JoinType.JOIN_LEFT_OUTER;
                }
            }
        }

        if (modifyJoin) {
            joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, result);
        }

        return result;
    }

    /**
     *  Returns true if the given criteria can be anything other than false (or unknown)
     *  given all null values for elements in the inner groups
     */
    public static boolean isNullDependent(QueryMetadataInterface metadata,
                                            final Collection<GroupSymbol> innerGroups,
                                            Criteria crit) {
        Criteria simplifiedCrit = (Criteria)replaceWithNullValues(innerGroups, crit);
        try {
            simplifiedCrit = QueryRewriter.rewriteCriteria(simplifiedCrit, null, metadata);
        } catch (TeiidException err) {
            //log the exception
            return true;
        }
        if (simplifiedCrit.equals(QueryRewriter.FALSE_CRITERIA) || simplifiedCrit.equals(QueryRewriter.UNKNOWN_CRITERIA)) {
            return false;
        }
        //this is a narrow check added for test consistency with TEIID-5933
        //the query rewriter based logic can't catch this and similar cases as it's possible
        //to be either false or unknown, so it can't fully rewrite
        if (simplifiedCrit instanceof SubquerySetCriteria) {
            SubquerySetCriteria setCriteria = (SubquerySetCriteria)simplifiedCrit;
            if (!setCriteria.isNegated() && QueryRewriter.isNull(setCriteria.getExpression())) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNullDependent(QueryMetadataInterface metadata,
                                          final Collection<GroupSymbol> innerGroups,
                                          Expression expr) {
        Expression simplifiedExpression = (Expression)replaceWithNullValues(innerGroups, expr);
        try {
            simplifiedExpression = QueryRewriter.rewriteExpression(simplifiedExpression, null, metadata);
        } catch (TeiidException err) {
            //log the exception
            return true;
        }
        return !QueryRewriter.isNull(simplifiedExpression);
    }

    private static LanguageObject replaceWithNullValues(final Collection<GroupSymbol> innerGroups,
                                                        LanguageObject obj) {
        ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {

            public Expression replaceExpression(Expression element) {
                if (!(element instanceof ElementSymbol)) {
                    return element;
                }

                ElementSymbol symbol = (ElementSymbol)element;

                if (innerGroups.contains(symbol.getGroupSymbol())) {
                    return new Constant(null, symbol.getType());
                }

                return element;
            }
        };

        if (obj instanceof ElementSymbol) {
            return emv.replaceExpression((ElementSymbol)obj);
        }
        obj = (LanguageObject)obj.clone();
        PreOrderNavigator.doVisit(obj, emv);
        return obj;
    }

    static JoinType getJoinTypePreventingCriteriaOptimization(PlanNode joinNode, PlanNode critNode) {
        Set<GroupSymbol> groups = critNode.getGroups();

        //special case for 0 group criteria
        if (groups.size() == 0) {
            critNode = FrameUtil.findOriginatingNode(critNode, groups);
            if (critNode == null) {
                return null;
            }
            groups = critNode.getGroups();
        }

        return getJoinTypePreventingCriteriaOptimization(joinNode, groups);
    }

    public static JoinType getJoinTypePreventingCriteriaOptimization(PlanNode joinNode,
                                                                      Set<GroupSymbol> groups) {
        JoinType joinType = (JoinType) joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);

        if(!joinType.isOuter()) {
            return null;
        }

        if(joinType.equals(JoinType.JOIN_FULL_OUTER)) {
            return joinType;
        }

        Set<GroupSymbol> innerGroups = getInnerSideJoinNodes(joinNode)[0].getGroups();
        for (GroupSymbol group : groups) {
            if (innerGroups.contains(group)) {
                return joinType;
            }
        }

        return null;
    }

    /**
     * Can be called after join planning on a join node to get the inner sides of the join
     * @param joinNode
     * @return
     */
    static PlanNode[] getInnerSideJoinNodes(PlanNode joinNode) {
        Assertion.assertTrue(joinNode.getType() == NodeConstants.Types.JOIN);
        JoinType jt = (JoinType)joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);

        if (jt == JoinType.JOIN_INNER || jt == JoinType.JOIN_CROSS) {
            return new PlanNode[] {joinNode.getFirstChild(), joinNode.getLastChild()};
        }
        if (jt == JoinType.JOIN_RIGHT_OUTER) {
            return new PlanNode[] {joinNode.getFirstChild()};
        }
        if (jt == JoinType.JOIN_LEFT_OUTER) {
            return new PlanNode[] {joinNode.getLastChild()};
        }
        //must be full outer, so there is no inner side
        return new PlanNode[] {};
    }

    /**
     * @param joinNode
     */
    static void swapJoinChildren(PlanNode joinNode) {
        PlanNode leftChild = joinNode.getFirstChild();
        joinNode.removeChild(leftChild);
        joinNode.addLastChild(leftChild);
        List leftExpressions = (List)joinNode.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS);
        List rightExpressions = (List)joinNode.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS);
        joinNode.setProperty(NodeConstants.Info.LEFT_EXPRESSIONS, rightExpressions);
        joinNode.setProperty(NodeConstants.Info.RIGHT_EXPRESSIONS, leftExpressions);
        JoinType jt = (JoinType)joinNode.getProperty(NodeConstants.Info.JOIN_TYPE);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, jt.getReverseType());
    }

}
