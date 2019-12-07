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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;

/*
 * TODO: may need to rerun plan joins sequence after this
 */
public class RulePlanOuterJoins implements OptimizerRule {

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, RuleStack rules,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {

        boolean beforeJoinPlanning = rules.contains(RuleConstants.PLAN_JOINS);

        while (beforeJoinPlanning?
                planLeftOuterJoinAssociativityBeforePlanning(plan, metadata, capabilitiesFinder, analysisRecord, context):
                planLeftOuterJoinAssociativity(plan, metadata, capabilitiesFinder, analysisRecord, context)) {
            //repeat
        }
        return plan;
    }

    private boolean planLeftOuterJoinAssociativity(PlanNode plan,
            QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryMetadataException, TeiidComponentException {

        boolean changedAny = false;
        LinkedHashSet<PlanNode> joins = new LinkedHashSet<PlanNode>(NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS));
        while (!joins.isEmpty()) {
            Iterator<PlanNode> i = joins.iterator();
            PlanNode join = i.next();
            i.remove();
            if (!join.getProperty(Info.JOIN_TYPE).equals(JoinType.JOIN_LEFT_OUTER) || join.hasBooleanProperty(Info.PRESERVE)) {
                continue;
            }
            PlanNode childJoin = null;
            PlanNode other = null;
            PlanNode left = join.getFirstChild();
            PlanNode right = join.getLastChild();

            if (left.getType() == NodeConstants.Types.JOIN && left.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER) {
                childJoin = left;
                other = right;
            } else if (right.getType() == NodeConstants.Types.JOIN && (right.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER || right.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_INNER)) {
                childJoin = right;
                other = left;
            } else {
                continue;
            }

            PlanNode cSource = other;

            if (cSource.getType() != NodeConstants.Types.ACCESS) {
                continue;
            }

            List<Criteria> joinCriteria = (List<Criteria>) join.getProperty(Info.JOIN_CRITERIA);
            if (!isCriteriaValid(joinCriteria, metadata, join)) {
                continue;
            }

            List<Criteria> childJoinCriteria = (List<Criteria>) childJoin.getProperty(Info.JOIN_CRITERIA);
            if (!isCriteriaValid(childJoinCriteria, metadata, childJoin) || childJoin.hasBooleanProperty(Info.PRESERVE)) {
                continue;
            }

            //there are 4 forms we can take
            // (a b) c -> a (b c) or (a c) b
            // c (b a) -> (c b) a or (c a) b
            Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(joinCriteria);
            if (Collections.disjoint(groups, FrameUtil.findJoinSourceNode(childJoin == left?childJoin.getFirstChild():childJoin.getLastChild()).getGroups())) {
                //case where absolute order remains the same
                PlanNode bSource = childJoin == left?childJoin.getLastChild():childJoin.getFirstChild();
                if (bSource.getType() != NodeConstants.Types.ACCESS) {
                    continue;
                }
                Object modelId = RuleRaiseAccess.canRaiseOverJoin(childJoin == left?Arrays.asList(bSource, cSource):Arrays.asList(cSource, bSource), metadata, capabilitiesFinder, joinCriteria, JoinType.JOIN_LEFT_OUTER, analysisRecord, context, false, false);
                if (modelId == null) {
                    continue;
                }
                //rearrange
                PlanNode newParent = RulePlanJoins.createJoinNode();
                newParent.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
                PlanNode newChild = RulePlanJoins.createJoinNode();
                newChild.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
                joins.remove(childJoin);
                if (childJoin == left) {
                    //a (b c)
                    //TODO: this case can probably be eliminated as it will be handled
                    //in BeforePlanning or during general join planning
                    newChild.addFirstChild(childJoin.getLastChild());
                    //promote the hints to the new parent
                    //so that makedep b can be honored
                    RulePlaceAccess.copyProperties(newChild.getFirstChild(), newChild);
                    newChild.addLastChild(other);
                    newChild.setProperty(Info.JOIN_CRITERIA, joinCriteria);
                    newParent.addFirstChild(childJoin.getFirstChild());
                    newParent.addLastChild(newChild);
                    newParent.setProperty(Info.JOIN_CRITERIA, childJoinCriteria);
                } else {
                    //(c b) a
                    newChild.addFirstChild(other);
                    newChild.addLastChild(childJoin.getFirstChild());
                    newChild.setProperty(Info.JOIN_CRITERIA, joinCriteria);
                    newParent.addFirstChild(newChild);
                    newParent.addLastChild(childJoin.getLastChild());
                    newParent.setProperty(Info.JOIN_CRITERIA, childJoinCriteria);
                }
                updateGroups(newChild);
                updateGroups(newParent);
                join.getParent().replaceChild(join, newParent);
                if(RuleRaiseAccess.checkConformedSubqueries(newChild.getFirstChild(), newChild, true)) {
                    RuleRaiseAccess.raiseAccessOverJoin(newChild, newChild.getFirstChild(), modelId, capabilitiesFinder, metadata, true);
                    changedAny = true;
                }
            } else if (Collections.disjoint(groups, FrameUtil.findJoinSourceNode(childJoin == right?childJoin.getFirstChild():childJoin.getLastChild()).getGroups())) {
                PlanNode aSource = childJoin == left?childJoin.getFirstChild():childJoin.getLastChild();
                if (aSource.getType() != NodeConstants.Types.ACCESS) {
                    continue;
                }

                if (!join.getExportedCorrelatedReferences().isEmpty()) {
                    //TODO: we are not really checking that specifically
                    continue;
                }
                Object modelId = RuleRaiseAccess.canRaiseOverJoin(childJoin == left?Arrays.asList(aSource, cSource):Arrays.asList(cSource, aSource), metadata, capabilitiesFinder, joinCriteria, JoinType.JOIN_LEFT_OUTER, analysisRecord, context, false, false);
                if (modelId == null) {
                    continue;
                }

                //rearrange
                PlanNode newParent = RulePlanJoins.createJoinNode();
                newParent.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
                PlanNode newChild = RulePlanJoins.createJoinNode();
                newChild.setProperty(Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
                joins.remove(childJoin);

                if (childJoin == left) {
                    newChild.addFirstChild(childJoin.getFirstChild());
                    newChild.addLastChild(other);
                    newParent.addLastChild(childJoin.getLastChild());
                } else {
                    newChild.addFirstChild(other);
                    newChild.addLastChild(childJoin.getLastChild());
                    newParent.addLastChild(childJoin.getFirstChild());
                }
                newChild.addGroups(newChild.getFirstChild().getGroups());
                newChild.setProperty(Info.JOIN_CRITERIA, joinCriteria);
                newParent.addFirstChild(newChild);
                newParent.setProperty(Info.JOIN_CRITERIA, childJoinCriteria);
                updateGroups(newChild);
                updateGroups(newParent);
                join.getParent().replaceChild(join, newParent);
                if(RuleRaiseAccess.checkConformedSubqueries(newChild.getFirstChild(), newChild, true)) {
                    RuleRaiseAccess.raiseAccessOverJoin(newChild, newChild.getFirstChild(), modelId, capabilitiesFinder, metadata, true);
                    changedAny = true;
                }
            }
        }
        return changedAny;
    }

    private void updateGroups(PlanNode node) {
        node.addGroups(GroupsUsedByElementsVisitor.getGroups(node.getCorrelatedReferenceElements()));
        node.addGroups(FrameUtil.findJoinSourceNode(node.getFirstChild()).getGroups());
        node.addGroups(FrameUtil.findJoinSourceNode(node.getLastChild()).getGroups());
    }

    private boolean isCriteriaValid(List<Criteria> joinCriteria, QueryMetadataInterface metadata, PlanNode join) {
        if (joinCriteria.isEmpty()) {
            return false;
        }
        Set<GroupSymbol> groups = join.getGroups();
        for (Criteria crit : joinCriteria) {
            if (JoinUtil.isNullDependent(metadata, groups, crit)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Similar to {@link #planLeftOuterJoinAssociativity(PlanNode, QueryMetadataInterface, CapabilitiesFinder, AnalysisRecord, CommandContext)},
     * but only looks for the creation of inner joins
     * @param plan
     * @param metadata
     * @return
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    private boolean planLeftOuterJoinAssociativityBeforePlanning(PlanNode plan,
            QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder,
            AnalysisRecord analysisRecord, CommandContext context) throws QueryMetadataException, TeiidComponentException {

        boolean changedAny = false;
        LinkedHashSet<PlanNode> joins = new LinkedHashSet<PlanNode>(NodeEditor.findAllNodes(plan, NodeConstants.Types.JOIN, NodeConstants.Types.ACCESS));
        while (!joins.isEmpty()) {
            Iterator<PlanNode> i = joins.iterator();
            PlanNode join = i.next();
            i.remove();

            if (join.hasBooleanProperty(Info.PRESERVE)) {
                continue;
            }

            //check for left outer join ordering, such that we can combine for pushdown
            boolean val = checkLeftOrdering(metadata, capabilitiesFinder, analysisRecord,
                    context, join);

            //we don't need to do further reordering at this point as it can be handled after join planning
            changedAny |= val;
        }
        return changedAny;
    }

    /**
     * Check if the current join spans inner/left outer joins, such that
     * a different tree structure would group the join for pushing
     *
     * TODO: this is not tolerant to more complex left nesting structures
     *
     * @param metadata
     * @param capabilitiesFinder
     * @param analysisRecord
     * @param context
     * @param join
     * @return true if the current join has been restructured
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private boolean checkLeftOrdering(QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder,
            AnalysisRecord analysisRecord, CommandContext context,
            PlanNode join)
            throws QueryMetadataException, TeiidComponentException {
        if (join.getFirstChild().getType() != NodeConstants.Types.JOIN || !(join.getFirstChild().getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER || join.getFirstChild().getProperty(Info.JOIN_TYPE) == JoinType.JOIN_INNER)) {
            return false;
        }
        if (RulePlanUnions.getModelId(metadata, NodeEditor.findAllNodes(join, NodeConstants.Types.ACCESS), capabilitiesFinder) != null) {
            //already grouped, we can't further optimize here
            return false;
        }
        PlanNode childJoin = null;
        PlanNode left = join.getFirstChild();
        PlanNode right = join.getLastChild();

        boolean nested = false;
        boolean hasOuter = join.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER;
        childJoin = left;
        while (childJoin.getFirstChild() != null && childJoin.getFirstChild().getType() != NodeConstants.Types.ACCESS) {
            if (childJoin.getType() != NodeConstants.Types.JOIN || !(childJoin.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER || childJoin.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_INNER)) {
                return false;
            }
            hasOuter |= childJoin.getProperty(Info.JOIN_TYPE) == JoinType.JOIN_LEFT_OUTER;
            List<Criteria> childJoinCriteria = (List<Criteria>) childJoin.getProperty(Info.JOIN_CRITERIA);
            if (!isCriteriaValid(childJoinCriteria, metadata, childJoin)) {
                return false;
            }
            childJoin = childJoin.getFirstChild();
            left = childJoin;
            nested = true;
        }
        if (nested && hasOuter && !left.hasBooleanProperty(Info.PRESERVE)) {
            if (right.getType() != NodeConstants.Types.ACCESS) {
                return false;
            }
            List<Criteria> joinCriteria = (List<Criteria>) join.getProperty(Info.JOIN_CRITERIA);
            if (!isCriteriaValid(joinCriteria, metadata, join)) {
                return false;
            }
            joinCriteria = new ArrayList<>(joinCriteria);
            RuleChooseJoinStrategy.filterOptionalCriteria(joinCriteria, false);
            Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(joinCriteria);
            if (groups.containsAll(left.getFirstChild().getGroups())
                    && groups.containsAll(right.getGroups())
                    && groups.size() == left.getFirstChild().getGroups().size() + right.getGroups().size()) {
                Object modelId = RuleRaiseAccess.canRaiseOverJoin(Arrays.asList(left.getFirstChild(), right), metadata, capabilitiesFinder, joinCriteria, JoinType.JOIN_LEFT_OUTER, analysisRecord, context, false, false);
                if (modelId == null) {
                    return false;
                }
                PlanNode parent = join.getParent();
                parent.replaceChild(join, join.getFirstChild());
                join.removeAllChildren();
                left.getFirstChild().addAsParent(join);
                join.addLastChild(right);
                join.getGroups().clear();
                updateGroups(join);
                //update left and parents to pick up new groups
                PlanNode toCorrect = left;
                while (toCorrect != parent) {
                    updateGroups(toCorrect);
                    toCorrect = toCorrect.getParent();
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "PlanOuterJoins"; //$NON-NLS-1$
    }

}

