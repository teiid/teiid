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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.Policy;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.ColumnMaskingHelper;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.RowBasedSecurityHelper;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;

/**
 * Applies row/column security to a non-update plan
 *
 * Should be run after rule assign output elements
 */
public class RuleApplySecurity implements OptimizerRule {

    @Override
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
            CapabilitiesFinder capabilitiesFinder, RuleStack rules,
            AnalysisRecord analysisRecord, CommandContext context)
            throws QueryPlannerException, QueryMetadataException,
            TeiidComponentException {
        try {
            for (PlanNode sourceNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
                GroupSymbol group = sourceNode.getGroups().iterator().next();
                if (!RowBasedSecurityHelper.applyRowSecurity(metadata, group, context)) {
                    continue;
                }
                List<ElementSymbol> cols = null;
                Command command = (Command) sourceNode.getProperty(Info.VIRTUAL_COMMAND);
                if (group.isProcedure()) {
                    if (command == null) {
                        continue; //proc relational, will instead apply at the proc level
                    }
                    cols = (List)command.getProjectedSymbols();
                } else if (command != null && !command.returnsResultSet()) {
                    continue; //should be handled in the planner
                }
                if (cols == null) {
                    cols = ResolverUtil.resolveElementsInGroup(group, metadata);
                }

                //apply masks first
                List<? extends Expression> masked = ColumnMaskingHelper.maskColumns(cols, group, metadata, context);
                Map<ElementSymbol, Expression> mapping = null;
                //TODO: we don't actually allow window function masks yet because they won't pass
                //validation.  but if we do, we need to check for them here
                List<WindowFunction> windowFunctions = new ArrayList<WindowFunction>(2);
                for (int i = 0; i < masked.size(); i++) {
                    Expression maskedCol = masked.get(i);
                    AggregateSymbolCollectorVisitor.getAggregates(maskedCol, null, null, null, windowFunctions, null);
                    if (maskedCol.equals(cols.get(i))) {
                        continue;
                    }
                    if (mapping == null) {
                        mapping = new HashMap<ElementSymbol, Expression>();
                    }
                    mapping.put(cols.get(i), SymbolMap.getExpression(maskedCol));
                }
                PlanNode parentJoin = NodeEditor.findParent(sourceNode.getParent(), NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
                if (mapping != null) {
                    //some element symbol has been replaced
                    PlanNode project = null;
                    if (group.isProcedure()) {
                        project = NodeEditor.findParent(sourceNode, NodeConstants.Types.PROJECT);
                        project.setProperty(NodeConstants.Info.PROJECT_COLS, masked);
                    }
                    if (windowFunctions.isEmpty() && RuleMergeVirtual.checkProjectedSymbols(group, parentJoin, metadata, masked, Collections.singleton(group), true)){
                        if (!group.isProcedure()) {
                            //just upwardly project - TODO: we could also handle some subquery simple projection situations here
                            FrameUtil.convertFrame(sourceNode.getParent(), group, Collections.singleton(group), mapping, metadata);
                        }
                    } else {
                        if (!group.isProcedure()) {
                            project = RelationalPlanner.createProjectNode(masked);
                        }
                        rules.getPlanner().planSubqueries(sourceNode.getGroups(), project, project.getSubqueryContainers(), true, false);
                        project.addGroups(GroupsUsedByElementsVisitor.getGroups(project.getCorrelatedReferenceElements()));
                        if (!group.isProcedure()) {
                            //we need to insert a view to give a single place to evaluate the subqueries
                            PlanNode root = sourceNode;
                            if (sourceNode.getParent().getType() == NodeConstants.Types.ACCESS) {
                                root = sourceNode.getParent();
                            }
                            root.addAsParent(project);
                            addView(metadata, context, group, cols, project);
                            parentJoin = null;
                        }
                    }
                    if (!windowFunctions.isEmpty() && project != null) {
                        project.setProperty(Info.HAS_WINDOW_FUNCTIONS, true);
                    }
                }

                //logically filters are applied below masking
                Criteria filter = RowBasedSecurityHelper.getRowBasedFilters(metadata, group, context, false, Policy.Operation.SELECT);
                if (filter == null) {
                    continue;
                }
                List<Criteria> crits = Criteria.separateCriteriaByAnd(filter);
                PlanNode root = sourceNode;
                if (sourceNode.getParent().getType() == NodeConstants.Types.ACCESS) {
                    root = sourceNode.getParent();
                }
                PlanNode parent = null;
                for (Criteria crit : crits) {
                    PlanNode critNode = RelationalPlanner.createSelectNode(crit, false);
                    if (parent == null) {
                        parent = critNode;
                    }
                    rules.getPlanner().planSubqueries(sourceNode.getGroups(), critNode, critNode.getSubqueryContainers(), true, false);
                    critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(critNode.getCorrelatedReferenceElements()));
                    root.addAsParent(critNode);
                }
                if (!RuleMergeVirtual.checkJoinCriteria(parent, group, parentJoin, metadata)) {
                    PlanNode project = RelationalPlanner.createProjectNode(cols);
                    parent.addAsParent(project);
                    //a view is needed to keep the logical placement of the criteria
                    addView(metadata, context, group, cols, project);
                }
            }
        } catch (TeiidProcessingException e) {
            throw new QueryPlannerException(e);
        }
        return plan;
    }

    private void addView(QueryMetadataInterface metadata,
            CommandContext context, GroupSymbol group,
            List<ElementSymbol> cols, PlanNode viewRoot) throws TeiidComponentException,
            QueryMetadataException, QueryPlannerException {
        GroupSymbol securityVeiw = new GroupSymbol("sec"); //$NON-NLS-1$
        Set<String> groups = context.getGroups();
        securityVeiw = RulePlaceAccess.recontextSymbol(securityVeiw, groups);
        List<ElementSymbol> newCols = RulePushAggregates.defineNewGroup(securityVeiw, cols, metadata);
        PlanNode newSourceNode = RuleDecomposeJoin.createSource(securityVeiw, viewRoot, newCols);
        Map<ElementSymbol, Expression> upperMapping = SymbolMap.createSymbolMap(cols, newCols).asMap();
        FrameUtil.convertFrame(newSourceNode.getParent(), group, Collections.singleton(securityVeiw), upperMapping, metadata);
    }

    @Override
    public String toString() {
        return "ApplySecurity"; //$NON-NLS-1$
    }

}
