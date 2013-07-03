/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
		boolean pushAssignOutputElements = false;
		try {
			for (PlanNode sourceNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
				GroupSymbol group = sourceNode.getGroups().iterator().next();
				if (!RowBasedSecurityHelper.applyRowSecurity(metadata, group, context)) {
					continue;
				}
				List<ElementSymbol> cols = (List<ElementSymbol>) sourceNode.getProperty(Info.OUTPUT_COLS);
				Command command = (Command) sourceNode.getProperty(Info.VIRTUAL_COMMAND);
				if (group.isProcedure()) {
					if (command == null) {
						continue; //proc relational, will instead apply at the proc level
					}
					if (cols == null) {
						cols = (List)command.getProjectedSymbols();
					}
				} else if (command != null && !command.returnsResultSet()) {
					continue; //should be handled in the planner
				}
				if (cols == null) {
		        	cols = ResolverUtil.resolveElementsInGroup(group, metadata);
				}
				
				//apply masks first
		        List<? extends Expression> masked = ColumnMaskingHelper.maskColumns(cols, group, metadata, context);
		        Map<ElementSymbol, Expression> mapping = null;
		        //TODO: we don't actually allow window function masks yet becuase they won't pass
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
		        	mapping.put(cols.get(i), maskedCol);
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
	        			rules.getPlanner().planSubqueries(null, null, sourceNode.getGroups(), project, project.getSubqueryContainers(), true);
	        			project.addGroups(GroupsUsedByElementsVisitor.getGroups(project.getCorrelatedReferenceElements()));
	        			if (!group.isProcedure()) {
	        				//we need to insert a view to give a single place to evaluate the subqueries
	        				PlanNode root = sourceNode;
			            	if (sourceNode.getParent().getType() == NodeConstants.Types.ACCESS) {
			            		root = sourceNode.getParent();
			            	}
	        				root.addAsParent(project);
	        				addView(metadata, context, group, cols, masked, project);
	        				pushAssignOutputElements = true;
	        				parentJoin = null;
	        			}
		        	}
	        		if (!windowFunctions.isEmpty() && project != null) {
	        			project.setProperty(Info.HAS_WINDOW_FUNCTIONS, true);
	        		}
		        }
		        
	            //logically filters are applied below masking
		        Criteria filter = RowBasedSecurityHelper.getRowBasedFilters(metadata, group, context, false);
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
                    rules.getPlanner().planSubqueries(null, null, sourceNode.getGroups(), critNode, critNode.getSubqueryContainers(), true);
                    critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(critNode.getCorrelatedReferenceElements()));
                    root.addAsParent(critNode);
                }
        	    if (!RuleMergeVirtual.checkJoinCriteria(parent, group, parentJoin)) {
        	    	PlanNode project = RelationalPlanner.createProjectNode(cols);
        	    	parent.addAsParent(project);
        	    	//a view is needed to keep the logical placement of the criteria
    				addView(metadata, context, group, cols, cols, project);
    				pushAssignOutputElements = true;
        	    }
			}
		} catch (TeiidProcessingException e) {
			throw new QueryPlannerException(e);
		}
		if (pushAssignOutputElements) {
			rules.push(new RuleAssignOutputElements(false));
		}
		return plan;
	}

	private void addView(QueryMetadataInterface metadata,
			CommandContext context, GroupSymbol group,
			List<ElementSymbol> cols, List<? extends Expression> old,
			PlanNode viewRoot) throws TeiidComponentException,
			QueryMetadataException, QueryPlannerException {
		GroupSymbol securityVeiw = new GroupSymbol("sec"); //$NON-NLS-1$
		Set<String> groups = context.getGroups();
		securityVeiw = RulePlaceAccess.recontextSymbol(securityVeiw, groups);
		List<ElementSymbol> newCols = RulePushAggregates.defineNewGroup(securityVeiw, old, metadata);
		PlanNode newSourceNode = RuleDecomposeJoin.createSource(securityVeiw, viewRoot, newCols);
		Map<ElementSymbol, Expression> upperMapping = SymbolMap.createSymbolMap(cols, newCols).asMap();
		FrameUtil.convertFrame(newSourceNode.getParent(), group, Collections.singleton(securityVeiw), upperMapping, metadata);
	}

}
