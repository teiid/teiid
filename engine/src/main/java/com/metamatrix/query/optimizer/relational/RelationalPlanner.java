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

package com.metamatrix.query.optimizer.relational;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.CommandPlanner;
import com.metamatrix.query.optimizer.CommandTreeNode;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.optimizer.relational.rules.RuleConstants;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.LogConstants;

/**
 * This class generates a relational plan for query execution.  The output of
 * this class is a {@link com.metamatrix.query.optimizer.relational.plantree.PlanNode PlanNode}
 * object - this object then becomes the input to
 * {@link PlanToProcessConverter PlanToProcessConverter}
 * to  produce a
 * {@link com. metamatrix.query.processor.relational.RelationalPlan RelationalPlan}.
 */
public class RelationalPlanner implements CommandPlanner {

    /**
	 * Key for a {@link PlanHints PlanHints} object 
	 */
	public static final Integer HINTS = Integer.valueOf(0);
	public static final Integer VARIABLE_CONTEXTS = Integer.valueOf(1);

	/**
     * @see com.metamatrix.query.optimizer.CommandPlanner#generateCanonical(com.metamatrix.query.optimizer.CommandTreeNode, boolean)
     */
    public void generateCanonical(CommandTreeNode node, QueryMetadataInterface metadata, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        // Create canonical plan for this command
        Command command = node.getCommand();

        PlanHints hints = (PlanHints)node.getProperty(RelationalPlanner.HINTS);
        PlanNode canonicalPlan = GenerateCanonical.generatePlan(command, hints, metadata);
        node.setCanonicalPlan(canonicalPlan);
    }

    /**
     * @see com.metamatrix.query.optimizer.CommandPlanner#optimize(com.metamatrix.query.optimizer.CommandTreeNode, java.util.Map, com.metamatrix.query.metadata.QueryMetadataInterface, boolean)
     */
    public ProcessorPlan optimize(
        CommandTreeNode node,
        IDGenerator idGenerator,
        QueryMetadataInterface metadata,
        CapabilitiesFinder capFinder,
        AnalysisRecord analysisRecord, CommandContext context)
        throws
            QueryPlannerException,
            QueryMetadataException,
            MetaMatrixComponentException {


        // Distribute childPlans into relational plan
        PlanNode plan = (PlanNode)node.getCanonicalPlan();
        connectChildPlans(plan);

        // Check whether command has virtual groups
        Command command = node.getCommand();
        PlanHints hints = (PlanHints)node.getProperty(RelationalPlanner.HINTS);
        RelationalPlanner.checkForVirtualGroups(command, hints, metadata);

        // Distribute make dependent hints as necessary
        if(hints.makeDepGroups != null) {
            RelationalPlanner.distributeDependentHints(hints.makeDepGroups, plan, metadata, NodeConstants.Info.MAKE_DEP);
        }
        if (hints.makeNotDepGroups != null) {
            RelationalPlanner.distributeDependentHints(hints.makeNotDepGroups, plan, metadata, NodeConstants.Info.MAKE_NOT_DEP);
        }

        // Connect ProcessorPlan to SubqueryContainer (if any) of SELECT or PROJECT nodes
    	if (node.getChildCount() > 0) {
    		connectSubqueryContainers(plan);
    	}
        
        // Set top column information on top node
        List projectCols = command.getProjectedSymbols();
        List<SingleElementSymbol> topCols = new ArrayList<SingleElementSymbol>(projectCols.size());
        Iterator projectIter = projectCols.iterator();
        while(projectIter.hasNext()) {
            SingleElementSymbol symbol = (SingleElementSymbol) projectIter.next();
            topCols.add( (SingleElementSymbol)symbol.clone() );
        }

        // Build rule set based on hints
        RuleStack rules = RelationalPlanner.buildRules(hints);

        // Run rule-based optimizer
        plan = RelationalPlanner.executeRules(rules, plan, metadata, capFinder, analysisRecord, context);
        node.setCanonicalPlan(plan);

        PlanToProcessConverter planToProcessConverter = null;
        if (context != null) {
        	planToProcessConverter = context.getPlanToProcessConverter();
        }
        if (planToProcessConverter == null) {
        	planToProcessConverter = new PlanToProcessConverter(metadata, idGenerator, analysisRecord, capFinder);
        }
        
        RelationalPlan result = planToProcessConverter.convert(plan);
        
        result.setOutputElements(topCols);
        
        return result;
    }

    private static void connectSubqueryContainers(PlanNode plan) {
        Set<GroupSymbol> groupSymbols = getGroupSymbols(plan);

        for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.PROJECT | NodeConstants.Types.SELECT | NodeConstants.Types.JOIN)) {
            List<SubqueryContainer> subqueryContainers = node.getSubqueryContainers();
            if (subqueryContainers.isEmpty()){
            	continue;
            }
            Set<GroupSymbol> localGroupSymbols = groupSymbols;
            if (node.getType() == NodeConstants.Types.JOIN) {
            	localGroupSymbols = getGroupSymbols(node);
            }
            for (SubqueryContainer container : subqueryContainers) {
                ArrayList<Reference> correlatedReferences = new ArrayList<Reference>(); 
                Command subCommand = container.getCommand();
                CorrelatedReferenceCollectorVisitor.collectReferences(subCommand, localGroupSymbols, correlatedReferences);
                if (!correlatedReferences.isEmpty()) {
	                SymbolMap map = new SymbolMap();
	                for (Reference reference : correlatedReferences) {
	    				map.addMapping(reference.getExpression(), reference.getExpression());
	    			}
	                subCommand.setCorrelatedReferences(map);
                }
            }
            node.addGroups(GroupsUsedByElementsVisitor.getGroups(node.getCorrelatedReferenceElements()));
        }
    }

	private static Set<GroupSymbol> getGroupSymbols(PlanNode plan) {
		Set<GroupSymbol> groupSymbols = new HashSet<GroupSymbol>();
        for (PlanNode source : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
            groupSymbols.addAll(source.getGroups());
        }
		return groupSymbols;
	}

    /**
     * Method connectChildPlans.
     * @param plan
     * @param childPlans
     * @param metadata
     * @return Map <Command - child ProcessorPlan>
     */
    private static void connectChildPlans(PlanNode plan) {
        // Find all nodes that need subcommands attached
        for (PlanNode source : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
            Command nodeCommand = (Command)source.getProperty(NodeConstants.Info.NESTED_COMMAND);
            if(nodeCommand != null && nodeCommand.getProcessorPlan() != null) {
                source.setProperty(NodeConstants.Info.PROCESSOR_PLAN, nodeCommand.getProcessorPlan());
            }
        }
    }

    /**
     * Look for any virtual groups in the user's command.  If some exist, then
     * set the hint for virtual groups to true.  Otherwise, leave the hint at it's
     * default value which is false.  This allows the buildRules() method later to
     * leave out a bunch of rules relating to virtual groups if none were used.
     * @param command Command to check
     * @param hints Hints to update if virtual groups are used in plan
     */
    private static void checkForVirtualGroups(Command command, PlanHints hints, QueryMetadataInterface metadata)
    throws QueryMetadataException, MetaMatrixComponentException {
        Collection groups = GroupCollectorVisitor.getGroups(command, true);
        Iterator groupIter = groups.iterator();
        while (groupIter.hasNext()) {
            GroupSymbol group = (GroupSymbol) groupIter.next();
            if( metadata.isVirtualGroup(group.getMetadataID()) ) {
                hints.hasVirtualGroups = true;
                break;
            }
        }
    }

    /**
     * Distribute and "make (not) dependent" hints specified in the query into the
     * fully resolved query plan.  This is done after virtual group resolution so
     * that all groups in the plan are known.  The hint is attached to all SOURCE
     * nodes for each group that should be made dependent/not dependent.
     * @param groups List of groups (Strings) to be made dependent
     * @param plan The canonical plan
     */
    private static void distributeDependentHints(Collection groups, PlanNode plan, QueryMetadataInterface metadata, NodeConstants.Info hintProperty)
        throws QueryMetadataException, MetaMatrixComponentException {
    
        if(groups != null && groups.size() > 0) {
            // Get all source nodes
            List nodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE);
    
            // Walk through each dependent group hint and
            // attach to the correct source node
            Iterator groupIter = groups.iterator();
            while(groupIter.hasNext()) {
                String groupName = (String) groupIter.next();
    
                // Walk through nodes and apply hint to all that match group name
                boolean appliedHint = applyHint(nodes, groupName, hintProperty);
    
                if(! appliedHint) {
                    //check if it is partial group name
                    Collection groupNames = metadata.getGroupsForPartialName(groupName);
                    if(groupNames.size() == 1) {
                        groupName = (String)groupNames.iterator().next();
                        appliedHint = applyHint(nodes, groupName, hintProperty);
                    }
                    
                    if(! appliedHint) {
                        LogManager.logWarning(LogConstants.CTX_QUERY_PLANNER, QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0010, groupName));
                    }
                }
            }
        }
    }
    
    private static boolean applyHint(List nodes, String groupName, NodeConstants.Info hintProperty) {
        boolean appliedHint = false;
        Iterator nodeIter = nodes.iterator();
        while(nodeIter.hasNext()) {
            PlanNode node = (PlanNode) nodeIter.next();
            GroupSymbol nodeGroup = node.getGroups().iterator().next();
            
            String sDefinition = nodeGroup.getDefinition();
            
            if (nodeGroup.getName().equalsIgnoreCase(groupName) 
             || (sDefinition != null && sDefinition.equalsIgnoreCase(groupName)) ) {
                node.setProperty(hintProperty, Boolean.TRUE);
                appliedHint = true;
            }
        }
        return appliedHint;
    }

    public static RuleStack buildRules(PlanHints hints) {
        RuleStack rules = new RuleStack();

        rules.push(RuleConstants.COLLAPSE_SOURCE);
        
        rules.push(RuleConstants.PLAN_SORTS);

        if(hints.hasJoin) {
            rules.push(RuleConstants.IMPLEMENT_JOIN_STRATEGY);
        }
        
        rules.push(RuleConstants.ASSIGN_OUTPUT_ELEMENTS);
        
        rules.push(RuleConstants.CALCULATE_COST);
        
        if (hints.hasLimit) {
            rules.push(RuleConstants.PUSH_LIMIT);
        }
        if (hints.hasJoin || hints.hasCriteria) {
            rules.push(RuleConstants.MERGE_CRITERIA);
        }
        if (hints.hasRelationalProc) {
            rules.push(RuleConstants.PLAN_PROCEDURES);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_DEPENDENT);
        }
        if(hints.hasAggregates) {
            rules.push(RuleConstants.PUSH_AGGREGATES);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_JOIN_STRATEGY);
            rules.push(RuleConstants.RAISE_ACCESS);
            //after planning the joins, let the criteria be pushed back into place
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
            rules.push(RuleConstants.PLAN_JOINS);
        }
        rules.push(RuleConstants.RAISE_ACCESS);
        if (hints.hasSetQuery) {
            rules.push(RuleConstants.PLAN_UNIONS);
        } 
        if(hints.hasCriteria || hints.hasJoin) {
            //after copy criteria, it is no longer necessary to have phantom criteria nodes, so do some cleaning
            rules.push(RuleConstants.CLEAN_CRITERIA);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.COPY_CRITERIA);
            rules.push(RuleConstants.PUSH_NON_JOIN_CRITERIA);
        }
        if(hints.hasVirtualGroups) {
            rules.push(RuleConstants.MERGE_VIRTUAL);
        }
        if(hints.hasCriteria) {
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }
        if (hints.hasJoin && hints.hasOptionalJoin) {
            rules.push(RuleConstants.REMOVE_OPTIONAL_JOINS);
        }
        rules.push(RuleConstants.PLACE_ACCESS);
        return rules;
    }

    private static PlanNode executeRules(RuleStack rules, PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        boolean debug = analysisRecord.recordDebug();
        while(! rules.isEmpty()) {
            if(debug) {
                analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
            }

            OptimizerRule rule = rules.pop();
            if(debug) {
                analysisRecord.println("EXECUTING " + rule); //$NON-NLS-1$
            }

            plan = rule.execute(plan, metadata, capFinder, rules, analysisRecord, context);
            if(debug) {
                analysisRecord.println("\nAFTER: \n" + plan); //$NON-NLS-1$
            }
        }
        return plan;
    }
}
