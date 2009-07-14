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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants.Info;
import com.metamatrix.query.processor.relational.JoinNode.JoinStrategyType;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.AggregateSymbolCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

public final class GenerateCanonical {

	/**
	 * Generate an initial plan given a command.  This plan will contain all necessary
	 * information from the command but will not be optimized or even executable.
	 * @param command Command to plan
	 * @param metadata Metadata access
	 * @param debug True if debugging information should be dumped to STDOUT
	 * @return Initial query plan tree
	 */
	public static PlanNode generatePlan(Command command, PlanHints hints, QueryMetadataInterface metadata)
	    throws QueryPlannerException, MetaMatrixComponentException {
		if( command.getType() == Command.TYPE_QUERY ) {

			return GenerateCanonical.createQueryPlan(command, hints, metadata);

		} else if( command.getType() == Command.TYPE_INSERT ||
					command.getType() == Command.TYPE_UPDATE ||
					command.getType() == Command.TYPE_DELETE ||
                    command.getType() == Command.TYPE_CREATE ||
                    command.getType() == Command.TYPE_DROP            
        ) {

			// update PlanHints to note that it is an update
			hints.isUpdate = true;

			return GenerateCanonical.createUpdatePlan(command, hints);

		} else if( command.getType() == Command.TYPE_STORED_PROCEDURE) {

			return GenerateCanonical.createStoredProcedurePlan(command, hints);

		} else {
            throw new QueryPlannerException(QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0005, command.getClass().getName()));
		}
	}

	private GenerateCanonical() { }

	static PlanNode createUpdatePlan(Command command, PlanHints hints) {

        // Create top project node - define output columns for stored query / procedure
        PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);

        Collection groups = GroupCollectorVisitor.getGroups(command, false);
        projectNode.addGroups(groups);

        // Set output columns
        List cols = command.getProjectedSymbols();
        projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, cols);

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.ATOMIC_REQUEST, command);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, command);
        List subCommands = command.getSubCommands();
        if (subCommands.size() == 1) {
            sourceNode.setProperty(NodeConstants.Info.NESTED_COMMAND, subCommands.iterator().next());
        }
        sourceNode.addGroups(groups);

        GenerateCanonical.attachLast(projectNode, sourceNode);

        return projectNode;
	}

    static PlanNode createStoredProcedurePlan(Command command, PlanHints hints) {

        StoredProcedure storedProc = (StoredProcedure) command;

        // Create top project node - define output columns for stored query / procedure
        PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);

        // Set output columns
        List cols = storedProc.getProjectedSymbols();
        projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, cols);

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, storedProc);
        if (storedProc.getSubCommand() != null) {
            sourceNode.setProperty(NodeConstants.Info.NESTED_COMMAND, storedProc.getSubCommand());
        }
        
        hints.hasRelationalProc |= storedProc.isProcedureRelational();

        // Set group on source node
        sourceNode.addGroup(storedProc.getGroup());

        GenerateCanonical.attachLast(projectNode, sourceNode);

        return projectNode;
    }

	static PlanNode createQueryPlan(Command command, PlanHints hints, QueryMetadataInterface metadata)
		throws QueryPlannerException, MetaMatrixComponentException {

        // Add make dependent hints from OPTION clause
        QueryCommand qcommand = (QueryCommand) command;
        Option option = qcommand.getOption();
        if(option != null) {
            hints.addMakeDepGroups(option.getDependentGroups());
            hints.addMakeNotDepGroups(option.getNotDependentGroups());
        }

        // Build canonical plan
    	PlanNode node = null;
        if(command instanceof Query) {
            node = createQueryPlan((Query) command, hints, metadata);
        } else {
            node = createQueryPlan((SetQuery) command, hints, metadata);
        }

        return node;
    }

    private static PlanNode createQueryPlan(SetQuery query, PlanHints hints, QueryMetadataInterface metadata)
		throws QueryPlannerException, MetaMatrixComponentException {

        hints.hasSetQuery = true;
        
        PlanNode leftPlan = createQueryPlan( query.getLeftQuery(), hints, metadata);
        PlanNode rightPlan = createQueryPlan( query.getRightQuery(), hints, metadata);

        PlanNode plan = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
        plan.setProperty(NodeConstants.Info.SET_OPERATION, query.getOperation());
        plan.setProperty(NodeConstants.Info.USE_ALL, query.isAll());
        
        GenerateCanonical.attachLast(plan, leftPlan);
        GenerateCanonical.attachLast(plan, rightPlan);

		// Attach sorting on top of union
		if(query.getOrderBy() != null) {
			plan = attachSorting(plan, query.getOrderBy());
            hints.hasSort = true;
		}

        if (query.getLimit() != null) {
            plan = attachTupleLimit(plan, query.getLimit(), hints);
        }
        
        return plan;
    }

    private static PlanNode createQueryPlan(Query query, PlanHints hints, QueryMetadataInterface metadata)
		throws QueryPlannerException, MetaMatrixComponentException {

        PlanNode plan = null;

        if(query.getFrom() != null){
            FromClause fromClause = mergeClauseTrees(query.getFrom());
            
            PlanNode dummyRoot = new PlanNode();
            
    		hints.hasOptionalJoin |= buildTree(fromClause, dummyRoot);
            
            plan = dummyRoot.getFirstChild();
            
            hints.hasJoin |= plan.getType() == NodeConstants.Types.JOIN;

    		// Attach criteria on top
    		if(query.getCriteria() != null) {
    			plan = attachCriteria(plan, query.getCriteria(), false);
                hints.hasCriteria = true;
    		}

    		// Attach grouping node on top
    		if(query.getGroupBy() != null || query.getHaving() != null || !AggregateSymbolCollectorVisitor.getAggregates(query.getSelect(), false).isEmpty()) {
    			plan = attachGrouping(plan, query, hints);
    		}

    		// Attach having criteria node on top
    		if(query.getHaving() != null) {
    			plan = attachCriteria(plan, query.getHaving(), true);
                hints.hasCriteria = true;
    		}
            
        }

		// Attach project on top
		plan = attachProject(plan, query.getSelect());

		// Attach dup removal on top
		if(query.getSelect().isDistinct()) {
			plan = attachDupRemoval(plan);
		}

		// Attach sorting on top
		if(query.getOrderBy() != null) {
			plan = attachSorting(plan, query.getOrderBy());
            hints.hasSort = true;
		}
        
        if (query.getLimit() != null) {
            plan = attachTupleLimit(plan, query.getLimit(), hints);
        }

        //for SELECT INTO, attach source and project nodes
        if(query.getInto() != null){
            // For defect 10976 - find project in current plan and
            // set top columns
            List groups = null;
            PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
                
            GenerateCanonical.attachLast(sourceNode, plan);
            plan = sourceNode;

            if (query.getFrom() != null){
                groups = query.getFrom().getGroups();
                sourceNode.addGroups(groups);
            }

            PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
            List selectCols = query.getProjectedSymbols();
            projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, selectCols);
            projectNode.setProperty(NodeConstants.Info.INTO_GROUP, query.getInto().getGroup());

            if (groups != null){
                projectNode.addGroups(groups);
            }
            GenerateCanonical.attachLast(projectNode, plan);
            plan = projectNode;
        }

		return plan;
	}

    /**
     * Merges the from clause into a single join predicate if there are more than 1 from clauses
     */
    private static FromClause mergeClauseTrees(From from) {
        List clauses = from.getClauses();
        
        while (clauses.size() > 1) {
            FromClause first = (FromClause)from.getClauses().remove(0);
            FromClause second = (FromClause)from.getClauses().remove(0);
            JoinPredicate jp = new JoinPredicate(first, second, JoinType.JOIN_CROSS);
            clauses.add(0, jp);
        }
        
        return (FromClause)clauses.get(0);
    }
    
    /**
     * Build a join plan based on the structure in a clause.  These structures should be
     * essentially the same tree, but with different objects and details.
     * @param clause Clause to build tree from
     * @param parent Parent node to attach join node structure to
     * @param sourceMap Map of group to source node, used for connecting children to join plan
     * @param markJoinsInternal Flag saying whether joins built in this method should be marked
     * as internal
     * @return true if there are optional join nodes
     */
    static boolean buildTree(FromClause clause, PlanNode parent)
        throws QueryPlannerException {
        
        boolean result = false;
        
        PlanNode node = null;
        
        if(clause instanceof UnaryFromClause) {
            // No join required
            UnaryFromClause ufc = (UnaryFromClause)clause;
            GroupSymbol group = ufc.getGroup();
            Command nestedCommand = ufc.getExpandedCommand();
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            node.addGroup(group);
            node.setProperty(NodeConstants.Info.NESTED_COMMAND, nestedCommand);

            parent.addLastChild(node);
        } else if(clause instanceof JoinPredicate) {
            JoinPredicate jp = (JoinPredicate) clause;

            // Set up new join node corresponding to this join predicate
            node = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
            node.setProperty(NodeConstants.Info.JOIN_TYPE, jp.getJoinType());
            node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
            node.setProperty(NodeConstants.Info.JOIN_CRITERIA, jp.getJoinCriteria());
            
            if (jp.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
            	result = true;
            }
         
            // Attach join node to parent
            parent.addLastChild(node);

            // Handle each child
            FromClause[] clauses = new FromClause[] {jp.getLeftClause(), jp.getRightClause()};
            for(int i=0; i<2; i++) {
                result |= buildTree(clauses[i], node);

                // Add groups to joinNode
                for (PlanNode child : node.getChildren()) {
                    node.addGroups(child.getGroups());
                }
            }
        } else if (clause instanceof SubqueryFromClause) {
            SubqueryFromClause sfc = (SubqueryFromClause)clause;
            GroupSymbol group = sfc.getGroupSymbol();
            Command nestedCommand = sfc.getCommand();
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            node.addGroup(group);
            node.setProperty(NodeConstants.Info.NESTED_COMMAND, nestedCommand);
            
            parent.addLastChild(node);
        }
        
        if (clause.isOptional()) {
            node.setProperty(NodeConstants.Info.IS_OPTIONAL, Boolean.TRUE);
            result = true;
        }
        
        if (clause.isMakeDep()) {
            node.setProperty(NodeConstants.Info.MAKE_DEP, Boolean.TRUE);
        } else if (clause.isMakeNotDep()) {
            node.setProperty(NodeConstants.Info.MAKE_NOT_DEP, Boolean.TRUE);
        }

        return result;
    }

	/**
	 * Attach all criteria above the join nodes.  The optimizer will push these
	 * criteria down to the appropriate source.
	 * @param plan Existing plan, which joins all source groups
	 * @param criteria Criteria from query
	 * @return Updated tree
	 */
	private static PlanNode attachCriteria(PlanNode plan, Criteria criteria, boolean isHaving) {
	    List crits = Criteria.separateCriteriaByAnd(criteria);
	    
	    for (final Iterator iterator = crits.iterator(); iterator.hasNext();) {
            final Criteria crit = (Criteria)iterator.next();
            PlanNode critNode = createSelectNode(crit, isHaving);
            GenerateCanonical.attachLast(critNode, plan);
            plan = critNode;
        } 
	    
		return plan;
	}

    public static PlanNode createSelectNode(final Criteria crit, boolean isHaving) {
        PlanNode critNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
        critNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, crit);
        if (isHaving && !AggregateSymbolCollectorVisitor.getAggregates(crit, false).isEmpty()) {
            critNode.setProperty(NodeConstants.Info.IS_HAVING, Boolean.TRUE);
        }
        // Add groups to crit node
        critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(crit));
        critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(critNode.getCorrelatedReferenceElements()));
        return critNode;
    }

	/**
	 * Attach a grouping node at top of tree.
	 * @param plan Existing plan
	 * @param groupBy Group by clause, which may be null
	 * @return Updated plan
	 */
	private static PlanNode attachGrouping(PlanNode plan, Query query, PlanHints hints) {
		PlanNode groupNode = NodeFactory.getNewNode(NodeConstants.Types.GROUP);

		GroupBy groupBy = query.getGroupBy();
		if(groupBy != null) {
			groupNode.setProperty(NodeConstants.Info.GROUP_COLS, groupBy.getSymbols());
            groupNode.addGroups(GroupsUsedByElementsVisitor.getGroups(groupBy));
		}

		GenerateCanonical.attachLast(groupNode, plan);
        
        // Mark in hints
        hints.hasAggregates = true;
        
		return groupNode;
	}

    /**
	 * Attach SORT node at top of tree.  The SORT may be pushed down to a source (or sources)
	 * if possible by the optimizer.
	 * @param plan Existing plan
	 * @param orderBy Sort description from the query
	 * @return Updated plan
	 */
	private static PlanNode attachSorting(PlanNode plan, OrderBy orderBy) {
		PlanNode sortNode = NodeFactory.getNewNode(NodeConstants.Types.SORT);
		
		sortNode.setProperty(NodeConstants.Info.SORT_ORDER, orderBy.getVariables());
		sortNode.setProperty(NodeConstants.Info.ORDER_TYPES, orderBy.getTypes());
		if (orderBy.hasUnrelated()) {
			sortNode.setProperty(Info.UNRELATED_SORT, true);
		}
		sortNode.addGroups(GroupsUsedByElementsVisitor.getGroups(orderBy));

		GenerateCanonical.attachLast(sortNode, plan);
		return sortNode;
	}
    
    private static PlanNode attachTupleLimit(PlanNode plan, Limit limit, PlanHints hints) {
        hints.hasLimit = true;
        PlanNode limitNode = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
        
        boolean attach = false;
        if (limit.getOffset() != null) {
            limitNode.setProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT, limit.getOffset());
            attach = true;
        }
        if (limit.getRowLimit() != null) {
            limitNode.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, limit.getRowLimit());
            attach = true;
        }
        if (attach) {
            GenerateCanonical.attachLast(limitNode, plan);
            plan = limitNode;
        }
        return plan;
    }

	/**
	 * Attach DUP_REMOVE node at top of tree.  The DUP_REMOVE may be pushed down
	 * to a source (or sources) if possible by the optimizer.
	 * @param plan Existing plan
	 * @return Updated plan
	 */
	private static PlanNode attachDupRemoval(PlanNode plan) {
		PlanNode dupNode = NodeFactory.getNewNode(NodeConstants.Types.DUP_REMOVE);
		GenerateCanonical.attachLast(dupNode, plan);
		return dupNode;
	}

	private static PlanNode attachProject(PlanNode plan, Select select) {
		PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
		projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, select.getProjectedSymbols());

		// Set groups
		projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(select));

		GenerateCanonical.attachLast(projectNode, plan);
		return projectNode;
	}

	static final void attachLast(PlanNode parent, PlanNode child) {
		if(child != null) {
			parent.addLastChild(child);
		}
	}

}
