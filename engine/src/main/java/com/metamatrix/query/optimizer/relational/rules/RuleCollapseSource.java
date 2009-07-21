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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants.Info;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.navigator.DeepPostOrderNavigator;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.ExpressionMappingVisitor;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.CommandContext;

public final class RuleCollapseSource implements OptimizerRule {

	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        for (PlanNode accessNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {
            
            // Get nested non-relational plan if there is one
            ProcessorPlan nonRelationalPlan = FrameUtil.getNestedPlan(accessNode);
    		Command command = FrameUtil.getNonQueryCommand(accessNode);

            if(nonRelationalPlan != null) {
                accessNode.setProperty(NodeConstants.Info.PROCESSOR_PLAN, nonRelationalPlan);
            } else { 
                // Create command from access on down and save in access node
                if(command == null) {
                	PlanNode commandRoot = accessNode;
                	GroupSymbol intoGroup = (GroupSymbol)accessNode.getFirstChild().getProperty(NodeConstants.Info.INTO_GROUP);
                	if (intoGroup != null) {
                		commandRoot = NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.SOURCE).getFirstChild();
                	}
                    plan = removeUnnecessaryInlineView(plan, commandRoot);
                    QueryCommand queryCommand = createQuery(metadata, capFinder, accessNode, commandRoot);
                	addSetOpDistinct(metadata, capFinder, accessNode, queryCommand);
                    command = queryCommand;
                    if (intoGroup != null) {
                    	Insert insertCommand = new Insert(intoGroup, ResolverUtil.resolveElementsInGroup(intoGroup, metadata), null);
                    	insertCommand.setQueryExpression(queryCommand);
                    	command = insertCommand;
                    }
                } 
            }
    		accessNode.setProperty(NodeConstants.Info.ATOMIC_REQUEST, command);
    		accessNode.removeAllChildren();
        }
       				
		return plan;
	}

	private void addSetOpDistinct(QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder, PlanNode accessNode,
			QueryCommand queryCommand) throws QueryMetadataException,
			MetaMatrixComponentException {
		if (queryCommand.getLimit() != null && queryCommand.getOrderBy() != null) {
			return; //TODO: could create an inline view
		}
		PlanNode parent = accessNode.getParent();
		boolean dupRemoval = false;
		while (parent != null && parent.getType() == NodeConstants.Types.SET_OP) {
			if (!parent.hasBooleanProperty(NodeConstants.Info.USE_ALL)) {
				dupRemoval = true;
			}
			parent = parent.getParent();
		}
		if (!dupRemoval || NewCalculateCostUtil.usesKey(queryCommand.getProjectedSymbols(), metadata)) {
			return;
		}
		//TODO: we should also order the results and update the set processing logic
		// this requires that we can guarantee null ordering
		if (queryCommand instanceof SetQuery) {
			((SetQuery)queryCommand).setAll(false);
		} else if (CapabilitiesUtil.supports(Capability.QUERY_SELECT_DISTINCT, RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder)) {
			((Query)queryCommand).getSelect().setDistinct(true);
		}
	}

    private PlanNode removeUnnecessaryInlineView(PlanNode root, PlanNode accessNode) {
    	PlanNode child = accessNode.getFirstChild();
        
        if (child.hasBooleanProperty(NodeConstants.Info.INLINE_VIEW)) {
        	child.removeProperty(NodeConstants.Info.INLINE_VIEW);
        	root = RuleRaiseAccess.performRaise(root, child, accessNode);
            //add the groups from the lower project
            accessNode.getGroups().clear();
            PlanNode sourceNode = FrameUtil.findJoinSourceNode(accessNode.getFirstChild());
            if (sourceNode != null) {
                accessNode.addGroups(sourceNode.getGroups());                
            }
            accessNode.setProperty(Info.OUTPUT_COLS, accessNode.getFirstChild().getProperty(Info.OUTPUT_COLS));
        }
        
        return root;
    }

	private QueryCommand createQuery(QueryMetadataInterface metadata, CapabilitiesFinder capFinder, PlanNode accessRoot, PlanNode node) throws QueryMetadataException, MetaMatrixComponentException, QueryPlannerException {
		PlanNode setOpNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
		if (setOpNode != null) {
            Operation setOp = (Operation)setOpNode.getProperty(NodeConstants.Info.SET_OPERATION);
            SetQuery unionCommand = new SetQuery(setOp);
            boolean unionAll = ((Boolean)setOpNode.getProperty(NodeConstants.Info.USE_ALL)).booleanValue();
            unionCommand.setAll(unionAll);
            PlanNode sort = NodeEditor.findNodePreOrder(node, NodeConstants.Types.SORT, NodeConstants.Types.SET_OP);
            if (sort != null) {
                processOrderBy(sort, unionCommand);
            }
            PlanNode limit = NodeEditor.findNodePreOrder(node, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.SET_OP);
            if (limit != null) {
                processLimit(limit, unionCommand);
            }
            int count = 0;
            for (PlanNode child : setOpNode.getChildren()) {
                QueryCommand command = createQuery(metadata, capFinder, accessRoot, child);
                if (count == 0) {
                    unionCommand.setLeftQuery(command);
                } else if (count == 1) {
                    unionCommand.setRightQuery(command);
                } else {
                    unionCommand = new SetQuery(setOp, unionAll, unionCommand, command);
                }
                count++;
            }
            return unionCommand;
        }
		Query query = new Query();
        Select select = new Select();
        List<SingleElementSymbol> columns = (List<SingleElementSymbol>)node.getProperty(NodeConstants.Info.OUTPUT_COLS);
        replaceCorrelatedReferences(ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(columns));
        select.addSymbols(columns);
        query.setSelect(select);
		query.setFrom(new From());
		buildQuery(accessRoot, node, query, metadata, capFinder);
		if (query.getCriteria() instanceof CompoundCriteria) {
            query.setCriteria(QueryRewriter.optimizeCriteria((CompoundCriteria)query.getCriteria()));
        }
		if (!CapabilitiesUtil.useAnsiJoin(RuleRaiseAccess.getModelIDFromAccess(accessRoot, metadata), metadata, capFinder)) {
			simplifyFromClause(query);
        }
		return query;
	}		

    void buildQuery(PlanNode accessRoot, PlanNode node, Query query, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, MetaMatrixComponentException, QueryPlannerException {
        
    	//visit source and join nodes as they appear
        switch(node.getType()) {
            case NodeConstants.Types.JOIN:
            {
                replaceCorrelatedReferences(node.getSubqueryContainers());
                JoinType joinType = (JoinType) node.getProperty(NodeConstants.Info.JOIN_TYPE);
                List<Criteria> crits = (List<Criteria>) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                
                if (crits == null || crits.isEmpty()) {
                    crits = new ArrayList<Criteria>();
                }
                
                PlanNode left = node.getFirstChild();
                PlanNode right = node.getLastChild();

                /* special handling is needed to determine criteria placement.
                 * 
                 * if the join is a left outer join, criteria from the right side will be added to the on clause
                 */
                Criteria savedCriteria = null;
                buildQuery(accessRoot, left, query, metadata, capFinder);
                if (joinType == JoinType.JOIN_LEFT_OUTER) {
                    savedCriteria = query.getCriteria();
                    query.setCriteria(null);
                } 
                buildQuery(accessRoot, right, query, metadata, capFinder);
                if (joinType == JoinType.JOIN_LEFT_OUTER) {
                    moveWhereClauseIntoOnClause(query, crits);
                    query.setCriteria(savedCriteria);
                } 
                
                // Get last two clauses added to the FROM and combine them into a JoinPredicate
                From from = query.getFrom();
                List clauses = from.getClauses();
                int lastClause = clauses.size()-1;
                FromClause clause1 = (FromClause) clauses.get(lastClause-1);
                FromClause clause2 = (FromClause) clauses.get(lastClause);
                 
                //correct the criteria or the join type if necessary
                if (joinType != JoinType.JOIN_CROSS && crits.isEmpty()) {
                    crits.add(QueryRewriter.TRUE_CRITERIA);
                } else if (joinType == JoinType.JOIN_CROSS && !crits.isEmpty()) {
                    joinType = JoinType.JOIN_INNER;
                }
                
                JoinPredicate jp = new JoinPredicate(clause1, clause2, joinType, crits);
                
                // Replace last two clauses with new predicate
                clauses.remove(lastClause);
                clauses.set(lastClause-1, jp);
                return;
            }
            case NodeConstants.Types.SOURCE:
            {
            	if (Boolean.TRUE.equals(node.getProperty(NodeConstants.Info.INLINE_VIEW))) {
                    PlanNode child = node.getFirstChild();
                    QueryCommand newQuery = createQuery(metadata, capFinder, accessRoot, child);
                    
                    //ensure that the group is consistent
                    GroupSymbol symbol = node.getGroups().iterator().next();
                    SubqueryFromClause sfc = new SubqueryFromClause(symbol, newQuery);
                    query.getFrom().addClause(sfc);
                    return;
                } 
                query.getFrom().addGroup(node.getGroups().iterator().next());
                break;
            }
    	}
            
        for (PlanNode childNode : node.getChildren()) {
            buildQuery(accessRoot, childNode, query, metadata, capFinder);              
        }
            
        switch(node.getType()) {
            case NodeConstants.Types.SELECT:
            {
                Criteria crit = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);       
                replaceCorrelatedReferences(node.getSubqueryContainers());
                if(!node.hasBooleanProperty(NodeConstants.Info.IS_HAVING)) {
                    query.setCriteria( CompoundCriteria.combineCriteria(query.getCriteria(), crit) );
                } else {
                    query.setHaving( CompoundCriteria.combineCriteria(query.getHaving(), crit) );                    
                }
                break;
            }
            case NodeConstants.Types.SORT: 
            {
                processOrderBy(node, query);
                break;
            }
            case NodeConstants.Types.DUP_REMOVE: 
            {
                query.getSelect().setDistinct(true);
                break;    
            }
            case NodeConstants.Types.GROUP: 
            {
                List groups = (List) node.getProperty(NodeConstants.Info.GROUP_COLS);
                if(groups != null && !groups.isEmpty()) {
                    query.setGroupBy(new GroupBy(groups));
                }
                break;
            }
            case NodeConstants.Types.TUPLE_LIMIT:
            {
                processLimit(node, query);
                break;
            }
        }        
    }

	private void replaceCorrelatedReferences(List<SubqueryContainer> containers) {
		for (SubqueryContainer container : containers) {
		    RelationalPlan subqueryPlan = (RelationalPlan)container.getCommand().getProcessorPlan();
		    if (subqueryPlan == null) {
		    	continue;
		    }
		    AccessNode child = (AccessNode)subqueryPlan.getRootNode();
		    Command command = child.getCommand();
		    final SymbolMap map = container.getCommand().getCorrelatedReferences();
		    if (map != null) {
		    	ExpressionMappingVisitor visitor = new ExpressionMappingVisitor(null) {
		    		@Override
		    		public Expression replaceExpression(
		    				Expression element) {
		    			if (element instanceof Reference) {
		    				Reference ref = (Reference)element;
		    				Expression replacement = map.getMappedExpression(ref.getExpression());
		    				if (replacement != null) {
		    					return replacement;
		    				}
		    			}
		    			return element;
		    		}
		    	};
		    	DeepPostOrderNavigator.doVisit(command, visitor);
		    }
		    container.setCommand(command);
		}
	}

    private void processLimit(PlanNode node,
                              QueryCommand query) {
        Expression limit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
        if (limit != null) {
            if (query.getLimit() != null) {
                Expression oldlimit = query.getLimit().getRowLimit();
                query.getLimit().setRowLimit(RulePushLimit.getMinValue(limit, oldlimit)); 
            } else {
                query.setLimit(new Limit(null, limit));
            }
        }
        Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
        if (offset != null) {
            if (query.getLimit() != null) {
                Expression oldoffset = query.getLimit().getOffset();
                query.getLimit().setOffset(RulePushLimit.getSum(offset, oldoffset)); 
            } else {
                query.setLimit(new Limit(offset, null));
            }
        }
    }

    /** 
     * Will combine the where criteria with the on criteria.
     * 
     * A full rewrite call is not necessary here, but it will attempt to flatten the criteria.
     * 
     * @param query
     * @param joinCrits
     */
    private void moveWhereClauseIntoOnClause(Query query,
                                List joinCrits) {
        if (query.getCriteria() == null) {
            return;
        }
        LinkedHashSet combinedCrits = new LinkedHashSet();
        combinedCrits.addAll(joinCrits);
        combinedCrits.addAll(Criteria.separateCriteriaByAnd(query.getCriteria()));
        joinCrits.clear();
        joinCrits.addAll(combinedCrits);
        query.setCriteria(null);
    }
    
	private void processOrderBy(PlanNode node, QueryCommand query) {
		List params = (List)node.getProperty(NodeConstants.Info.SORT_ORDER);
		List types = (List)node.getProperty(NodeConstants.Info.ORDER_TYPES);
		OrderBy orderBy = new OrderBy(params, types);
		query.setOrderBy(orderBy);
	}

   /**
    * Take the query, built straight from the subtree, and rebuild as a simple query
    * if possible.
    * @param query Query built from collapsing the source nodes
    * @return Same query with simplified from clause if possible 
    */
    private void simplifyFromClause(Query query) {
        From from = query.getFrom();
        List clauses = from.getClauses();
        FromClause rootClause = (FromClause) clauses.get(0);
       
        // If only one group, this is as good as we can do
        if(rootClause instanceof UnaryFromClause) {
            return;
        }
       
        // If all joins are inner joins, move criteria to WHERE and make 
        // FROM a list of groups instead of a tree of JoinPredicates
        if(! hasOuterJoins(rootClause)) {
            from.setClauses(new ArrayList());
            shredJoinTree(rootClause, query);

        } // else leave as is
    }    

    /**
    * @param rootClause
    * @param query
    */
    private void shredJoinTree(FromClause clause, Query query) {
        if(clause instanceof UnaryFromClause || clause instanceof SubqueryFromClause) {
            query.getFrom().addClause(clause);
        } else {
            JoinPredicate jp = (JoinPredicate) clause;
            
            List<Criteria> crits = jp.getJoinCriteria();
            if(crits != null && crits.size() > 0) {
            	Criteria joinCrit = null;
            	if (crits.size() > 1) {
            		joinCrit = new CompoundCriteria(crits);
            	} else {
            		joinCrit = crits.get(0);
            	}
                query.setCriteria(CompoundCriteria.combineCriteria(joinCrit, query.getCriteria()));
            }
            
            // Recurse through tree
            shredJoinTree(jp.getLeftClause(), query);
            shredJoinTree(jp.getRightClause(), query);            
        }
    }

    /**
     * @param clause Clause to check recursively
     * @return True if tree has outer joins, false otherwise
     */
    private boolean hasOuterJoins(FromClause clause) {
        if(clause instanceof UnaryFromClause || clause instanceof SubqueryFromClause) {
            return false;
        }
        JoinPredicate jp = (JoinPredicate) clause;
        if(jp.getJoinType().isOuter()) {
            return true;
        }
        // Walk children
        boolean childHasOuter = hasOuterJoins(jp.getLeftClause());
        if(childHasOuter) {
            return true;
        }
        return hasOuterJoins(jp.getRightClause());
    }
    
    public String toString() {
   		return "CollapseSource"; //$NON-NLS-1$
   	}

}
