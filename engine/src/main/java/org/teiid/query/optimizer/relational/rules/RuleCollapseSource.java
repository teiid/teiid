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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


public final class RuleCollapseSource implements OptimizerRule {

	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        for (PlanNode accessNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {
            
            // Get nested non-relational plan if there is one
            ProcessorPlan nonRelationalPlan = FrameUtil.getNestedPlan(accessNode);
    		Command command = FrameUtil.getNonQueryCommand(accessNode);

            if(nonRelationalPlan != null) {
                accessNode.setProperty(NodeConstants.Info.PROCESSOR_PLAN, nonRelationalPlan);
            } else if (RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata) == null) {
            	//with query
            	accessNode.setProperty(NodeConstants.Info.IS_COMMON_TABLE, Boolean.TRUE);
            } else if(command == null) {
            	PlanNode commandRoot = accessNode;
            	GroupSymbol intoGroup = (GroupSymbol)accessNode.getFirstChild().getProperty(NodeConstants.Info.INTO_GROUP);
            	if (intoGroup != null) {
            		commandRoot = NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.SOURCE).getFirstChild();
            	}
                plan = removeUnnecessaryInlineView(plan, commandRoot);
                QueryCommand queryCommand = createQuery(metadata, capFinder, accessNode, commandRoot);
            	addDistinct(metadata, capFinder, accessNode, queryCommand);
                command = queryCommand;
                if (intoGroup != null) {
                	Insert insertCommand = (Insert)commandRoot.getParent().getProperty(NodeConstants.Info.VIRTUAL_COMMAND);
                	if (insertCommand == null) {
                		//TODO: this is probably no longer needed as we rewrite select into
                		insertCommand = new Insert(intoGroup, ResolverUtil.resolveElementsInGroup(intoGroup, metadata), null);
                	}
                	insertCommand.setQueryExpression(queryCommand);
                	command = insertCommand;
                }
            }
            if (command != null) {
            	accessNode.setProperty(NodeConstants.Info.ATOMIC_REQUEST, command);
            }
    		accessNode.removeAllChildren();
        }
       				
		return plan;
	}

	/**
	 * This functions as "RulePushDistinct", however we do not bother
	 * checking to see if a parent dup removal can actually be removed
	 * - which can only happen if there are sources/selects/simple projects/limits/order by
	 * between the access node and the parent dup removal.
	 * 
	 * @param metadata
	 * @param capFinder
	 * @param accessNode
	 * @param queryCommand
	 * @throws QueryMetadataException
	 * @throws TeiidComponentException
	 */
	private void addDistinct(QueryMetadataInterface metadata,
			CapabilitiesFinder capFinder, PlanNode accessNode,
			QueryCommand queryCommand) throws QueryMetadataException,
			TeiidComponentException {
		if (queryCommand.getLimit() != null) {
			return; //TODO: could create an inline view
		}
		if (queryCommand.getOrderBy() == null) {
			/* 
			 * we're assuming that a pushed order by implies that the cost of the distinct operation 
			 * will be marginal - which is not always true.
			 * 
			 * TODO: we should add costing for the benefit of pushing distinct by itself
			 * cardinality without = c
			 * assume cost ~ c lg c for c' cardinality and a modification for associated bandwidth savings
			 * recompute cost of processing plan with c' and see if new cost + c lg c < original cost
			 */
			return; 
		}
		if (RuleRemoveOptionalJoins.useNonDistinctRows(accessNode.getParent())) {
			return;
		}
		// ensure that all columns are comparable - they might not be if there is an intermediate project
		for (SingleElementSymbol ses : queryCommand.getProjectedSymbols()) {
			if (DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(ses.getType()))) {
				return;
			}
		}
		/* 
		 * TODO: if we are under a grouping/union not-all, then we should also fully order the results 
		 * and update the processing logic (this requires that we can guarantee null ordering) to assume sorted
		 */
		if (queryCommand instanceof SetQuery) {
			((SetQuery)queryCommand).setAll(false);
		} else if (CapabilitiesUtil.supports(Capability.QUERY_SELECT_DISTINCT, RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata), metadata, capFinder)) {
			Query query = (Query)queryCommand;
			HashSet<GroupSymbol> keyPreservingGroups = new HashSet<GroupSymbol>();
			ResolverUtil.findKeyPreserved(query, keyPreservingGroups, metadata);
			if (!QueryRewriter.isDistinctWithGroupBy(query) && !NewCalculateCostUtil.usesKey(query.getSelect().getProjectedSymbols(), keyPreservingGroups, metadata, true)) {
				((Query)queryCommand).getSelect().setDistinct(true);
			}
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

	private QueryCommand createQuery(QueryMetadataInterface metadata, CapabilitiesFinder capFinder, PlanNode accessRoot, PlanNode node) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
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
                processLimit(limit, unionCommand, metadata);
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
        prepareSubqueries(ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(columns));
        select.addSymbols(columns);
        query.setSelect(select);
		query.setFrom(new From());
		buildQuery(accessRoot, node, query, metadata, capFinder);
		if (query.getCriteria() instanceof CompoundCriteria) {
            query.setCriteria(QueryRewriter.optimizeCriteria((CompoundCriteria)query.getCriteria(), metadata));
        }
		Object modelID = RuleRaiseAccess.getModelIDFromAccess(accessRoot, metadata);
		if (!CapabilitiesUtil.useAnsiJoin(modelID, metadata, capFinder)) {
			simplifyFromClause(query);
        }
		if (columns.isEmpty()) {
        	if (CapabilitiesUtil.supports(Capability.QUERY_SELECT_EXPRESSION, modelID, metadata, capFinder)) {
        		select.addSymbol(new ExpressionSymbol("dummy", new Constant(1))); //$NON-NLS-1$
        	} else {
        		//TODO: need to ensure the type is consistent  
        		//- should be rare as the source would typically support select expression if it supports union
        		select.addSymbol(selectOutputElement(query.getFrom().getGroups(), metadata));
        	}
        }
		PlanNode groupNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE);
		if (groupNode != null) {
			if (query.getOrderBy() != null) {
				query.setOrderBy(query.getOrderBy().clone());
			}
			if (query.getHaving() != null) {
				query.setHaving((Criteria) query.getHaving().clone());
			}
			query.setSelect(query.getSelect().clone());
	        SymbolMap symbolMap = (SymbolMap) groupNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
	      
	        //map back to expression form
	        ExpressionMappingVisitor.mapExpressions(query.getOrderBy(), symbolMap.asMap());
	        ExpressionMappingVisitor.mapExpressions(query.getSelect(), symbolMap.asMap()); 
	        ExpressionMappingVisitor.mapExpressions(query.getHaving(), symbolMap.asMap()); 
	
			if (!CapabilitiesUtil.supports(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, modelID, metadata, capFinder)) {
				//if group by expressions are not support, add an inline view to compensate
				query = RuleCollapseSource.rewriteGroupByExpressionsAsView(query, metadata);
			}
		}
		return query;
	}		
	
    /**
     * Find a selectable element in the specified groups.  This is a helper for fixing
     * the "no elements" case.
     *
     * @param groups Bunch of groups
     * @param metadata Metadata implementation
     * @throws QueryPlannerException
     */
    private ElementSymbol selectOutputElement(Collection<GroupSymbol> groups, QueryMetadataInterface metadata)
        throws QueryMetadataException, TeiidComponentException {

        // Find a group with selectable elements and pick the first one
        for (GroupSymbol group : groups) {
            List<ElementSymbol> elements = (List<ElementSymbol>)ResolverUtil.resolveElementsInGroup(group, metadata);
            
            for (ElementSymbol element : elements) {
                if(metadata.elementSupports(element.getMetadataID(), SupportConstants.Element.SELECT)) {
                    element = element.clone();
                    element.setGroupSymbol(group);
                    return element;
                }
            }
        }
        
        return null;
    }

    void buildQuery(PlanNode accessRoot, PlanNode node, Query query, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        
    	//visit source and join nodes as they appear
        switch(node.getType()) {
            case NodeConstants.Types.JOIN:
            {
                prepareSubqueries(node.getSubqueryContainers());
                JoinType joinType = (JoinType) node.getProperty(NodeConstants.Info.JOIN_TYPE);
                List<Criteria> crits = (List<Criteria>) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                
                if (crits == null || crits.isEmpty()) {
                    crits = new ArrayList<Criteria>();
                } else {
                	RuleChooseJoinStrategy.filterOptionalCriteria(crits);
                	if (crits.isEmpty() && joinType == JoinType.JOIN_INNER) {
                		joinType = JoinType.JOIN_CROSS;
                	}
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
                List<FromClause> clauses = from.getClauses();
                int lastClause = clauses.size()-1;
                FromClause clause1 = clauses.get(lastClause-1);
                FromClause clause2 = clauses.get(lastClause);
                 
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
                prepareSubqueries(node.getSubqueryContainers());
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
                processLimit(node, query, metadata);
                break;
            }
        }        
    }

	private void prepareSubqueries(List<SubqueryContainer> containers) {
		for (SubqueryContainer container : containers) {
		    prepareSubquery(container);
		}
	}

	public static void prepareSubquery(SubqueryContainer container) {
		RelationalPlan subqueryPlan = (RelationalPlan)container.getCommand().getProcessorPlan();
		AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(subqueryPlan);
		QueryCommand command = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode);
		if (command == null) {
			return;
		}
		final SymbolMap map = container.getCommand().getCorrelatedReferences();
		if (map != null) {
			ExpressionMappingVisitor visitor = new RuleMergeCriteria.ReferenceReplacementVisitor(map);
			DeepPostOrderNavigator.doVisit(command, visitor);
		}
		command.setProcessorPlan(container.getCommand().getProcessorPlan());
		boolean removeLimit = false;
		if (container instanceof ExistsCriteria) {
			removeLimit = !((ExistsCriteria)container).shouldEvaluate();
		} else if (container instanceof ScalarSubquery) {
			removeLimit = !((ScalarSubquery)container).shouldEvaluate();
		}
		if (removeLimit && command.getLimit() != null && command.getLimit().isImplicit()) {
			command.setLimit(null);
		}
		container.setCommand(command);
	}

    private void processLimit(PlanNode node,
                              QueryCommand query, QueryMetadataInterface metadata) {
    	
        Expression limit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
        Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
        
        PlanNode limitNode = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
        Expression childLimit = null;
        Expression childOffset = null;
        if (query.getLimit() != null) {
        	childLimit = query.getLimit().getRowLimit();
        	childOffset = query.getLimit().getOffset();
        }
        RulePushLimit.combineLimits(limitNode, metadata, limit, offset, childLimit, childOffset);
        Limit lim = new Limit((Expression)limitNode.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT), (Expression)limitNode.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT));
        lim.setImplicit(node.hasBooleanProperty(Info.IS_IMPLICIT_LIMIT));
        query.setLimit(lim);
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
		OrderBy orderBy = (OrderBy)node.getProperty(NodeConstants.Info.SORT_ORDER);
		query.setOrderBy(orderBy);
		if (query instanceof Query) {
			List<SingleElementSymbol> cols = query.getProjectedSymbols();
			for (OrderByItem item : orderBy.getOrderByItems()) {
				item.setExpressionPosition(cols.indexOf(item.getSymbol()));
			}
			QueryRewriter.rewriteOrderBy(query, orderBy, query.getProjectedSymbols(), new LinkedList<OrderByItem>());
		}
	}

   /**
    * Take the query, built straight from the subtree, and rebuild as a simple query
    * if possible.
    * @param query Query built from collapsing the source nodes
    * @return Same query with simplified from clause if possible 
    */
    private void simplifyFromClause(Query query) {
        From from = query.getFrom();
        List<FromClause> clauses = from.getClauses();
        FromClause rootClause = clauses.get(0);
       
        // If all joins are inner joins, move criteria to WHERE and make 
        // FROM a list of groups instead of a tree of JoinPredicates
        if(! hasOuterJoins(rootClause)) {
            from.setClauses(new ArrayList<FromClause>());
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
    static boolean hasOuterJoins(FromClause clause) {
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

	public static Query rewriteGroupByExpressionsAsView(Query query, QueryMetadataInterface metadata) {
		if (query.getGroupBy() == null) {
			return query;
		}
	    // we check for group by expressions here to create an ANSI SQL plan
	    boolean hasExpression = false;
	    for (final Iterator<Expression> iterator = query.getGroupBy().getSymbols().iterator(); !hasExpression && iterator.hasNext();) {
	        hasExpression = !(iterator.next() instanceof ElementSymbol);
	    } 
	    if (!hasExpression) {
	    	return query;
	    }
		Select select = query.getSelect();
	    GroupBy groupBy = query.getGroupBy();
	    query.setGroupBy(null);
	    Criteria having = query.getHaving();
	    query.setHaving(null);
	    OrderBy orderBy = query.getOrderBy();
	    query.setOrderBy(null);
	    Limit limit = query.getLimit();
	    query.setLimit(null);
	    Set<Expression> newSelectColumns = new HashSet<Expression>();
	    for (final Iterator<Expression> iterator = groupBy.getSymbols().iterator(); iterator.hasNext();) {
	        newSelectColumns.add(iterator.next());
	    }
	    Set<AggregateSymbol> aggs = new HashSet<AggregateSymbol>();
	    aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(select, true));
	    if (having != null) {
	        aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(having, true));
	    }
	    for (AggregateSymbol aggregateSymbol : aggs) {
	        if (aggregateSymbol.getExpression() != null) {
	            Expression expr = aggregateSymbol.getExpression();
	            newSelectColumns.add(SymbolMap.getExpression(expr));
	        }
	    }
	    Select innerSelect = new Select();
	    int index = 0;
	    for (Expression expr : newSelectColumns) {
	        if (expr instanceof SingleElementSymbol) {
	            innerSelect.addSymbol((SingleElementSymbol)expr);
	        } else {
	            innerSelect.addSymbol(new ExpressionSymbol("EXPR" + index++ , expr)); //$NON-NLS-1$
	        }
	    }
	    query.setSelect(innerSelect);
	    Query outerQuery = null;
	    try {
	        outerQuery = QueryRewriter.createInlineViewQuery(new GroupSymbol("X"), query, metadata, query.getSelect().getProjectedSymbols()); //$NON-NLS-1$
	    } catch (TeiidException err) {
	        throw new TeiidRuntimeException(err);
	    }
	    Iterator<SingleElementSymbol> iter = outerQuery.getSelect().getProjectedSymbols().iterator();
	    HashMap<Expression, SingleElementSymbol> expressionMap = new HashMap<Expression, SingleElementSymbol>();
	    for (SingleElementSymbol symbol : query.getSelect().getProjectedSymbols()) {
	        expressionMap.put(SymbolMap.getExpression(symbol), iter.next());
	    }
	    ExpressionMappingVisitor.mapExpressions(groupBy, expressionMap);
	    outerQuery.setGroupBy(groupBy);
	    ExpressionMappingVisitor.mapExpressions(having, expressionMap);
	    outerQuery.setHaving(having);
	    ExpressionMappingVisitor.mapExpressions(orderBy, expressionMap);
	    outerQuery.setOrderBy(orderBy);
	    outerQuery.setLimit(limit);
	    ExpressionMappingVisitor.mapExpressions(select, expressionMap);
	    outerQuery.setSelect(select);
	    outerQuery.setOption(query.getOption());
	    query = outerQuery;
		return query;
	}

}
