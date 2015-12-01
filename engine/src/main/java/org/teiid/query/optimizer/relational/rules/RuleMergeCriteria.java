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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.id.IDGenerator;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.util.CommandContext;

public final class RuleMergeCriteria implements OptimizerRule {
	
	/**
	 * Used to replace correlated references
	 */
	public static final class ReferenceReplacementVisitor extends
			ExpressionMappingVisitor {
		private final SymbolMap refs;
		private boolean replacedAny;
		
		public ReferenceReplacementVisitor(SymbolMap refs) {
			super(null);
			this.refs = refs;
		}
		
		public Expression replaceExpression(Expression element) {
			if (element instanceof Reference) {
				Reference r = (Reference)element;
				Expression ex = refs.getMappedExpression(r.getExpression());
				if (ex != null) {
					if (ex instanceof ElementSymbol) {
						ElementSymbol es = (ElementSymbol) ex.clone();
						es.setIsExternalReference(false);
						ex = es;
					}
					replacedAny = true;
					return ex;
				}
			}
			return element;
		}
		
	}
	
	public static class PlannedResult {
		public List leftExpressions = new LinkedList(); 
		public List rightExpressions = new LinkedList();
		public Query query;
		public boolean not;
		public List<Criteria> nonEquiJoinCriteria = new LinkedList<Criteria>();
		public Criteria additionalCritieria;
		public Class<?> type;
		public boolean mergeJoin;
		public boolean madeDistinct;
		public boolean makeInd;
	}

	private IDGenerator idGenerator;
	private CapabilitiesFinder capFinder;
	private AnalysisRecord analysisRecord;
	private CommandContext context;
	private QueryMetadataInterface metadata;
	private boolean dependent;
	
	public RuleMergeCriteria(IDGenerator idGenerator, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context, QueryMetadataInterface metadata) {
		this.idGenerator = idGenerator;
    	this.capFinder = capFinder;
    	this.analysisRecord = analysisRecord;
    	this.context = context;
    	this.metadata = metadata;
	}

    /**
     * @see OptimizerRule#execute(PlanNode, QueryMetadataInterface, RuleStack)
     */
    public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
        throws QueryPlannerException, TeiidComponentException {
    	dependent = false;
        // Find strings of criteria and merge them, removing duplicates
        List<PlanNode> criteriaChains = new ArrayList<PlanNode>();
        findCriteriaChains(plan, criteriaChains, analysisRecord);
        
        // Merge chains
        for (PlanNode critNode : criteriaChains) {
            mergeChain(critNode, metadata);
        }
        if (dependent) {
        	//rules.push(new RuleAssignOutputElements(true));
        	rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }
        return plan;
    }

    /**
     * Walk the tree pre-order, looking for any chains of criteria
     * @param node Root node to search
     * @param foundNodes Roots of criteria chains
     */
     void findCriteriaChains(PlanNode root, List<PlanNode> foundNodes, AnalysisRecord analysisRecord)
        throws QueryPlannerException, TeiidComponentException {

        PlanNode recurseRoot = root;
        if(root.getType() == NodeConstants.Types.SELECT) {
        	
            // Walk to end of the chain and change recurse root
            while(recurseRoot.getType() == NodeConstants.Types.SELECT) {
            	// Look for opportunities to replace with a semi-join 
            	recurseRoot = planMergeJoin(recurseRoot, root);
            	if (root.getChildCount() == 0) {
            		root = recurseRoot.getFirstChild();
            		if (root.getType() != NodeConstants.Types.SELECT) {
            			root = root.getParent();
            		}
            	}
            	recurseRoot = recurseRoot.getFirstChild();
            }

            // Ignore trivial 1-node case
            if(recurseRoot.getParent() != root) {
                // Found root for chain
                foundNodes.add(root);
            }
        }
        
        if (recurseRoot.getType() != NodeConstants.Types.ACCESS) {
            for (PlanNode child : recurseRoot.getChildren()) {
                findCriteriaChains(child, foundNodes, analysisRecord);
            }
        }
    }

    static void mergeChain(PlanNode chainRoot, QueryMetadataInterface metadata) {
        // Remove all of chain except root, collect crit from each
        CompoundCriteria critParts = new CompoundCriteria();
        LinkedList<Criteria> subqueryCriteria = new LinkedList<Criteria>();
        PlanNode current = chainRoot;
        boolean isDependentSet = false;
        while(current.getType() == NodeConstants.Types.SELECT) {
        	if (!current.getCorrelatedReferenceElements().isEmpty()) {
        		//add at the end for delayed evaluation
        		subqueryCriteria.add(0, (Criteria)current.getProperty(NodeConstants.Info.SELECT_CRITERIA));
        	} else {
        		critParts.getCriteria().add(0, (Criteria)current.getProperty(NodeConstants.Info.SELECT_CRITERIA));	
        	}
            
            isDependentSet |= current.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET);
            
            // Recurse
            PlanNode last = current;
            current = current.getLastChild();

            // Remove current
            if(last != chainRoot) {
                NodeEditor.removeChildNode(last.getParent(), last);
            }
        }
        critParts.getCriteria().addAll(subqueryCriteria);
        Criteria combinedCrit = QueryRewriter.optimizeCriteria(critParts, metadata);

        if (isDependentSet) {
            chainRoot.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
        }
        
        // Replace criteria at root with new combined criteria
        chainRoot.setProperty(NodeConstants.Info.SELECT_CRITERIA, combinedCrit);
        
        // Reset group for node based on combined criteria
        chainRoot.getGroups().clear();
        
        chainRoot.addGroups(GroupsUsedByElementsVisitor.getGroups(combinedCrit));
        chainRoot.addGroups(GroupsUsedByElementsVisitor.getGroups(chainRoot.getCorrelatedReferenceElements()));
    }

    /**
     * Look for:
     * [NOT] EXISTS ( )
     * IN ( ) / SOME ( )
     * 
     * and replace with a semi join
     */
	private PlanNode planMergeJoin(PlanNode current, PlanNode root) throws QueryMetadataException,
			TeiidComponentException {
		float sourceCost = NewCalculateCostUtil.computeCostForTree(current.getFirstChild(), metadata);
		Criteria crit = (Criteria)current.getProperty(NodeConstants.Info.SELECT_CRITERIA);
		
		PlannedResult plannedResult = findSubquery(crit, true);
		if (plannedResult.query == null) {
			return current;
		}
		if (sourceCost != NewCalculateCostUtil.UNKNOWN_VALUE 
				&& sourceCost < RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY && !plannedResult.mergeJoin) {
			//TODO: see if a dependent join applies the other direction
			return current;
		}
		
		RelationalPlan originalPlan = (RelationalPlan)plannedResult.query.getProcessorPlan();
        Number originalCardinality = originalPlan.getRootNode().getEstimateNodeCardinality();
        if (!plannedResult.mergeJoin && originalCardinality.floatValue() == NewCalculateCostUtil.UNKNOWN_VALUE) {
            //TODO: this check isn't really accurate - exists and scalarsubqueries will always have cardinality 2/1
        	//if it's currently unknown, removing criteria won't make it any better
        	return current;
        }
        
        Collection<GroupSymbol> leftGroups = FrameUtil.findJoinSourceNode(current).getGroups();

		if (!planQuery(leftGroups, false, plannedResult)) {
			if (plannedResult.mergeJoin && analysisRecord != null && analysisRecord.recordAnnotations()) {
				this.analysisRecord.addAnnotation(new Annotation(Annotation.HINTS, "Could not plan as a merge join: " + crit, "ignoring MJ hint", Priority.HIGH)); //$NON-NLS-1$ //$NON-NLS-2$
			}
			return current;
		}
		
		//check if the child is already ordered.  TODO: see if the ordering is compatible.
		PlanNode childSort = NodeEditor.findNodePreOrder(root, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
		if (childSort != null) {
			if (plannedResult.mergeJoin && analysisRecord != null && analysisRecord.recordAnnotations()) {
				this.analysisRecord.addAnnotation(new Annotation(Annotation.HINTS, "Could not plan as a merge join since the parent join requires a sort: " + crit, "ignoring MJ hint", Priority.HIGH)); //$NON-NLS-1$ //$NON-NLS-2$
			}
			return current;
		}
		
		//add an order by, which hopefully will get pushed down
		plannedResult.query.setOrderBy(new OrderBy(plannedResult.rightExpressions).clone());
		for (OrderByItem item : plannedResult.query.getOrderBy().getOrderByItems()) {
			int index = plannedResult.query.getProjectedSymbols().indexOf(item.getSymbol());
			if (index >= 0 && !(item.getSymbol() instanceof ElementSymbol)) {
				item.setSymbol((Expression) plannedResult.query.getProjectedSymbols().get(index).clone());
			}
			item.setExpressionPosition(index);
		}
		
		try {
			//clone the symbols as they may change during planning
			List<Expression> projectedSymbols = LanguageObject.Util.deepClone(plannedResult.query.getProjectedSymbols(), Expression.class);
			//NOTE: we could tap into the relationalplanner at a lower level to get this in a plan node form,
			//the major benefit would be to reuse the dependent join planning logic if possible.
			RelationalPlan subPlan = (RelationalPlan)QueryOptimizer.optimizePlan(plannedResult.query, metadata, idGenerator, capFinder, analysisRecord, context);
			Number planCardinality = subPlan.getRootNode().getEstimateNodeCardinality();
            
			if (!plannedResult.mergeJoin) {
				//if we don't have a specific hint, then use costing
	            if (planCardinality.floatValue() == NewCalculateCostUtil.UNKNOWN_VALUE 
	            		|| planCardinality.floatValue() > 10000000
	            		|| (sourceCost == NewCalculateCostUtil.UNKNOWN_VALUE && planCardinality.floatValue() > 1000)
	            		|| (sourceCost != NewCalculateCostUtil.UNKNOWN_VALUE && sourceCost * originalCardinality.floatValue() < planCardinality.floatValue() / (100 * Math.log(Math.max(4, sourceCost))))) {
	            	//bail-out if both are unknown or the new plan is too large
	            	if (analysisRecord != null && analysisRecord.recordDebug()) {
	            		current.recordDebugAnnotation("cost of merge join plan was not favorable", null, "semi merge join will not be used", analysisRecord, metadata); //$NON-NLS-1$ //$NON-NLS-2$
	    			}
	            	return current;
	            }
			}
            
			//assume dependent
			if ((sourceCost != NewCalculateCostUtil.UNKNOWN_VALUE && planCardinality.floatValue() != NewCalculateCostUtil.UNKNOWN_VALUE 
					&& planCardinality.floatValue() < sourceCost / 8) || (sourceCost == NewCalculateCostUtil.UNKNOWN_VALUE && planCardinality.floatValue() <= 1000)) {
				plannedResult.makeInd = true;
			}
			
			/*if (plannedResult.makeInd 
					&& plannedResult.query.getCorrelatedReferences() == null
					&& !plannedResult.not
					&& plannedResult.leftExpressions.size() == 1) {
            	//TODO: this should just be a dependent criteria node to avoid sorts
            }*/
			
			current.recordDebugAnnotation("Conditions met (hint or cost)", null, "Converting to a semi merge join", analysisRecord, metadata); //$NON-NLS-1$ //$NON-NLS-2$
			
            PlanNode semiJoin = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
            semiJoin.addGroups(current.getGroups());
            Set<GroupSymbol> groups = GroupsUsedByElementsVisitor.getGroups(plannedResult.rightExpressions);
            semiJoin.addGroups(groups);
            semiJoin.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.MERGE);
            semiJoin.setProperty(NodeConstants.Info.JOIN_TYPE, plannedResult.not?JoinType.JOIN_ANTI_SEMI:JoinType.JOIN_SEMI);
            semiJoin.setProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA, plannedResult.nonEquiJoinCriteria);
            List<Criteria> joinCriteria = new ArrayList<Criteria>();
            joinCriteria.addAll(plannedResult.nonEquiJoinCriteria);
            for (int i = 0; i < plannedResult.leftExpressions.size(); i++) {
            	joinCriteria.add(new CompareCriteria((Expression)plannedResult.rightExpressions.get(i), CompareCriteria.EQ, (Expression)plannedResult.leftExpressions.get(i)));
            }
            semiJoin.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
            //nested subqueries are possibly being promoted, so they need their references updated
            List<SymbolMap> refMaps = semiJoin.getAllReferences();
            SymbolMap parentRefs = plannedResult.query.getCorrelatedReferences();
            for (SymbolMap refs : refMaps) {
            	for (Map.Entry<ElementSymbol, Expression> ref : refs.asUpdatableMap().entrySet()) {
    	            Expression expr = ref.getValue();
    	            if (expr instanceof ElementSymbol) {
	    	            Expression convertedExpr = parentRefs.getMappedExpression((ElementSymbol)expr);
	    	            if (convertedExpr != null) {
	    	            	ref.setValue(convertedExpr);
	    	            }
    	            }
    	            semiJoin.getGroups().addAll(GroupsUsedByElementsVisitor.getGroups(ref.getValue()));
    	        }
            }
            semiJoin.setProperty(NodeConstants.Info.LEFT_EXPRESSIONS, plannedResult.leftExpressions);
            semiJoin.getGroups().addAll(GroupsUsedByElementsVisitor.getGroups(plannedResult.leftExpressions));
            semiJoin.setProperty(NodeConstants.Info.RIGHT_EXPRESSIONS, plannedResult.rightExpressions);
            semiJoin.getGroups().addAll(GroupsUsedByElementsVisitor.getGroups(plannedResult.rightExpressions));
            semiJoin.setProperty(NodeConstants.Info.SORT_RIGHT, SortOption.ALREADY_SORTED);
            semiJoin.setProperty(NodeConstants.Info.OUTPUT_COLS, root.getProperty(NodeConstants.Info.OUTPUT_COLS));
            
            List childOutput = (List)current.getFirstChild().getProperty(NodeConstants.Info.OUTPUT_COLS);
            PlanNode toCorrect = root;
            while (toCorrect != current) {
            	toCorrect.setProperty(NodeConstants.Info.OUTPUT_COLS, childOutput);
            	toCorrect = toCorrect.getFirstChild();
            }
            
            PlanNode node = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
            node.setProperty(NodeConstants.Info.PROCESSOR_PLAN, subPlan);
            node.setProperty(NodeConstants.Info.OUTPUT_COLS, projectedSymbols);
            node.setProperty(NodeConstants.Info.EST_CARDINALITY, planCardinality);
            node.addGroups(groups);
            root.addAsParent(semiJoin);
            semiJoin.addLastChild(node);
            PlanNode result = current.getParent();
            NodeEditor.removeChildNode(result, current);
            RuleImplementJoinStrategy.insertSort(semiJoin.getFirstChild(), (List<Expression>) plannedResult.leftExpressions, semiJoin, metadata, capFinder, true);
            if (plannedResult.makeInd && !plannedResult.not) {
            	//TODO: would like for an enhanced sort merge with the semi dep option to avoid the sorting
            	//this is a little different than a typical dependent join in that the right is the independent side
            	String id = RuleChooseDependent.nextId();
            	PlanNode dep = RuleChooseDependent.getDependentCriteriaNode(id, plannedResult.rightExpressions, plannedResult.leftExpressions, node, metadata, null, false, null);
            	semiJoin.getFirstChild().addAsParent(dep);
            	semiJoin.setProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE, id);
            	this.dependent = true;
            }
            return result;
		} catch (QueryPlannerException e) {
			//can't be done - probably access patterns - what about dependent
			return current;
		}
	}

	public PlannedResult findSubquery(Criteria crit, boolean unnest) throws TeiidComponentException, QueryMetadataException {
		PlannedResult result = new PlannedResult();
		if (crit instanceof SubquerySetCriteria) {
			//convert to the quantified form
			SubquerySetCriteria ssc = (SubquerySetCriteria)crit;
			if (ssc.getSubqueryHint().isNoUnnest()) {
				return result;
			}
			result.not = ssc.isNegated();
			result.type = ssc.getClass();
			crit = new SubqueryCompareCriteria(ssc.getExpression(), ssc.getCommand(), SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
			((SubqueryCompareCriteria)crit).setSubqueryHint(ssc.getSubqueryHint());
		} else if (crit instanceof CompareCriteria) {
			CompareCriteria cc = (CompareCriteria)crit;
			if (cc.getRightExpression() instanceof ScalarSubquery) {
				ScalarSubquery ss = (ScalarSubquery)cc.getRightExpression();
				if (ss.getSubqueryHint().isNoUnnest()) {
					return result;
				}
				result.type = ss.getClass();
				//we can only use a semi-join if we know that 1 row will be present
				if (ss.getCommand() instanceof Query) {
					Query query = (Query)ss.getCommand();
					if (query.getGroupBy() == null && query.hasAggregates()) {
						crit = new SubqueryCompareCriteria(cc.getLeftExpression(), ss.getCommand(), cc.getOperator(), SubqueryCompareCriteria.SOME);
						((SubqueryCompareCriteria)crit).setSubqueryHint(ss.getSubqueryHint());
					}
				}
			}
		}
		if (crit instanceof SubqueryCompareCriteria) {
			SubqueryCompareCriteria scc = (SubqueryCompareCriteria)crit;
			if (scc.getSubqueryHint().isNoUnnest()) {
				return result;
			}
			if (scc.getPredicateQuantifier() != SubqueryCompareCriteria.SOME
					//TODO: could add an inline view if not a query
					|| !(scc.getCommand() instanceof Query)) {
				return result;
			}     

			Query query = (Query)scc.getCommand();
			Expression rightExpr = SymbolMap.getExpression(query.getProjectedSymbols().get(0));
			
			if (result.not && !isNonNull(query, rightExpr)) {
				return result;
			}
			if (result.type == null) {
				result.type = scc.getClass();
			}
			result.mergeJoin = scc.getSubqueryHint().isMergeJoin();
			if (!unnest && !result.mergeJoin) {
				return result;
			}
			result.makeInd = scc.getSubqueryHint().isDepJoin();
			result.query = query;
			result.additionalCritieria = (Criteria)new CompareCriteria(scc.getLeftExpression(), scc.getOperator(), rightExpr).clone();
		}
		if (crit instanceof ExistsCriteria) {
			ExistsCriteria exists = (ExistsCriteria)crit;
			if (exists.getSubqueryHint().isNoUnnest()) {
				return result;
			}
			if (!(exists.getCommand() instanceof Query)) {
				return result;
			} 
			result.type = crit.getClass();
			result.not = exists.isNegated();
			//the correlations can only be in where (if no group by or aggregates) or having
			result.mergeJoin = exists.getSubqueryHint().isMergeJoin();
			result.makeInd = exists.getSubqueryHint().isDepJoin();
			if (!unnest && !result.mergeJoin) {
				return result;
			}
			result.query = (Query)exists.getCommand();
		}
		return result;
	}

	private boolean isNonNull(Query query, Expression rightExpr)
			throws TeiidComponentException, QueryMetadataException {
		if (rightExpr instanceof ElementSymbol) {
			ElementSymbol es = (ElementSymbol)rightExpr;
			if (metadata.elementSupports(es.getMetadataID(), SupportConstants.Element.NULL)) {
				return false;
			}
			if (!isSimpleJoin(query)) {
				return false;
			}
		} else if (rightExpr instanceof Constant) {
			if (((Constant)rightExpr).isNull()) {
				return false;
			}
		} else if (rightExpr instanceof AggregateSymbol) {
			AggregateSymbol as = (AggregateSymbol)rightExpr;
			if (as.getAggregateFunction() != Type.COUNT) {
				return false;
			}
		} else {
			return false;
		}
		return true;
	}

	private boolean isSimpleJoin(Query query) {
		if (query.getFrom() != null) {
			for (FromClause clause : query.getFrom().getClauses()) {
				if (RuleCollapseSource.hasOuterJoins(clause)) {
					return false;
				}
			}
		}
		return true;
	}
	
	public boolean planQuery(Collection<GroupSymbol> leftGroups, boolean requireDistinct, PlannedResult plannedResult) throws QueryMetadataException, TeiidComponentException {
		if ((plannedResult.query.getLimit() != null && !plannedResult.query.getLimit().isImplicit()) || plannedResult.query.getFrom() == null) {
			return false;
		}
		
		if ((plannedResult.type == ExistsCriteria.class || plannedResult.type == ScalarSubquery.class) && plannedResult.query.getCorrelatedReferences() == null) {
			//we can't really improve on this case
			//TODO: do this check earlier
			return false;
		}
		
		plannedResult.query = (Query)plannedResult.query.clone();
		plannedResult.query.setLimit(null);

		List<GroupSymbol> rightGroups = plannedResult.query.getFrom().getGroups();
		Set<Expression> requiredExpressions = new LinkedHashSet<Expression>();
		final SymbolMap refs = plannedResult.query.getCorrelatedReferences();
		boolean addGroupBy = false;
		if (refs != null) {
			boolean hasAggregates = plannedResult.query.hasAggregates();
			Criteria where = plannedResult.query.getCriteria();
			if (plannedResult.query.getGroupBy() == null) {
				plannedResult.query.setCriteria(null);
			}
			Criteria having = plannedResult.query.getHaving();
			plannedResult.query.setHaving(null);
			if (hasCorrelatedReferences(plannedResult.query, refs)) {
				return false;
			}
			if (plannedResult.query.getGroupBy() == null) {
				processCriteria(leftGroups, plannedResult, rightGroups, requiredExpressions, refs, where, null, true);
				if (hasAggregates) {
					if (!plannedResult.nonEquiJoinCriteria.isEmpty()) {
						return false;
					}
					addGroupBy = true;
				}
			}
			processCriteria(leftGroups, plannedResult, rightGroups, requiredExpressions, refs, having, plannedResult.query.getGroupBy(), false);
		}
				
		if (plannedResult.additionalCritieria != null) {
			RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, plannedResult.leftExpressions, plannedResult.rightExpressions, Criteria.separateCriteriaByAnd(plannedResult.additionalCritieria), plannedResult.nonEquiJoinCriteria);
		}
		
		if (plannedResult.leftExpressions.isEmpty()) {
			return false;
		}
		
		plannedResult.leftExpressions = RuleChooseJoinStrategy.createExpressionSymbols(plannedResult.leftExpressions);
		plannedResult.rightExpressions = RuleChooseJoinStrategy.createExpressionSymbols(plannedResult.rightExpressions);
		
		if (requireDistinct && !addGroupBy) {
			//ensure that uniqueness applies to the in condition
			if (plannedResult.rightExpressions.size() > 1 
					&& (plannedResult.type != SubquerySetCriteria.class || !isDistinct(plannedResult.query, plannedResult.rightExpressions.subList(plannedResult.rightExpressions.size() - 1, plannedResult.rightExpressions.size()), metadata))) { 
				return false;
			}
			
			if (!isDistinct(plannedResult.query, plannedResult.rightExpressions, metadata)) {
				if (plannedResult.type == ExistsCriteria.class) {
					if (requiredExpressions.size() > plannedResult.leftExpressions.size()) {
						return false; //not an equi join
					}
				} else if (!requiredExpressions.isEmpty() && !isDistinct(plannedResult.query, plannedResult.query.getProjectedSymbols(), metadata)) {
					return false;
				}
				plannedResult.query.getSelect().setDistinct(true);
				plannedResult.madeDistinct = true;
			}
		}
		
		//it doesn't matter what the select columns are
		if (plannedResult.type == ExistsCriteria.class) {
			plannedResult.query.getSelect().clearSymbols();
		}

		if (addGroupBy) {
			LinkedHashSet<Expression> groupingSymbols = new LinkedHashSet<Expression>();
			for (Expression expr : (List<Expression>)plannedResult.rightExpressions) {
				AggregateSymbolCollectorVisitor.getAggregates(expr, null, groupingSymbols, null, null, null);
			}
			if (!groupingSymbols.isEmpty()) {
				plannedResult.query.setGroupBy((GroupBy) new GroupBy(new ArrayList<Expression>(groupingSymbols)).clone());
			}
		}
		HashSet<Expression> projectedSymbols = new HashSet<Expression>();
		for (Expression ses : plannedResult.query.getProjectedSymbols()) {
			projectedSymbols.add(SymbolMap.getExpression(ses));
		}
		for (Expression ses : requiredExpressions) {
			if (projectedSymbols.add(ses)) {
				plannedResult.query.getSelect().addSymbol((Expression)ses.clone());
			}
		}
		for (Expression ses : (List<Expression>)plannedResult.rightExpressions) {
			if (projectedSymbols.add(SymbolMap.getExpression(ses))) {
				plannedResult.query.getSelect().addSymbol((Expression)ses.clone());
			}
		}
		return true;
	}

	private void processCriteria(Collection<GroupSymbol> leftGroups,
			PlannedResult plannedResult, List<GroupSymbol> rightGroups,
			Set<Expression> requiredExpressions, final SymbolMap refs,
			Criteria joinCriteria, GroupBy groupBy, boolean where) {
		if (joinCriteria == null) {
			return;
		}
		List<Criteria> crits = Criteria.separateCriteriaByAnd((Criteria)joinCriteria.clone());

		for (Iterator<Criteria> critIter = crits.iterator(); critIter.hasNext();) {
			Criteria conjunct = critIter.next();
			List<Expression> additionalRequired = new LinkedList<Expression>();
			AggregateSymbolCollectorVisitor.getAggregates(conjunct, additionalRequired, additionalRequired, additionalRequired, null, groupBy!=null?groupBy.getSymbols():null);
			ReferenceReplacementVisitor emv = new ReferenceReplacementVisitor(refs);
			DeepPostOrderNavigator.doVisit(conjunct, emv);
			if (!emv.replacedAny) {
				//if not correlated, then leave it on the query
				critIter.remove();
				if (where) {
					plannedResult.query.setCriteria(Criteria.combineCriteria(plannedResult.query.getCriteria(), conjunct));
				} else {
					plannedResult.query.setHaving(Criteria.combineCriteria(plannedResult.query.getHaving(), conjunct));
				}
			} else {
				requiredExpressions.addAll(additionalRequired);
			}
		}
		RuleChooseJoinStrategy.separateCriteria(leftGroups, rightGroups, plannedResult.leftExpressions, plannedResult.rightExpressions, crits, plannedResult.nonEquiJoinCriteria);
	}

	public static boolean isDistinct(Query query, List<Expression> expressions, QueryMetadataInterface metadata)
			throws QueryMetadataException, TeiidComponentException {
		boolean distinct = false;
		if (query.getGroupBy() != null) {
			distinct = true;
			for (Expression groupByExpr : query.getGroupBy().getSymbols()) {
				if (!expressions.contains(groupByExpr)) {
					distinct = false;
					break;
				}
			}
		}
		if (distinct) {
			return true;
		}
		HashSet<GroupSymbol> keyPreservingGroups = new HashSet<GroupSymbol>();
		ResolverUtil.findKeyPreserved(query, keyPreservingGroups, metadata);
		return NewCalculateCostUtil.usesKey(expressions, keyPreservingGroups, metadata, true);			
	}

	private boolean hasCorrelatedReferences(LanguageObject object, SymbolMap correlatedReferences) {
		Collection<Reference> references =  ReferenceCollectorVisitor.getReferences(object);
		for (Reference reference : references) {
			if (correlatedReferences.asMap().containsKey(reference.getExpression())) {
				return true;
			}
		}
		return false;
	}

    public String toString() {
        return "MergeCriteria"; //$NON-NLS-1$
    }

}
