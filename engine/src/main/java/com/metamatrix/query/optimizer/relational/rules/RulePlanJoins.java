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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.connector.api.ConnectorCapabilities.SupportedJoinCriteria;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.common.util.Permutation;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.processor.relational.JoinNode.JoinStrategyType;
import com.metamatrix.query.resolver.util.AccessPattern;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.CommandContext;

/**
 *  Determines join orderings based upon dependency and cost information 
 *  
 *  The algorithm works as follows:
 *  
 *  Stage 1.  Find join regions.  A join region is an set of inner and cross joins
 *  (with the join and intermediate criteria removed).
 *  
 *  Dependency Phase
 *  
 *  Stage 2.  Determine if dependencies found can be satisfied.
 *      a. Throw an exception if a quick check fails.
 *  
 *  Stage 3.  A satisfying set of access patterns and join ordering will be found
 *  for each join region.
 *      a. If this is not possible, an exception will be thrown
 *      b. only one possible set of access patterns will be considered
 *  
 *  Optimization Phase
 *    
 *  Stage 4.  Heuristically push joins down. Join regions (with more than one join source) will be
 *  exhaustively searched (bottom up) for join pairs that can be pushed to a source.
 *      a. A join is eligible for pushing if the access node can be raised and
 *         there is at least one join criteria that can also be pushed.
 *         -- costing information is not considered at this point.
 *      b. Once a pair has been pushed, they will be replaced in the join region
 *         with a single access node.
 *         
 *  Stage 5.  The remaining join regions will be ordered in a left linear tree based
 *  upon a an exhaustive, or random, algorithm that considers costing and criteria information.
 *   
 */
public class RulePlanJoins implements OptimizerRule {
    
    public static final int EXHAUSTIVE_SEARCH_GROUPS = 6;
                
    /** 
     * @see com.metamatrix.query.optimizer.relational.OptimizerRule#execute(com.metamatrix.query.optimizer.relational.plantree.PlanNode, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder, com.metamatrix.query.optimizer.relational.RuleStack, com.metamatrix.query.analysis.AnalysisRecord, com.metamatrix.query.util.CommandContext)
     */
    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capabilitiesFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   MetaMatrixComponentException {
        
            
        
        List<JoinRegion> joinRegions = new LinkedList<JoinRegion>();

        findJoinRegions(plan, null, joinRegions);
        
        //dependency phase
        
        for (Iterator<JoinRegion> joinRegionIter = joinRegions.iterator(); joinRegionIter.hasNext();) {
            JoinRegion joinRegion = joinRegionIter.next();
            
            //skip regions that have nothing to plan
            if (joinRegion.getJoinSourceNodes().size() + joinRegion.getDependentJoinSourceNodes().size() < 2) {
                joinRegionIter.remove();
                continue;
            }
            
            joinRegion.initializeJoinInformation();
            
            //check for unsatisfied dependencies
            if (joinRegion.getUnsatisfiedAccessPatterns().isEmpty()) {
                continue;
            }
                        
            //quick check for satisfiability
            if (!joinRegion.isSatisfiable()) {
                throw new QueryPlannerException(QueryExecPlugin.Util.getString("RulePlanJoins.cantSatisfy", joinRegion.getUnsatisfiedAccessPatterns())); //$NON-NLS-1$
            }
                        
            planForDependencies(joinRegion);
        }
        
        //optimization phase
        for (JoinRegion joinRegion : joinRegions) {
            groupJoinsForPushing(metadata, capabilitiesFinder, joinRegion, context);
        }
        
        for (Iterator<JoinRegion> joinRegionIter = joinRegions.iterator(); joinRegionIter.hasNext();) {
            JoinRegion joinRegion = joinRegionIter.next();
            
            //move the dependent nodes back into all joinSources
            joinRegion.getJoinSourceNodes().putAll(joinRegion.getDependentJoinSourceNodes());
            joinRegion.getCriteriaNodes().addAll(joinRegion.getDependentCriteriaNodes());
            joinRegion.getDependentJoinSourceNodes().clear();
            joinRegion.getDependentCriteriaNodes().clear();
            
            if (joinRegion.getJoinSourceNodes().size() < 2) {
                joinRegion.reconstructJoinRegoin();
                joinRegionIter.remove();
                continue;
            }
            
            joinRegion.initializeCostingInformation(metadata);
            
            Object[] bestOrder = findBestJoinOrder(joinRegion, metadata);
            
            //if no best order was found, just stick with how the user entered the query
            if (bestOrder == null) {
                continue;
            }
                        
            joinRegion.changeJoinOrder(bestOrder);
            joinRegion.reconstructJoinRegoin();
        }
                
        return plan;
    }

    /** 
     * This is a heuristic that checks for joins that may be pushed so they can be removed 
     * before considering the joins that must be evaluated in MetaMatrix.
     * 
     * By running this, we eliminate the need for running RuleRaiseAccess during join ordering
     * 
     * @param metadata
     * @param joinRegion
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     * @throws QueryPlannerException 
     */
    private void groupJoinsForPushing(QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
                                      JoinRegion joinRegion, CommandContext context) throws QueryMetadataException,
                                                            MetaMatrixComponentException, QueryPlannerException {
        //TODO: consider moving select criteria if it is preventing a join from being pushed down
        //TODO: make the criteria checks based upon a guess at selectivity
        
        Map accessMap = getAccessMap(metadata, capFinder, joinRegion);
        
        boolean structureChanged = false;
        
        //search for combinations of join sources that should be pushed down
        for (Iterator accessNodeIter = accessMap.entrySet().iterator(); accessNodeIter.hasNext();) {
            Map.Entry entry = (Map.Entry)accessNodeIter.next();
            
            List accessNodes = (List)entry.getValue();
            
            if (accessNodes.size() < 2) {
                continue;
            }
            
            for (int i = accessNodes.size() - 1; i >= 0; i--) {
                
                PlanNode accessNode1 = (PlanNode)accessNodes.get(i);
                
                for (int k = accessNodes.size() - 1; k >= 0; k--) {
                    if (k == i) {
                        continue;
                    }
                    
                    PlanNode accessNode2 = (PlanNode)accessNodes.get(k);
                    
                    List<PlanNode> criteriaNodes = joinRegion.getCriteriaNodes();
                    
                    List<PlanNode> joinCriteriaNodes = new LinkedList<PlanNode>();
                    
                    /* hasJoinCriteria will be true if
                     *  1. there is criteria between accessNode1 and accessNode2 exclusively
                     *  2. there is criteria between some other source (not the same logical connector) and accessNode1 or accessNode2
                     *  
                     *  Ideally we should be a little smarter in case 2 
                     *    - pushing down a same source cross join can be done if we know that a dependent join will be performed 
                     */
                    boolean hasJoinCriteria = false; 
                    LinkedList<Criteria> joinCriteria = new LinkedList<Criteria>();
                    Object modelId = RuleRaiseAccess.getModelIDFromAccess(accessNode1, metadata);
                    SupportedJoinCriteria sjc = CapabilitiesUtil.getSupportedJoinCriteria(modelId, metadata, capFinder);
                    for (PlanNode critNode : criteriaNodes) {
                        Set sources = (Set)joinRegion.getCritieriaToSourceMap().get(critNode);

                        if (sources == null) {
                            continue;
                        }
                        
                        if (sources.contains(accessNode1)) {
                            if (sources.contains(accessNode2) && sources.size() == 2) {
                                hasJoinCriteria = true;
                                Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
								if (RuleRaiseAccess.isSupportedJoinCriteria(sjc, crit, modelId, metadata, capFinder)) {
	                                joinCriteriaNodes.add(critNode);
	                                joinCriteria.add(crit);
                                }
                            } else if (!accessNodes.containsAll(sources)) {
                                hasJoinCriteria = true;
                            }
                        } else if (sources.contains(accessNode2) && !accessNodes.containsAll(sources)) {
                            hasJoinCriteria = true;
                        }
                    }
                    
                    /*
                     * If we failed to find direct criteria, but still have non-pushable or criteria to
                     * other groups we'll use additional checks
                     */
                    if ((!hasJoinCriteria || (hasJoinCriteria && joinCriteriaNodes.isEmpty())) && !canPushCrossJoin(metadata, context, accessNode1, accessNode2)) {
                    	continue;
                    }                    
                    
                    List toTest = new ArrayList(2);
                    toTest.add(accessNode1);
                    toTest.add(accessNode2);
                    
                    JoinType joinType = joinCriteria.isEmpty()?JoinType.JOIN_CROSS:JoinType.JOIN_INNER;
                    
                    //try to push to the source
                    if (RuleRaiseAccess.canRaiseOverJoin(toTest, metadata, capFinder, joinCriteria, joinType) == null) {
                        continue;
                    }
                    
                    structureChanged = true;
                    
                    //remove the information that is no longer relevant to the join region
                    joinRegion.getCritieriaToSourceMap().keySet().removeAll(joinCriteriaNodes);
                    joinRegion.getCriteriaNodes().removeAll(joinCriteriaNodes);
                    joinRegion.getJoinSourceNodes().remove(accessNode1);
                    joinRegion.getJoinSourceNodes().remove(accessNode2);
                    accessNodes.remove(i);
                    accessNodes.remove(k < i ? k : k - 1);

                    //build a new join node
                    PlanNode joinNode = createJoinNode();
                    joinNode.getGroups().addAll(accessNode1.getGroups());
                    joinNode.getGroups().addAll(accessNode2.getGroups());
                    joinNode.addFirstChild(accessNode2);
                    joinNode.addLastChild(accessNode1);
                    joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, joinType);
                    joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);

                    PlanNode newAccess = RuleRaiseAccess.raiseAccessOverJoin(joinNode, entry.getKey(), false);
                    for (Iterator joinCriteriaIter = joinCriteriaNodes.iterator(); joinCriteriaIter.hasNext();) {
                        PlanNode critNode = (PlanNode)joinCriteriaIter.next();
                        critNode.removeFromParent();
                        critNode.removeAllChildren();
                    }
                                    
                    //update with the new source
                    
                    for (Iterator sourceIter = joinRegion.getCritieriaToSourceMap().values().iterator(); sourceIter.hasNext();) {
                        Set source = (Set)sourceIter.next();
                        
                        if (source.remove(accessNode1) || source.remove(accessNode2)) {
                            source.add(newAccess);
                        }
                    }

                    joinRegion.getJoinSourceNodes().put(newAccess, newAccess);
                    accessNodes.add(newAccess);
                    i = accessNodes.size();
                    k = accessNodes.size();
                    break;
                }
            }
        }
        
        if (structureChanged) {
            joinRegion.reconstructJoinRegoin();
        }
    }

	private boolean canPushCrossJoin(QueryMetadataInterface metadata, CommandContext context,
			PlanNode accessNode1, PlanNode accessNode2)
			throws QueryMetadataException, MetaMatrixComponentException {
		float cost1 = NewCalculateCostUtil.computeCostForTree(accessNode1, metadata);
		float cost2 = NewCalculateCostUtil.computeCostForTree(accessNode2, metadata);
		float acceptableCost = context == null? 45.0f : (float)Math.sqrt(context.getProcessorBatchSize());
		return !((cost1 == -1 || cost2 == -1 || (cost1 > acceptableCost && cost2 > acceptableCost)));
	}

    /**
     * Return a map of Access Nodes to JoinSources that may be eligible for pushdown as
     * joins.
     */
    private Map getAccessMap(QueryMetadataInterface metadata,
                             CapabilitiesFinder capFinder,
                             JoinRegion joinRegion) throws QueryMetadataException,
                                                   MetaMatrixComponentException {
        Map accessMap = new HashMap();
        
        for (Iterator joinSourceIter = joinRegion.getJoinSourceNodes().values().iterator(); joinSourceIter.hasNext();) {
            PlanNode node = (PlanNode)joinSourceIter.next();
            
            /* check to see if we are directly over an access node.  in the event that the join source root
             * looks like select->access, we still won't consider this node for pushing
             */
            if (node.getType() != NodeConstants.Types.ACCESS) {
                continue;
            }
            Object accessModelID = RuleRaiseAccess.getModelIDFromAccess(node, metadata);
            
            if (accessModelID == null || !CapabilitiesUtil.supportsJoin(accessModelID, JoinType.JOIN_INNER, metadata, capFinder)) {
                continue;
            }
            
            RulePlanUnions.buildModelMap(metadata, capFinder, accessMap, node, accessModelID);
        }
        return accessMap;
    }
    
    /** 
     * Greedily choose the first set of access patterns that can be satisfied
     * TODO: this is greedy.  the first access pattern that can be satisfied will be 
     * TODO: order access patterns by number of dependent groups
     * 
     * If we could flatten to a single set of dependencies, then a topological sort would be faster
     * 
     * @param joinRegion
     * @throws QueryPlannerException
     */
    private void planForDependencies(JoinRegion joinRegion) throws QueryPlannerException {
                
        if (joinRegion.getJoinSourceNodes().isEmpty()) {
            throw new QueryPlannerException(QueryExecPlugin.Util.getString("RulePlanJoins.cantSatisfy", joinRegion.getUnsatisfiedAccessPatterns())); //$NON-NLS-1$
        }
        
        HashSet currentGroups = new HashSet();
        
        for (Iterator joinSources = joinRegion.getJoinSourceNodes().keySet().iterator(); joinSources.hasNext();) {
            PlanNode joinSource = (PlanNode)joinSources.next();
                        
            currentGroups.addAll(joinSource.getGroups());
        }
                
        HashMap dependentNodes = new HashMap(joinRegion.getDependentJoinSourceNodes());
                
        boolean satisfiedAP = true;
        
        while (!dependentNodes.isEmpty() && satisfiedAP) {
            
            satisfiedAP = false;
        
            for (Iterator joinSources = dependentNodes.entrySet().iterator(); joinSources.hasNext();) {
                Map.Entry entry = (Map.Entry)joinSources.next();
                PlanNode joinSource = (PlanNode)entry.getKey();
                
                Collection accessPatterns = (Collection)joinSource.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
                for (Iterator i = accessPatterns.iterator(); i.hasNext();) {
                    AccessPattern ap = (AccessPattern)i.next();
                    
                    boolean foundGroups = true;
                    HashSet allRequiredGroups = new HashSet();
                    for (Iterator j = ap.getUnsatisfied().iterator(); j.hasNext();) {
                        ElementSymbol symbol = (ElementSymbol)j.next();
                        Collection requiredGroupsSet = (Collection)joinRegion.getDependentCriteriaElements().get(symbol);
                        boolean elementSatisfied = false;
                        if (requiredGroupsSet != null) {
                            for (Iterator k = requiredGroupsSet.iterator(); k.hasNext();) {
                                Collection requiredGroups = (Collection)k.next();
                                if (currentGroups.containsAll(requiredGroups)) {
                                    elementSatisfied = true;
                                    allRequiredGroups.addAll(requiredGroups);
                                    break;
                                }
                            }
                        }
                        if (!elementSatisfied) {
                            foundGroups = false;
                            break;
                        }
                    }
                    
                    if (!foundGroups) {
                        continue;
                    }
                    
                    joinSources.remove();
                    satisfiedAP = true;
                    joinSource.setProperty(NodeConstants.Info.ACCESS_PATTERN_USED, ap.clone());
                    joinSource.setProperty(NodeConstants.Info.REQUIRED_ACCESS_PATTERN_GROUPS, allRequiredGroups);
                    break;
                }
            }
        }
        
        if (!dependentNodes.isEmpty()) {
            throw new QueryPlannerException(QueryExecPlugin.Util.getString("RulePlanJoins.cantSatisfy", joinRegion.getUnsatisfiedAccessPatterns())); //$NON-NLS-1$
        }
        
    }

    static PlanNode createJoinNode() {
        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
        joinNode.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
        return joinNode;
    }
    
    /**
     * Finds all regions of inner and cross joins
     * 
     * Join regions have boundaries at source nodes, outer joins, and unsatisfied dependencies
     *  
     * @param root
     * @param currentRegion
     * @param joinRegions
     */
    static void findJoinRegions(PlanNode root, JoinRegion currentRegion, List<JoinRegion> joinRegions) {
        switch (root.getType()) {
            case NodeConstants.Types.JOIN:
            {
                if (currentRegion == null) {
                    currentRegion = new JoinRegion();
                    joinRegions.add(currentRegion);
                }
                JoinType jt = (JoinType)root.getProperty(NodeConstants.Info.JOIN_TYPE);
                
                boolean treatJoinAsSource = jt.isOuter() || root.getProperty(NodeConstants.Info.ACCESS_PATTERNS) != null || root.hasProperty(NodeConstants.Info.MAKE_DEP);
                
                if (treatJoinAsSource) {
                    currentRegion.addJoinSourceNode(root);
                } else {
                    currentRegion.addParentCriteria(root);
                    currentRegion.addJoinCriteriaList((List)root.getProperty(NodeConstants.Info.JOIN_CRITERIA));
                }
                
                for (PlanNode child : root.getChildren()) {
                    findJoinRegions(child, treatJoinAsSource?null:currentRegion, joinRegions);
                }
                                
                return;
            }
            case NodeConstants.Types.SOURCE:
            {
                if (currentRegion != null) {
                    currentRegion.addJoinSourceNode(root);
                }
                currentRegion = null;
                break;
            }
            case NodeConstants.Types.NULL:
            case NodeConstants.Types.ACCESS:
            {
                if (currentRegion != null) {
                    currentRegion.addJoinSourceNode(root);
                }
                return;
            }
        }
        if (root.getChildCount() == 0) {
            return;
        }
        for (PlanNode child : root.getChildren()) {
            findJoinRegions(child, root.getChildCount()==1?currentRegion:null, joinRegions);
        }
    }
    
    /**
     * The scoring algorithm is partially exhaustive and partially greedy.  For
     * regions up to the exhaustive search group size all possible left linear join
     * trees will be searched in O(n!) time.
     * 
     * Beyond this number, every join will be determined greedily in O(n^2) time.
     *  
     * TODO: this method together with scoreRegion have not been optimized 
     * 
     * @param region
     * @param metadata
     * @return
     */
    Object[] findBestJoinOrder(JoinRegion region, QueryMetadataInterface metadata) {
        int regionCount = region.getJoinSourceNodes().size();
        
        List orderList = new ArrayList(regionCount);
        for(int i=0; i<regionCount; i++) {
            orderList.add(new Integer(i));
        }
        
        double bestSubScore = Double.MAX_VALUE;
        Object[] bestSubOrder = null;
        
        Permutation perms = new Permutation(orderList.toArray());

        int exhaustive = regionCount;

        //after 16 sources this will be completely greedy. before that it will try to strike a compromise between the exhaustive
        //and non-exhaustive searches
        if (regionCount > EXHAUSTIVE_SEARCH_GROUPS) {
            exhaustive = Math.max(2, EXHAUSTIVE_SEARCH_GROUPS - (int)Math.ceil(Math.sqrt((regionCount - EXHAUSTIVE_SEARCH_GROUPS))));
        } 
        
        Iterator permIter = perms.generate(exhaustive);
        
        while(permIter.hasNext()) {
            Object[] order = (Object[]) permIter.next();

            double score = region.scoreRegion(order, metadata);
            if(score < bestSubScore) {
                bestSubScore = score;
                bestSubOrder = order;
            }
        }
        
        if (bestSubOrder == null) {
            return null;
        }
        
        if (regionCount <= exhaustive) {
            return bestSubOrder;
        }
        
        Integer[] result = new Integer[regionCount];
        
        //remove the joins that have already been placed
        for(int i=0; i<bestSubOrder.length; i++) {
            orderList.remove(bestSubOrder[i]);
            result[i] = (Integer)bestSubOrder[i];
        }
        
        while(!orderList.isEmpty()) {
            
            double bestPartialScore = Double.MAX_VALUE;
            List bestOrder = null;

            for (int i = 0; i < orderList.size(); i++) {
                Integer index = (Integer)orderList.get(i);
                
                List order = new ArrayList(Arrays.asList(bestSubOrder));
                order.add(index);
                
                double partialScore = region.scoreRegion(order.toArray(), metadata);
                
                if (partialScore < bestPartialScore) {
                    bestPartialScore = partialScore;
                    bestOrder = order;
                }
            }
            
            if (bestOrder == null) {
                return null;
            }
            
            Integer next = (Integer)bestOrder.get(bestOrder.size() - 1);
            result[regionCount - orderList.size()] = next;
            orderList.remove(next);
            bestSubOrder = bestOrder.toArray();
        }
        
        return result;
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "PlanJoins"; //$NON-NLS-1$
    }

}
