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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Permutation;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;


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
     * @see org.teiid.query.optimizer.relational.OptimizerRule#execute(org.teiid.query.optimizer.relational.plantree.PlanNode, org.teiid.query.metadata.QueryMetadataInterface, org.teiid.query.optimizer.capabilities.CapabilitiesFinder, org.teiid.query.optimizer.relational.RuleStack, org.teiid.query.analysis.AnalysisRecord, org.teiid.query.util.CommandContext)
     */
    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capabilitiesFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {
        
            
        
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
            
            //account for nested table correlations
            for (PlanNode joinSource : joinRegion.getJoinSourceNodes().keySet()) {
            	SymbolMap map = (SymbolMap)joinSource.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
            	if (map !=null) {
                    joinSource.setProperty(NodeConstants.Info.REQUIRED_ACCESS_PATTERN_GROUPS, GroupsUsedByElementsVisitor.getGroups(map.getValues()));
                    joinRegion.setContainsNestedTable(true);
            	}
            }
            
            //check for unsatisfied dependencies
            if (joinRegion.getUnsatisfiedAccessPatterns().isEmpty()) {
                continue;
            }
                        
            //quick check for satisfiability
            if (!joinRegion.isSatisfiable()) {
                throw new QueryPlannerException(QueryPlugin.Util.getString("RulePlanJoins.cantSatisfy", joinRegion.getUnsatisfiedAccessPatterns())); //$NON-NLS-1$
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
            
            Object[] bestOrder = findBestJoinOrder(joinRegion, metadata, capabilitiesFinder, context);
            
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
     * @throws TeiidComponentException
     * @throws QueryPlannerException 
     */
    private void groupJoinsForPushing(QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
                                      JoinRegion joinRegion, CommandContext context) throws QueryMetadataException,
                                                            TeiidComponentException, QueryPlannerException {
        //TODO: consider moving select criteria if it is preventing a join from being pushed down
        //TODO: make the criteria checks based upon a guess at selectivity
        
        Map accessMap = getAccessMap(metadata, capFinder, joinRegion);
        
        boolean structureChanged = false;
        
        //search for combinations of join sources that should be pushed down
        for (Iterator accessNodeIter = accessMap.entrySet().iterator(); accessNodeIter.hasNext();) {
            Map.Entry entry = (Map.Entry)accessNodeIter.next();
            
            List<PlanNode> accessNodes = (List)entry.getValue();
            
            if (accessNodes.size() < 2) {
                continue;
            }
            
            int secondPass = -1;
            
            for (int i = accessNodes.size() - 1; i >= 0; i--) {
                
                PlanNode accessNode1 = accessNodes.get(i);
                Object modelId = RuleRaiseAccess.getModelIDFromAccess(accessNode1, metadata);
                SupportedJoinCriteria sjc = CapabilitiesUtil.getSupportedJoinCriteria(modelId, metadata, capFinder);
                
                int discoveredJoin = -1;
                 
                for (int k = (secondPass==-1?accessNodes.size() - 1:secondPass); k >= 0; k--) {
                    if (k == i) {
                        continue;
                    }
                    
                    PlanNode accessNode2 = accessNodes.get(k);
                    
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
                    for (PlanNode critNode : criteriaNodes) {
                        Set<PlanNode> sources = joinRegion.getCritieriaToSourceMap().get(critNode);

                        if (sources == null) {
                            continue;
                        }
                        
                        if (sources.contains(accessNode1)) {
                            if (sources.contains(accessNode2) && sources.size() == 2) {
                                Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
								if (RuleRaiseAccess.isSupportedJoinCriteria(sjc, crit, modelId, metadata, capFinder, null)) {
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
                     * If we failed to find direct criteria, a cross join may still be acceptable
                     */
                    if (joinCriteriaNodes.isEmpty() && (hasJoinCriteria || !canPushCrossJoin(metadata, context, accessNode1, accessNode2))) {
                    	continue;
                    }                    
                    
                    List<PlanNode> toTest = Arrays.asList(accessNode1, accessNode2);
                    
                    JoinType joinType = joinCriteria.isEmpty()?JoinType.JOIN_CROSS:JoinType.JOIN_INNER;
                    
                    //try to push to the source
                    if (RuleRaiseAccess.canRaiseOverJoin(toTest, metadata, capFinder, joinCriteria, joinType, null, secondPass != -1) == null) {
                    	if (secondPass == - 1 && sjc != SupportedJoinCriteria.KEY && discoveredJoin == -1) {
                			for (Criteria criteria : joinCriteria) {
                				if (criteria instanceof CompareCriteria && ((CompareCriteria) criteria).isOptional()) {
                					discoveredJoin = k;
                				}
                			}
                		}
                        continue;
                    }
                    
                    secondPass = -1;
                    discoveredJoin = -1;
                    
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
                    for (PlanNode critNode : joinCriteriaNodes) {
                        critNode.removeFromParent();
                        critNode.removeAllChildren();
                    }
                                    
                    //update with the new source
                    
                    for (Set<PlanNode> source : joinRegion.getCritieriaToSourceMap().values()) {
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
                
                if (discoveredJoin != -1) {
                	i++; //rerun with the discoveredJoin criteria
                	secondPass = discoveredJoin;
                }
            }
        }
        
        if (structureChanged) {
            joinRegion.reconstructJoinRegoin();
        }
    }

	private boolean canPushCrossJoin(QueryMetadataInterface metadata, CommandContext context,
			PlanNode accessNode1, PlanNode accessNode2)
			throws QueryMetadataException, TeiidComponentException {
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
                                                   TeiidComponentException {
        Map accessMap = new HashMap();
        
        for (PlanNode node : joinRegion.getJoinSourceNodes().values()) {
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
            throw new QueryPlannerException(QueryPlugin.Util.getString("RulePlanJoins.cantSatisfy", joinRegion.getUnsatisfiedAccessPatterns())); //$NON-NLS-1$
        }
        
        HashSet<GroupSymbol> currentGroups = new HashSet<GroupSymbol>();
        
        for (PlanNode joinSource : joinRegion.getJoinSourceNodes().keySet()) {
            currentGroups.addAll(joinSource.getGroups());
        }
                
        HashMap<PlanNode, PlanNode> dependentNodes = new HashMap<PlanNode, PlanNode>(joinRegion.getDependentJoinSourceNodes());
                
        boolean satisfiedAP = true;
        
        while (!dependentNodes.isEmpty() && satisfiedAP) {
            
            satisfiedAP = false;
        
            for (Iterator<Map.Entry<PlanNode, PlanNode>> joinSources = dependentNodes.entrySet().iterator(); joinSources.hasNext();) {
            	Map.Entry<PlanNode, PlanNode> entry = joinSources.next();
                PlanNode joinSource = entry.getKey();
                
                Collection accessPatterns = (Collection)joinSource.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
                for (Iterator i = accessPatterns.iterator(); i.hasNext();) {
                    AccessPattern ap = (AccessPattern)i.next();
                    
                    boolean foundGroups = true;
                    HashSet<GroupSymbol> allRequiredGroups = new HashSet<GroupSymbol>();
                    for (ElementSymbol symbol : ap.getUnsatisfied()) {
                        Set<Collection<GroupSymbol>> requiredGroupsSet = joinRegion.getDependentCriteriaElements().get(symbol);
                        boolean elementSatisfied = false;
                        if (requiredGroupsSet != null) {
                            for (Collection<GroupSymbol> requiredGroups : requiredGroupsSet) {
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
            throw new QueryPlannerException(QueryPlugin.Util.getString("RulePlanJoins.cantSatisfy", joinRegion.getUnsatisfiedAccessPatterns())); //$NON-NLS-1$
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
                
                boolean treatJoinAsSource = jt.isOuter() || root.getProperty(NodeConstants.Info.ACCESS_PATTERNS) != null 
                || root.hasProperty(NodeConstants.Info.MAKE_DEP) || root.hasProperty(NodeConstants.Info.MAKE_IND)
                || !root.getExportedCorrelatedReferences().isEmpty();
                
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
     * @throws QueryPlannerException 
     */
    Object[] findBestJoinOrder(JoinRegion region, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        int regionCount = region.getJoinSourceNodes().size();
        
        List<Integer> orderList = new ArrayList<Integer>(regionCount);
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

            double score = region.scoreRegion(order, 0, metadata, capFinder, context);
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
            result[i] = (Integer)bestSubOrder[i];
        }
        
        while(!orderList.isEmpty()) {
            
            double bestPartialScore = Double.MAX_VALUE;
            List bestOrder = null;

            for (int i = 0; i < orderList.size(); i++) {
                Integer index = orderList.get(i);
                
                List order = new ArrayList(Arrays.asList(bestSubOrder));
                order.add(index);
                
                double partialScore = region.scoreRegion(order.toArray(), bestSubOrder.length, metadata, capFinder, context);
                
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
