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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.relational.GenerateCanonical;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.resolver.util.AccessPattern;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.FunctionCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;

/**
 *  A join region is a set of cross and inner joins whose ordering is completely interchangable.
 *  
 *  It can be conceptually thought of as:
 *     Criteria node some combination of groups A, B, C
 *     Criteria node some combination of groups A, B, C
 *     ...
 *     Join
 *       JoinSourceA
 *       JoinSourceB
 *       JoinSourceC
 *  
 *  A full binary join tree is then constructed out of this join region such that all of the
 *  criteria is pushed to its lowest point.
 *  
 */
class JoinRegion {
    
    private PlanNode joinRoot;
    
    public static final int UNKNOWN_TUPLE_EST = 100000;
    
    private LinkedHashMap dependentJoinSourceNodes = new LinkedHashMap();
    private LinkedHashMap joinSourceNodes = new LinkedHashMap();
        
    private List dependentCritieraNodes = new ArrayList();
    private List criteriaNodes = new ArrayList();
    
    private List unsatisfiedAccessPatterns = new LinkedList();
    
    private Map<ElementSymbol, Set<Collection<GroupSymbol>>> dependentCriteriaElements;
    private Map critieriaToSourceMap;
    
    public PlanNode getJoinRoot() {
        return joinRoot;
    }
    
    public List getUnsatisfiedAccessPatterns() {
        return unsatisfiedAccessPatterns;
    }
    
    public Map getJoinSourceNodes() {
        return joinSourceNodes;
    }

    public Map getDependentJoinSourceNodes() {
        return dependentJoinSourceNodes;
    }
    
    public List<PlanNode> getCriteriaNodes() {
        return criteriaNodes;
    }
    
    public List getDependentCriteriaNodes() {
        return dependentCritieraNodes;
    }
    
    public Map<ElementSymbol, Set<Collection<GroupSymbol>>> getDependentCriteriaElements() {
        return this.dependentCriteriaElements;
    }

    public Map getCritieriaToSourceMap() {
        return this.critieriaToSourceMap;
    }

    public void addJoinSourceNode(PlanNode sourceNode) {
        PlanNode root = sourceNode;
        while (root.getParent() != null && root.getParent().getType() == NodeConstants.Types.SELECT) {
            root = root.getParent();
        }
        if (sourceNode.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
            Collection aps = (Collection)sourceNode.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
            unsatisfiedAccessPatterns.add(aps);
            dependentJoinSourceNodes.put(sourceNode, root);
        } else {
            joinSourceNodes.put(sourceNode, root);
        }
        
        if (joinRoot == null) {
            joinRoot = root;
        }
    }

    public void addParentCriteria(PlanNode sourceNode) {
        PlanNode parent = sourceNode.getParent();
        while (parent != null && parent.getType() == NodeConstants.Types.SELECT) {
            criteriaNodes.add(parent);
            sourceNode = parent;
            parent = parent.getParent();
        }
        if (joinRoot == null) {
            joinRoot = sourceNode;
        }
    }
                    
    public void addJoinCriteriaList(List joinCriteria) {
        if (joinCriteria == null || joinCriteria.isEmpty()) {
            return;
        }
        for (Iterator i = joinCriteria.iterator(); i.hasNext();) {
            Criteria crit = (Criteria)i.next();
            criteriaNodes.add(GenerateCanonical.createSelectNode(crit, false));
        }
    }
    
    /**
     * This will rebuild the join tree starting at the join root.
     * 
     * A left linear tree will be constructed out of the ordering of the 
     * join sources.
     * 
     * Criteria nodes are simply placed at the top of the join region in order
     * to be pushed by rule PushSelectSriteria.
     * 
     */
    public void reconstructJoinRegoin() {
        LinkedHashMap combined = new LinkedHashMap(joinSourceNodes);
        combined.putAll(dependentJoinSourceNodes);
        
        PlanNode root = null;
        
        if (combined.size() < 2) {
            root = (PlanNode)combined.values().iterator().next();
            root.removeProperty(NodeConstants.Info.EST_CARDINALITY);
        } else {
            root = RulePlanJoins.createJoinNode();
        
            for (Iterator i = combined.entrySet().iterator(); i.hasNext();) {
                Map.Entry entry = (Map.Entry)i.next();
                PlanNode joinSourceRoot = (PlanNode)entry.getValue();
                joinSourceRoot.removeProperty(NodeConstants.Info.EST_CARDINALITY);
                if (root.getChildCount() == 2) {
                    PlanNode parentJoin = RulePlanJoins.createJoinNode();
                    parentJoin.addFirstChild(root);
                    parentJoin.addGroups(root.getGroups());
                    root = parentJoin;
                }
                root.addLastChild(joinSourceRoot);
                root.addGroups(((PlanNode)entry.getKey()).getGroups());
            }
        }
        LinkedList criteria = new LinkedList(dependentCritieraNodes);
        criteria.addAll(criteriaNodes);

        PlanNode parent = this.joinRoot.getParent();
        
        boolean isLeftChild = parent.getFirstChild() == this.joinRoot;

        parent.removeChild(joinRoot);
        
        for (Iterator i = criteria.iterator(); i.hasNext();) {
            PlanNode critNode = (PlanNode)i.next();
            
            critNode.removeFromParent();
            critNode.removeAllChildren();
            critNode.addFirstChild(root);
            root = critNode;
            critNode.removeProperty(NodeConstants.Info.IS_COPIED);
            critNode.removeProperty(NodeConstants.Info.EST_CARDINALITY);
        }

        if (isLeftChild) {
            parent.addFirstChild(root);
        } else {
            parent.addLastChild(root);
        }
        this.joinRoot = root;
    }
    
    /**
     * Will provide an estimate of cost by summing the estimated tuples flowing through
     * each intermediate join. 
     *  
     * @param joinOrder
     * @param metadata
     * @return
     */
    public double scoreRegion(Object[] joinOrder, QueryMetadataInterface metadata) {
        List joinSourceEntries = new ArrayList(joinSourceNodes.entrySet());
        double totalIntermediatCost = 0;
        double cost = 1;
        
        HashSet criteria = new HashSet(this.criteriaNodes);
        HashSet groups = new HashSet(this.joinSourceNodes.size());
        
        for (int i = 0; i < joinOrder.length; i++) {
            Integer source = (Integer)joinOrder[i];
            
            Map.Entry entry = (Map.Entry)joinSourceEntries.get(source.intValue());
            PlanNode joinSourceRoot = (PlanNode)entry.getValue();
            
            //check to make sure that this group ordering satisfies the access patterns
            if (!this.unsatisfiedAccessPatterns.isEmpty()) {
                PlanNode joinSource = (PlanNode)entry.getKey();
                
                Collection requiredGroups = (Collection)joinSource.getProperty(NodeConstants.Info.REQUIRED_ACCESS_PATTERN_GROUPS);
                
                if (requiredGroups != null && !groups.containsAll(requiredGroups)) {
                    return Double.MAX_VALUE;
                }
            }
            
            groups.addAll(joinSourceRoot.getGroups());
            
            float sourceCost = ((Float)joinSourceRoot.getProperty(NodeConstants.Info.EST_CARDINALITY)).floatValue();
            
            if (sourceCost == NewCalculateCostUtil.UNKNOWN_VALUE) {
                sourceCost = UNKNOWN_TUPLE_EST;
            } else if (Double.isInfinite(sourceCost) || Double.isNaN(sourceCost)) {
                sourceCost = UNKNOWN_TUPLE_EST * 10;
            }
            
            cost *= sourceCost;
            
            if (!criteria.isEmpty() && i > 0) {
                List applicableCriteria = getJoinCriteriaForGroups(groups, criteria);
                
                for (Iterator j = applicableCriteria.iterator(); j.hasNext();) {
                    PlanNode criteriaNode = (PlanNode)j.next();
                    
                    float filter = ((Float)criteriaNode.getProperty(NodeConstants.Info.EST_SELECTIVITY)).floatValue();
                    
                    cost *= filter;
                }
                
                criteria.removeAll(applicableCriteria);
            }
            
            totalIntermediatCost += cost;
        }
        
        return totalIntermediatCost;
    }
    
    /**
     *  Returns true if every element in an unsatisfied access pattern can be satisfied by the current join criteria
     *  This does not necessarily mean that a join tree will be successfully created
     */
    public boolean isSatisfiable() {
        if (getUnsatisfiedAccessPatterns().isEmpty()) {
            return true;
        }
        
        for (Iterator i = getUnsatisfiedAccessPatterns().iterator(); i.hasNext();) {
            Collection accessPatterns = (Collection)i.next();
            boolean matchedAll = false;
            for (Iterator j = accessPatterns.iterator(); j.hasNext();) {
                AccessPattern ap = (AccessPattern)j.next();
                if (dependentCriteriaElements.keySet().containsAll(ap.getUnsatisfied())) {
                    matchedAll = true;
                    break;
                }
            }
            if (!matchedAll) {
                return false;
            }
        }
        
        return true;
    }

    public void initializeCostingInformation(QueryMetadataInterface metadata) throws QueryMetadataException, MetaMatrixComponentException {
        for (Iterator i = joinSourceNodes.values().iterator(); i.hasNext();) {
            PlanNode node = (PlanNode)i.next();
            float value = NewCalculateCostUtil.computeCostForTree(node, metadata);
            node.setProperty(NodeConstants.Info.EST_CARDINALITY, new Float(value));
        }
        
        estimateCriteriaSelectivity(metadata);        
    }

    /** 
     * @param metadata
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    private void estimateCriteriaSelectivity(QueryMetadataInterface metadata) throws QueryMetadataException,
                                                                             MetaMatrixComponentException {
        for (Iterator i = criteriaNodes.iterator(); i.hasNext();) {
            PlanNode node = (PlanNode)i.next();
            
            Criteria crit = (Criteria)node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            
            float[] baseCosts = new float[] {100, 10000, 1000000};
            
            float filterValue = 0;
            
            for (int j = 0; j < baseCosts.length; j++) {
                float filter = NewCalculateCostUtil.recursiveEstimateCostOfCriteria(baseCosts[j], node, crit, metadata);
                
                filterValue += filter/baseCosts[j];
            }
            
            filterValue /= baseCosts.length;
   
            node.setProperty(NodeConstants.Info.EST_SELECTIVITY, new Float(filterValue));
        }
    }
    
    /**
     *  Initializes information on the joinRegion about dependency information, etc.
     *  
     *  TODO: assumptions are made here about how dependent criteria must look that are a little restrictive
     */
    public void initializeJoinInformation() {
        critieriaToSourceMap = new HashMap();
                
        LinkedList crits = new LinkedList(criteriaNodes);
        crits.addAll(dependentCritieraNodes);
        
        LinkedHashMap source = new LinkedHashMap(joinSourceNodes);
        source.putAll(dependentJoinSourceNodes);
        
        for (Iterator j = crits.iterator(); j.hasNext();) {
            PlanNode critNode = (PlanNode)j.next();
            
            for (Iterator k = critNode.getGroups().iterator(); k.hasNext();) {

                GroupSymbol group = (GroupSymbol)k.next();
                
                for (Iterator i = source.keySet().iterator(); i.hasNext();) {
                    PlanNode node = (PlanNode)i.next();
                    
                    if (node.getGroups().contains(group)) {
                        Set sources = (Set)critieriaToSourceMap.get(critNode);
                        if (sources == null) {
                            sources = new HashSet();
                            critieriaToSourceMap.put(critNode, sources);
                        }
                        sources.add(node);
                        break;
                    }
                }
            }
        }            

        if (unsatisfiedAccessPatterns.isEmpty()) {
            return;
        }
        
        Map dependentGroupToSourceMap = new HashMap();
        
        for (Iterator i = dependentJoinSourceNodes.keySet().iterator(); i.hasNext();) {
            PlanNode node = (PlanNode)i.next();
            
            for (Iterator j = node.getGroups().iterator(); j.hasNext();) {
                GroupSymbol symbol = (GroupSymbol)j.next();
                dependentGroupToSourceMap.put(symbol, node);
            }
        }
        
        for (Iterator i = getCriteriaNodes().iterator(); i.hasNext();) {
            PlanNode node = (PlanNode)i.next();
            
            for (Iterator j = node.getGroups().iterator(); j.hasNext();) {
                GroupSymbol symbol = (GroupSymbol)j.next();
                if (dependentGroupToSourceMap.containsKey(symbol)) {
                    i.remove();
                    dependentCritieraNodes.add(node);
                    break;
                }
            }
        }
        
        dependentCriteriaElements = new HashMap<ElementSymbol, Set<Collection<GroupSymbol>>>();
        
        for (Iterator i = dependentCritieraNodes.iterator(); i.hasNext();) {
            PlanNode critNode = (PlanNode)i.next();
            Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            if(!(crit instanceof CompareCriteria)) {
                continue;
            }
            CompareCriteria compCrit = (CompareCriteria) crit;                
            if(compCrit.getOperator() != CompareCriteria.EQ) {
                continue;
            }
            CompareCriteria compareCriteria = (CompareCriteria)crit;
            //this may be a proper dependent join criteria
            Collection<ElementSymbol>[] critElements = new Collection[2];
            critElements[0] = ElementCollectorVisitor.getElements(compareCriteria.getLeftExpression(), true);
            if (critElements[0].isEmpty()) {
            	continue;
            }
            critElements[1] = ElementCollectorVisitor.getElements(compareCriteria.getRightExpression(), true);
            if (critElements[1].isEmpty()) {
            	continue;
            }
            for (int expr = 0; expr < critElements.length; expr++) {
                //simplifying assumption that there will be a single element on the dependent side
                if (critElements[expr].size() != 1) {
                    continue;
                }
                ElementSymbol elem = critElements[expr].iterator().next();
                if (!dependentGroupToSourceMap.containsKey(elem.getGroupSymbol())) {
                    continue;
                }
                //this is also a simplifying assumption.  don't consider criteria that can't be pushed
                if (containsFunctionsThatCannotBePushed(expr==0?compareCriteria.getRightExpression():compareCriteria.getLeftExpression())) {
                    continue;
                }
                Set<Collection<GroupSymbol>> independentGroups = dependentCriteriaElements.get(elem);
                if (independentGroups == null) {
                    independentGroups = new HashSet<Collection<GroupSymbol>>();
                    dependentCriteriaElements.put(elem, independentGroups);
                }
                //set the other side as independent elements
                independentGroups.add(GroupsUsedByElementsVisitor.getGroups(critElements[(expr+1)%2]));
            }
        }
    }
    
    /**
     * Returns true if the expression is, or contains, any functions that cannot be pushed 
     * down to the source
     * @param expression
     * @return
     * @since 4.2
     */
    private static boolean containsFunctionsThatCannotBePushed(Expression expression) {
        Iterator functions = FunctionCollectorVisitor.getFunctions(expression, true).iterator();
        while (functions.hasNext()) {
            Function function = (Function)functions.next();
            if (function.getFunctionDescriptor().getPushdown() == FunctionMethod.CANNOT_PUSHDOWN) {
                return true;
            }
        }
        return false;
    }
    
    public List getJoinCriteriaForGroups(Set groups) {
        return getJoinCriteriaForGroups(groups, getCriteriaNodes());
    }
    
    //TODO: this should be better than a linear search
    protected List getJoinCriteriaForGroups(Set groups, Collection nodes) {
        List result = new LinkedList();
        
        for (Iterator i = nodes.iterator(); i.hasNext();) {
            PlanNode critNode = (PlanNode)i.next();
            
            if (groups.containsAll(critNode.getGroups())) {
                result.add(critNode);
            }
        }
        
        return result;
    }
    
    public void changeJoinOrder(Object[] joinOrder) {
        List joinSourceEntries = new ArrayList(joinSourceNodes.entrySet());
        
        for (int i = 0; i < joinOrder.length; i++) {
            Integer source = (Integer)joinOrder[i];
            
            Map.Entry entry = (Map.Entry)joinSourceEntries.get(source.intValue());
            
            this.joinSourceNodes.remove(entry.getKey());
            this.joinSourceNodes.put(entry.getKey(), entry.getValue());
        }
    }

}