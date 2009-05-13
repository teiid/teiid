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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.resolver.util.AccessPattern;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.navigator.PostOrderNavigator;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.ExpressionMappingVisitor;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

public class FrameUtil {

    static void convertFrame(PlanNode startNode, GroupSymbol oldGroup, Set<GroupSymbol> newGroups, Map symbolMap, QueryMetadataInterface metadata) 
        throws QueryPlannerException {

        PlanNode current = startNode;
        
        PlanNode endNode = NodeEditor.findParent(startNode.getType()==NodeConstants.Types.SOURCE?startNode.getParent():startNode, NodeConstants.Types.SOURCE);
        
        while(current != endNode) { 
            
            // Make translations as defined in node in each current node
            convertNode(current, oldGroup, newGroups, symbolMap);
            
            PlanNode parent = current.getParent(); 
            
            //check if this is not the first set op branch
            if (parent != null && parent.getType() == NodeConstants.Types.SET_OP && parent.getFirstChild() != current) {
                return;
            }

            // Move up the tree
            current = parent;
        }
                
        if(endNode == null) {
            return;
        }
        // Top of a frame - fix symbol mappings on endNode      
        SymbolMap parentSymbolMap = (SymbolMap) endNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        if(parentSymbolMap == null) {
            return;
        }
        
        for (Map.Entry<ElementSymbol, Expression> entry : new HashSet<Map.Entry<ElementSymbol, Expression>>(parentSymbolMap.asMap().entrySet())) {
            parentSymbolMap.addMapping(entry.getKey(), convertExpression(entry.getValue(), symbolMap));
        }
        
    }
    
    static boolean canConvertAccessPatterns(PlanNode sourceNode) throws QueryPlannerException {
        List accessPatterns = (List)sourceNode.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
        if (accessPatterns == null) {
            return true;
        }
        SymbolMap symbolMap = (SymbolMap)sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        for (Iterator i = accessPatterns.iterator(); i.hasNext();) {
            AccessPattern ap = (AccessPattern)i.next();
            for (Iterator elems = ap.getUnsatisfied().iterator(); elems.hasNext();) {
                ElementSymbol symbol = (ElementSymbol)elems.next();
                Expression mapped = convertExpression(symbol, symbolMap.asMap());
                if (ElementCollectorVisitor.getElements(mapped, true).isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    /** 
     * @param symbolMap
     * @param node
     * @throws QueryPlannerException
     */
    private static void convertAccessPatterns(Map symbolMap,
                                              PlanNode node) throws QueryPlannerException {
        List accessPatterns = (List)node.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
        if (accessPatterns != null) {
            for (Iterator i = accessPatterns.iterator(); i.hasNext();) {
                AccessPattern ap = (AccessPattern)i.next();
                Set newElements = new HashSet();
                for (Iterator elems = ap.getUnsatisfied().iterator(); elems.hasNext();) {
                    ElementSymbol symbol = (ElementSymbol)elems.next();
                    Expression mapped = convertExpression(symbol, symbolMap);
                    newElements.addAll(ElementCollectorVisitor.getElements(mapped, true));
                }
                ap.setUnsatisfied(newElements);
                Set newHistory = new HashSet();
                for (Iterator elems = ap.getCurrentElements().iterator(); elems.hasNext();) {
                    ElementSymbol symbol = (ElementSymbol)elems.next();
                    Expression mapped = convertExpression(symbol, symbolMap);
                    newHistory.addAll(ElementCollectorVisitor.getElements(mapped, true));
                }
                ap.addElementHistory(newHistory);
            }
            Collections.sort(accessPatterns);
        }
    }
        
    // If newGroup == null, this will be performing a straight symbol swap - that is, 
    // an oldGroup is undergoing a name change and that is the only difference in the 
    // symbols.  In that case, some additional work can be done because we can assume 
    // that an oldElement isn't being replaced by an expression using elements from 
    // multiple new groups.  
    static void convertNode(PlanNode node, GroupSymbol oldGroup, Set<GroupSymbol> newGroups, Map symbolMap)
        throws QueryPlannerException {

        // Update groups for current node   
        Set groups = node.getGroups();  

        boolean hasOld = groups.remove(oldGroup);

        int type = node.getType();

        // Convert expressions from correlated subquery references;
        // currently only for SELECT or PROJECT nodes
        List refs = (List)node.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
        if (refs != null){
            Iterator refIter = refs.iterator();
            while (refIter.hasNext()) {
                Reference ref = (Reference)refIter.next();
                Expression expr = ref.getExpression();
                Expression convertedExpr = convertExpression(expr, symbolMap);
                ref.setExpression(convertedExpr);
            }
        }
        
        boolean singleMapping = newGroups != null && newGroups.size() == 1;
        
        if(singleMapping) {
            if (!hasOld) {
                return;
            }
            groups.addAll(newGroups);
        } else if ((type & (NodeConstants.Types.ACCESS | NodeConstants.Types.JOIN | NodeConstants.Types.SOURCE)) == type) {
        	if (newGroups != null) {
        		groups.addAll(newGroups);
        	}
        } else {
        	groups.clear();
        }   

        if(type == NodeConstants.Types.SELECT) { 
            Criteria crit = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            convertCriteria(crit, symbolMap);
            
            if (!singleMapping) {
                GroupsUsedByElementsVisitor.getGroups(crit, groups);
            }
                            
        } else if(type == NodeConstants.Types.PROJECT) {                    
            List elements = (List) node.getProperty(NodeConstants.Info.PROJECT_COLS);           
            List newElements = new ArrayList(elements.size());
            
            Iterator elementIter = elements.iterator();
            while(elementIter.hasNext()) { 
                SingleElementSymbol symbol = (SingleElementSymbol) elementIter.next();
                SingleElementSymbol mappedSymbol = convertSingleElementSymbol(symbol, symbolMap, true);
                newElements.add(mappedSymbol);
                      
                if (!singleMapping) {
                    GroupsUsedByElementsVisitor.getGroups(mappedSymbol, groups);
                }
            }
            
            node.setProperty(NodeConstants.Info.PROJECT_COLS, newElements);
            
        } else if(type == NodeConstants.Types.JOIN) { 
            // Convert join criteria property
            List joinCrits = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
            if(joinCrits != null && joinCrits.size() > 0) { 
                Iterator critIter = joinCrits.iterator();
                while(critIter.hasNext()) { 
                    Criteria crit = (Criteria) critIter.next();
                    convertCriteria(crit, symbolMap);
                }   
            }
            
            convertAccessPatterns(symbolMap, node);
        
        } else if(type == NodeConstants.Types.SORT) { 
            List elements = (List) node.getProperty(NodeConstants.Info.SORT_ORDER);         
            List newElements = new ArrayList(elements.size());
            
            Iterator elementIter = elements.iterator();
            while(elementIter.hasNext()) { 
                SingleElementSymbol symbol = (SingleElementSymbol) elementIter.next();
                SingleElementSymbol mappedSymbol = convertSingleElementSymbol(symbol, symbolMap, true);
                newElements.add( mappedSymbol );
                
                if (!singleMapping) {
                    GroupsUsedByElementsVisitor.getGroups(mappedSymbol, groups);
                }
            }
            
            node.setProperty(NodeConstants.Info.SORT_ORDER, newElements);   
            
        } else if(type == NodeConstants.Types.GROUP) {  
            // Grouping columns 
            List groupCols = (List) node.getProperty(NodeConstants.Info.GROUP_COLS);
            if(groupCols != null) {
                List newGroupCols = new ArrayList(groupCols.size());            
                Iterator groupIter = groupCols.iterator();                
                while(groupIter.hasNext()) { 
                    SingleElementSymbol groupCol = (SingleElementSymbol) groupIter.next();
                    Expression mappedCol = convertSingleElementSymbol(groupCol, symbolMap, false);                   
                    newGroupCols.add( mappedCol );
                    
                    if (!singleMapping) {
                        GroupsUsedByElementsVisitor.getGroups(mappedCol, groups);
                    }
                }                        
                node.setProperty(NodeConstants.Info.GROUP_COLS, newGroupCols);            
            }               
        } else if (type == NodeConstants.Types.SOURCE || type == NodeConstants.Types.ACCESS) {
            convertAccessPatterns(symbolMap, node);
        }
    }
    
    static SingleElementSymbol convertSingleElementSymbol(SingleElementSymbol symbol, Map symbolMap, boolean shouldAlias) {
    
        // Preserve old "short name" of alias by aliasing
        String name = symbol.getShortName();
        
        Expression mappedExpression = convertExpression(SymbolMap.getExpression(symbol), symbolMap);
        
        // Convert symbol using symbol map  
        SingleElementSymbol mappedSymbol = null;
        
        if(!(mappedExpression instanceof SingleElementSymbol)) { 
            mappedSymbol = new ExpressionSymbol(name, mappedExpression);
        } else {
            mappedSymbol = (SingleElementSymbol)mappedExpression;
        }
        
        // Re-alias to maintain name if necessary
        if(shouldAlias && (mappedSymbol instanceof ExpressionSymbol || !mappedSymbol.getShortCanonicalName().equals(name.toUpperCase()))) { 
            mappedSymbol = new AliasSymbol(name, mappedSymbol);
        }   
        
        return mappedSymbol;
    }   
    
    static Expression convertExpression(Expression expression, Map symbolMap) {
        
        if (expression == null || expression instanceof Constant) {
            return expression;
        }
        
        if(expression instanceof ElementSymbol) { 
            Expression mappedSymbol = (Expression) symbolMap.get(expression);
            if (mappedSymbol != null) {
                return mappedSymbol;
            }
            return expression;
        }
        
        if(expression instanceof AggregateSymbol) {
            AggregateSymbol aggSymbol = (AggregateSymbol) expression;
            
            // First try to replace the entire aggregate
            SingleElementSymbol replacement = (SingleElementSymbol) symbolMap.get(aggSymbol);
            if(replacement != null) {
                return replacement;
            }
        } 
        
        ExpressionMappingVisitor emv = new ExpressionMappingVisitor(symbolMap);
        
        PreOrderNavigator.doVisit(expression, emv);
        
        return expression;
    }   
        
    static Criteria convertCriteria(Criteria criteria, final Map symbolMap)
        throws QueryPlannerException {

        ExpressionMappingVisitor emv = new ExpressionMappingVisitor(symbolMap);
        
        PostOrderNavigator.doVisit(criteria, emv);
        
        // Simplify criteria if possible
        try {
            return QueryRewriter.rewriteCriteria(criteria, null, null, null);
        } catch(QueryValidatorException e) {
            throw new QueryPlannerException(e, QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0023, criteria));
        }
    }

    /**
     * creates a symbol map of elements in oldGroup mapped to corresponding elements in newGroup
     * 
     * if newGroup is null, then a mapping of oldGroup elements to null constants will be returned
     *  
     * @param oldGroup
     * @param newGroup
     * @param metadata
     * @return
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    public static Map<ElementSymbol, Expression> buildSymbolMap(GroupSymbol oldGroup, GroupSymbol newGroup, QueryMetadataInterface metadata) 
        throws QueryMetadataException, MetaMatrixComponentException {

        String newGroupName = null;
        if (newGroup != null) {
            newGroupName = newGroup.getName();
        }
        Map map = new HashMap();    

        // Get elements of old group
        List elements = ResolverUtil.resolveElementsInGroup(oldGroup, metadata);
        
        Iterator iter = elements.iterator();
        while(iter.hasNext()) {
            ElementSymbol oldElementSymbol = (ElementSymbol)iter.next();
            
            Expression symbol = null;
            if (newGroup != null) {
                String newFullName = metadata.getFullElementName(newGroupName, oldElementSymbol.getShortName());
                ElementSymbol newElementSymbol = new ElementSymbol(newFullName);
                newElementSymbol.setGroupSymbol(newGroup);
                newElementSymbol.setMetadataID(oldElementSymbol.getMetadataID());
                String elementType = metadata.getElementType(newElementSymbol.getMetadataID());
                newElementSymbol.setType(DataTypeManager.getDataTypeClass(elementType));
                symbol = newElementSymbol;
            } else {
                symbol = new Constant(null, oldElementSymbol.getType());
            }
            
            // Update map
            map.put(oldElementSymbol, symbol);                
        }
        
        return map;
    }
    
    static boolean hasSubquery(PlanNode critNode) {
        Criteria crit = (Criteria)critNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
        Collection subCrits = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(crit);
        return (subCrits.size() > 0);
    }

    /**
     * Find the SOURCE, SET_OP, JOIN, or NULL node that originates the given groups (typically from a criteria node).
     * In the case of join nodes the best fit will be found rather than just the first
     * join node that contains all the groups. 
     *
     * Returns null if the originating node cannot be found.
     * 
     * @param root
     * @param groups
     * @return
     */
    static PlanNode findOriginatingNode(PlanNode root, Set groups) {
        return findOriginatingNode(root, groups, false);
    }
    
    /**
     * 
     * Find the ACCESS, SOURCE, SET_OP, JOIN, or NULL node that originates the given groups, but will stop at the
     * first join node rather than searching for the best fit.
     * 
     * Returns null if the originating node cannot be found.
     * 
     * @param root
     * @param groups
     * @return
     */
    static PlanNode findJoinSourceNode(PlanNode root) {
        return findOriginatingNode(root, root.getGroups(), true);
    }
    
    private static PlanNode findOriginatingNode(PlanNode root, Set groups, boolean joinSource) {
        boolean containsGroups = false;
        
    	if(root.getType() == NodeConstants.Types.NULL || root.getType() == NodeConstants.Types.SOURCE 
                        || root.getType() == NodeConstants.Types.JOIN || root.getType() == NodeConstants.Types.SET_OP ||
                        (joinSource && root.getType() == NodeConstants.Types.ACCESS)) {
    	    
            //if there are no groups then the first possible match is the one we want
            if(groups.isEmpty()) {
               return root;
            }
            
            containsGroups = root.getGroups().containsAll(groups);
            
            if (containsGroups) {
                //if this is a group, source, or set op we're done, else if join make sure the groups match
                if (root.getType() != NodeConstants.Types.JOIN || joinSource || root.getGroups().size() == groups.size()) {
                    return root;
                }
            }
            
            //check to see if a recursion is not necessary
            if (root.getType() != NodeConstants.Types.JOIN || joinSource || !containsGroups) {
                return null;
            }
        }
    
    	// Check children, left to right
    	for (PlanNode child : root.getChildren()) {
    		PlanNode found = findOriginatingNode(child, groups, joinSource);
    		if(found != null) {
    			return found;
    		}
    	}
        
        //look for best fit instead after visiting children
        if(root.getType() == NodeConstants.Types.JOIN && containsGroups) {
            return root;
        }
        
    	// Not here
    	return null;
    }
    
    /**
     * Replaces the given node with a NULL node.  This will also preserve
     * the groups that originate under the node on the NULL node 
     *  
     * @param node
     */
    static void replaceWithNullNode(PlanNode node) {
        PlanNode nullNode = NodeFactory.getNewNode(NodeConstants.Types.NULL);
        PlanNode source = FrameUtil.findJoinSourceNode(node);
        if (source != null) {
            nullNode.addGroups(source.getGroups());
        }
        node.getParent().replaceChild(node, nullNode);
    }

    /**
     * Look for SOURCE node either one or two steps below the access node.  Typically
     * these options look like ACCESS-SOURCE or ACCESS-PROJECT-SOURCE.
     * 
     * @param accessNode
     * @return
     */
    static ProcessorPlan getNestedPlan(PlanNode accessNode) {
        ProcessorPlan plan = null;
        PlanNode sourceNode = accessNode.getFirstChild(); 
        if(sourceNode.getType() != NodeConstants.Types.SOURCE) {
            sourceNode = sourceNode.getFirstChild();
        }
        if(sourceNode.getType() == NodeConstants.Types.SOURCE) {
            plan = (ProcessorPlan) sourceNode.getProperty(NodeConstants.Info.PROCESSOR_PLAN);
        }
        return plan;            
    }

    /**
     * Look for SOURCE node either one or two steps below the access node.  Typically
     * these options look like ACCESS-SOURCE or ACCESS-PROJECT-SOURCE.
     *
     * @param accessNode
     * @return The actual stored procedure
     */
    static Command getNonQueryCommand(PlanNode node) {
        if (node.getChildCount() == 0) {
            return null;
        }
        PlanNode sourceNode = node.getFirstChild();
        if(sourceNode.getType() != NodeConstants.Types.SOURCE) {
            if (sourceNode.getChildCount() == 0) {
                return null;
            }
            sourceNode = sourceNode.getFirstChild();
        } 
        if(sourceNode.getType() == NodeConstants.Types.SOURCE) {
            Command command = (Command) sourceNode.getProperty(NodeConstants.Info.VIRTUAL_COMMAND);
            if(! (command instanceof QueryCommand)) {
                return command;
            }                
        }
        
        return null;
    }
    
    static boolean isProcedure(PlanNode projectNode) {
        if(projectNode.getType() == NodeConstants.Types.PROJECT && projectNode.getChildCount() > 0) {
            PlanNode accessNode = projectNode.getFirstChild();
            Command command = getNonQueryCommand(accessNode);
            return command instanceof StoredProcedure;
        }
        return false;
    }
    
    /**
     * Finds the closest project columns in the current frame
     */
    static List<SingleElementSymbol> findTopCols(PlanNode node) {
        List projects = NodeEditor.findAllNodes(node, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE|NodeConstants.Types.PROJECT);
        PlanNode project = null;
        if (projects.isEmpty()) {
            project = NodeEditor.findParent(node, NodeConstants.Types.PROJECT, NodeConstants.Types.SOURCE);
        } else {
            project = (PlanNode)projects.get(0);
        }
        if (project != null) {
            return (List<SingleElementSymbol>)project.getProperty(NodeConstants.Info.PROJECT_COLS);
        }
        Assertion.failed("no top cols in frame");
        return null;
    }

}
