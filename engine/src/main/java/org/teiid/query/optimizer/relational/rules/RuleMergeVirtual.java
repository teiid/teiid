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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.resolver.util.AccessPattern;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;


public final class RuleMergeVirtual implements
                                   OptimizerRule {

    public PlanNode execute(PlanNode plan,
                            QueryMetadataInterface metadata,
                            CapabilitiesFinder capFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   TeiidComponentException {
    	boolean beforeDecomposeJoin = rules.contains(RuleConstants.DECOMPOSE_JOIN);
        for (PlanNode sourceNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
            if (sourceNode.getChildCount() > 0) {
                plan = doMerge(sourceNode, plan, beforeDecomposeJoin, metadata);
            }
        }

        return plan;
    }

    static PlanNode doMerge(PlanNode frame,
                            PlanNode root, boolean beforeDecomposeJoin,
                            QueryMetadataInterface metadata) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        GroupSymbol virtualGroup = frame.getGroups().iterator().next();

        // check to see if frame represents a proc relational query.
        if (virtualGroup.isProcedure()) {
            return root;
        }
        
        SymbolMap references = (SymbolMap)frame.getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
        if (references != null) {
        	return root; //correlated nested table commands should not be merged
        }

        PlanNode parentProject = NodeEditor.findParent(frame, NodeConstants.Types.PROJECT);

        // Check whether the upper frame is a SELECT INTO
        if (parentProject.getProperty(NodeConstants.Info.INTO_GROUP) != null) {
            return root;
        }

        if (!FrameUtil.canConvertAccessPatterns(frame)) {
            return root;
        }

        PlanNode projectNode = frame.getFirstChild();

        // Check if lower frame has only a stored procedure execution - this cannot be merged to parent frame
        if (FrameUtil.isProcedure(projectNode)) {
            return root;
        }
        
        SymbolMap symbolMap = (SymbolMap)frame.getProperty(NodeConstants.Info.SYMBOL_MAP);
        
        PlanNode sortNode = NodeEditor.findParent(parentProject, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
        
        if (sortNode != null && sortNode.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT)) {
        	OrderBy sortOrder = (OrderBy)sortNode.getProperty(NodeConstants.Info.SORT_ORDER);
        	boolean unrelated = false;
        	for (OrderByItem item : sortOrder.getOrderByItems()) {
        		if (!item.isUnrelated()) {
        			continue;
        		}
        		Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(item.getSymbol(), true);
        		for (ElementSymbol elementSymbol : elements) {
					if (virtualGroup.equals(elementSymbol.getGroupSymbol())) {
						unrelated = true;
					}
				}
			}
        	// the lower frame cannot contain DUP_REMOVE, GROUP, UNION if unrelated
        	if (unrelated && NodeEditor.findNodePreOrder(frame, NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.PROJECT) != null
        			|| NodeEditor.findNodePreOrder(frame, NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE) != null
        			|| NodeEditor.findNodePreOrder(frame, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE) != null) {
        		return root;
        	}
        }

        //try to remove the virtual layer if we are only doing a simple projection in the following cases:
        // 1. if the frame root is something other than a project (SET_OP, SORT, LIMIT, etc.)
        // 2. if the frame has a grouping node
        // 3. if the frame has no sources
        if (projectNode.getType() != NodeConstants.Types.PROJECT
            || NodeEditor.findNodePreOrder(frame.getFirstChild(), NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE
                                                                                             | NodeConstants.Types.JOIN) != null
            || NodeEditor.findAllNodes(frame.getFirstChild(), NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE).isEmpty()) {
        	
            PlanNode parentSource = NodeEditor.findParent(parentProject, NodeConstants.Types.SOURCE);
            if (beforeDecomposeJoin && parentSource != null && parentSource.hasProperty(Info.PARTITION_INFO) 
            		&& !NodeEditor.findAllNodes(frame.getFirstChild(), NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE).isEmpty()) {
            	return root; //don't bother to merge until after
            }

            return checkForSimpleProjection(frame, root, parentProject, metadata);
        }

        PlanNode parentJoin = NodeEditor.findParent(frame, NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);

        if (!checkJoinCriteria(frame, virtualGroup, parentJoin)) {
            return root;
        }
        
        if (!checkProjectedSymbols(projectNode, virtualGroup, parentJoin, metadata)) {
            return root;
        }

        // Otherwise merge should work

        // Convert parent frame before merge
        FrameUtil.convertFrame(frame, virtualGroup, FrameUtil.findJoinSourceNode(projectNode).getGroups(), symbolMap.asMap(), metadata);

        PlanNode parentBottom = frame.getParent();
        prepareFrame(frame);

        // Remove top 2 nodes (SOURCE, PROJECT) of virtual group - they're no longer needed
        NodeEditor.removeChildNode(parentBottom, frame);
        NodeEditor.removeChildNode(parentBottom, projectNode);

        return root;
    }

    private static void prepareFrame(PlanNode frame) {
        // find the new root of the frame so that access patterns can be propagated
        PlanNode newRoot = FrameUtil.findJoinSourceNode(frame.getFirstChild());
        if (newRoot != null) {
            Collection<AccessPattern> ap = (Collection)frame.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
            if (ap != null) {
                Collection<AccessPattern> newAp = (Collection)newRoot.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
                if (newAp == null) {
                    newRoot.setProperty(NodeConstants.Info.ACCESS_PATTERNS, ap);
                } else {
                    newAp.addAll(ap);
                }
            }
            RulePlaceAccess.copyDependentHints(frame, newRoot);
        }
    }

    /**
     * Removes source layers that only do a simple projection of the elements below.
     * @throws TeiidComponentException 
     * @throws QueryMetadataException 
     */
    private static PlanNode checkForSimpleProjection(PlanNode frame,
                                                     PlanNode root,
                                                     PlanNode parentProject,
                                                     QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        // check that the parent only performs projection
        PlanNode nodeToCheck = parentProject.getFirstChild();
        while (nodeToCheck != frame) {
            if (nodeToCheck.getType() != NodeConstants.Types.SELECT
                || !nodeToCheck.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)) {
                return root;
            }
            nodeToCheck = nodeToCheck.getFirstChild();
        }
        
        if (frame.getFirstChild().getType() == NodeConstants.Types.TUPLE_LIMIT
            && NodeEditor.findParent(parentProject,
                                     NodeConstants.Types.SORT | NodeConstants.Types.DUP_REMOVE,
                                     NodeConstants.Types.SOURCE | NodeConstants.Types.SET_OP) != null) {
            return root;
        }
        
        List<? extends SingleElementSymbol> requiredElements = RuleAssignOutputElements.determineSourceOutput(frame, new ArrayList<SingleElementSymbol>(), metadata, null);
        List<SingleElementSymbol> selectSymbols = (List<SingleElementSymbol>)parentProject.getProperty(NodeConstants.Info.PROJECT_COLS);

        // check that it only performs simple projection and that all required symbols are projected
        LinkedHashSet<ElementSymbol> symbols = new LinkedHashSet<ElementSymbol>(); //ensuring there are no duplicates prevents problems with subqueries  
        for (SingleElementSymbol symbol : selectSymbols) {
            Expression expr = SymbolMap.getExpression(symbol);
            if (!(expr instanceof ElementSymbol)) {
                return root;
            }
            requiredElements.remove(expr);
            if (!symbols.add((ElementSymbol)expr)) {
                return root;
            }
        }
        if (!requiredElements.isEmpty()) {
            return root;
        }
        
        // re-order the lower projects
        RuleAssignOutputElements.filterVirtualElements(frame, new ArrayList<SingleElementSymbol>(symbols), metadata);

        // remove phantom select nodes
        nodeToCheck = parentProject.getFirstChild();
        while (nodeToCheck != frame) {
            PlanNode current = nodeToCheck;
            nodeToCheck = nodeToCheck.getFirstChild();
            NodeEditor.removeChildNode(current.getParent(), current);
        }
        
        if (NodeEditor.findParent(parentProject, NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.SOURCE) != null) {
            PlanNode lowerDup = NodeEditor.findNodePreOrder(frame.getFirstChild(), NodeConstants.Types.DUP_REMOVE, NodeConstants.Types.PROJECT);
            if (lowerDup != null) {
                NodeEditor.removeChildNode(lowerDup.getParent(), lowerDup);
            }

            PlanNode setOp = NodeEditor.findNodePreOrder(frame.getFirstChild(), NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
            if (setOp != null) {
                setOp.setProperty(NodeConstants.Info.USE_ALL, Boolean.FALSE);
                if (parentProject.getParent().getParent() != null) {
                    NodeEditor.removeChildNode(parentProject.getParent().getParent(), parentProject.getParent());
                } else {
                    parentProject.removeFromParent();
                    root = parentProject;
                }
            }
        }

        correctOrderBy(frame, selectSymbols, parentProject);
        PlanNode parentSource = NodeEditor.findParent(frame, NodeConstants.Types.SOURCE);        
        PlanNode parentSetOp = NodeEditor.findParent(parentProject, NodeConstants.Types.SET_OP, NodeConstants.Types.SOURCE);
  
        if (parentSetOp == null || NodeEditor.findNodePreOrder(parentSetOp, NodeConstants.Types.PROJECT) == parentProject) {
	        if (parentSource != null) {
	        	FrameUtil.correctSymbolMap(((SymbolMap)frame.getProperty(NodeConstants.Info.SYMBOL_MAP)).asMap(), parentSource);
	        }
	        if (parentSetOp != null) {
	        	correctOrderBy(frame, selectSymbols, parentSetOp);
	        }
        }
        
        prepareFrame(frame);
        //remove the parent project and the source node
        NodeEditor.removeChildNode(parentProject, frame);
        if (parentProject.getParent() == null) {
            root = parentProject.getFirstChild();
            parentProject.removeChild(root);
            return root;
        } 
        NodeEditor.removeChildNode(parentProject.getParent(), parentProject);
                 
        return root;
    }

	private static void correctOrderBy(PlanNode frame,
			List<SingleElementSymbol> selectSymbols, PlanNode startNode) {
		PlanNode sort = NodeEditor.findParent(startNode, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE | NodeConstants.Types.SET_OP);
		if (sort != null) { //special handling is needed since we are retaining the child aliases
			List<SingleElementSymbol> childProject = (List<SingleElementSymbol>)NodeEditor.findNodePreOrder(frame, NodeConstants.Types.PROJECT).getProperty(NodeConstants.Info.PROJECT_COLS);
			OrderBy elements = (OrderBy)sort.getProperty(NodeConstants.Info.SORT_ORDER);
			for (OrderByItem item : elements.getOrderByItems()) {
				item.setSymbol(childProject.get(selectSymbols.indexOf(item.getSymbol())));
			}
		    sort.getGroups().clear();
		    sort.addGroups(GroupsUsedByElementsVisitor.getGroups(elements));
		}
	}
    
    /**
     * Check to ensure that we are not projecting a subquery or null dependent expressions
     */
    private static boolean checkProjectedSymbols(PlanNode projectNode,
                                                 GroupSymbol virtualGroup,
                                                 PlanNode parentJoin,
                                                 QueryMetadataInterface metadata) {
        List<SingleElementSymbol> selectSymbols = (List<SingleElementSymbol>)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);
        
        HashSet<GroupSymbol> groups = new HashSet<GroupSymbol>();
        for (PlanNode sourceNode : NodeEditor.findAllNodes(projectNode, NodeConstants.Types.SOURCE, NodeConstants.Types.SOURCE)) {
            groups.addAll(sourceNode.getGroups());
        }

        boolean checkForNullDependent = false;
        // check to see if there are projected literal on the inner side of an outer join that needs to be preserved
        if (parentJoin != null) {
            PlanNode joinToTest = parentJoin;
            while (joinToTest != null) {
                JoinType joinType = (JoinType)joinToTest.getProperty(NodeConstants.Info.JOIN_TYPE);
                if (joinType == JoinType.JOIN_FULL_OUTER) {
                    checkForNullDependent = true;
                    break;
                } else if (joinType == JoinType.JOIN_LEFT_OUTER
                           && FrameUtil.findJoinSourceNode(joinToTest.getLastChild()).getGroups().contains(virtualGroup)) {
                    checkForNullDependent = true;
                    break;
                }
                joinToTest = NodeEditor.findParent(joinToTest.getParent(), NodeConstants.Types.JOIN, NodeConstants.Types.SOURCE);
            }
        }

        for (int i = 0; i < selectSymbols.size(); i++) {
        	SingleElementSymbol symbol = selectSymbols.get(i);
            Collection scalarSubqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(symbol);
            if (!scalarSubqueries.isEmpty()) {
                return false;
            }
            if (checkForNullDependent && JoinUtil.isNullDependent(metadata, groups, SymbolMap.getExpression(symbol))) {
                return false;
            }
            // TEIID-16: We do not want to merge a non-deterministic scalar function
            if (FunctionCollectorVisitor.isNonDeterministic(symbol)) {
            	return false;
            }
        }

        return true;
    }

    /**
     * check to see if criteria is used in a full outer join or has no groups and is on the inner side of an outer join. if this
     * is the case then the layers cannot be merged, since merging would possibly force the criteria to change it's position (into
     * the on clause or above the join).
     */
    private static boolean checkJoinCriteria(PlanNode frame,
                                             GroupSymbol virtualGroup,
                                             PlanNode parentJoin) {
        if (parentJoin != null) {
            List<PlanNode> selectNodes = NodeEditor.findAllNodes(frame.getFirstChild(),
                                                                 NodeConstants.Types.SELECT,
                                                                 NodeConstants.Types.SOURCE);
            Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
            groups.add(virtualGroup);
            for (PlanNode selectNode : selectNodes) {
                if (selectNode.hasBooleanProperty(NodeConstants.Info.IS_PHANTOM)) {
                    continue;
                }
                JoinType jt = JoinUtil.getJoinTypePreventingCriteriaOptimization(parentJoin, groups);

                if (jt != null && (jt == JoinType.JOIN_FULL_OUTER || selectNode.getGroups().size() == 0)) {
                    return false;
                }
            }
        }
        return true;
    }

    public String toString() {
        return "MergeVirtual"; //$NON-NLS-1$
    }

}
