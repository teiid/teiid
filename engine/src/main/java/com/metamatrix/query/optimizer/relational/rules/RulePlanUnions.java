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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.util.CommandContext;

/**
 *  Organizes union branches so that push down is possible.  This does not check to actually ensure that push down will happen.
 */
public class RulePlanUnions implements OptimizerRule {
    
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
        
        optimizeUnions(plan, metadata, capabilitiesFinder);
        
        return plan;
    }

    /** 
     * @param plan
     * @param metadata
     * @param capabilitiesFinder
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    private void optimizeUnions(PlanNode plan,
                                QueryMetadataInterface metadata,
                                CapabilitiesFinder capabilitiesFinder) throws QueryMetadataException,
                                                                      MetaMatrixComponentException {
        //look for all union branches and their sources
        for (PlanNode unionNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SET_OP, NodeConstants.Types.SET_OP | NodeConstants.Types.ACCESS)) {
            List accessNodes = NodeEditor.findAllNodes(unionNode, NodeConstants.Types.ACCESS);
            
            Object id = getModelId(metadata, accessNodes, capabilitiesFinder);
            
            //check to see if this union is already to the same source
            if (id != null) {
                continue;
            }
            
            //a linked hasmap is used so that the first entry is logically the first branch
            Map sourceNodes = new LinkedHashMap();
            
            boolean all = Boolean.TRUE.equals(unionNode.getProperty(NodeConstants.Info.USE_ALL));
            
            collectUnionSources(metadata, capabilitiesFinder, unionNode, sourceNodes, all, (Operation)unionNode.getProperty(NodeConstants.Info.SET_OPERATION));
            
            if (sourceNodes.size() == 1) {
                continue;
            }
            
            //rebuild unions based upon the source map
            boolean shouldRebuild = false;
            
            for (Iterator j = sourceNodes.entrySet().iterator(); j.hasNext();) {
                Map.Entry entry = (Map.Entry)j.next();
                
                if (entry.getKey() != null && ((List)entry.getValue()).size() > 1) {
                    shouldRebuild = true;
                    break;
                }
            }
            
            if (!shouldRebuild) {
                continue;
            }
            
            List sourceUnions = new LinkedList();
            
            for (Iterator j = sourceNodes.entrySet().iterator(); j.hasNext();) {
                Map.Entry entry = (Map.Entry)j.next();
                
                List sources = (List)entry.getValue();
                
                sourceUnions.add(buildUnionTree(unionNode, sources));
            }
            
            PlanNode tempRoot = buildUnionTree(unionNode, sourceUnions);
            
            unionNode.removeAllChildren();
            unionNode.addChildren(tempRoot.removeAllChildren());
        }
    }

    private PlanNode buildUnionTree(PlanNode rootUnionNode,
                                List sources) {
        
        PlanNode root = null;
        
        for (Iterator k = sources.iterator(); k.hasNext();) {
            PlanNode source = (PlanNode)k.next();
            
            if (root == null) {
                root = source;
            } else {
                PlanNode union = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
                union.setProperty(NodeConstants.Info.SET_OPERATION, rootUnionNode.getProperty(NodeConstants.Info.SET_OPERATION));
                union.setProperty(NodeConstants.Info.USE_ALL, rootUnionNode.getProperty(NodeConstants.Info.USE_ALL));
                union.addLastChild(root);
                union.addLastChild(source);
                union.addGroups(root.getGroups());
                union.addGroups(source.getGroups());
                root = union;
            }
        }
        
        return root;
    }

    /**
     * TODO: union and intersect are associative
     */
    private void collectUnionSources(QueryMetadataInterface metadata,
                                     CapabilitiesFinder capabilitiesFinder,
                                     PlanNode unionNode,
                                     Map sourceNodes,
                                     boolean all, Operation setOp) throws QueryMetadataException,
                                                 MetaMatrixComponentException {
        for (PlanNode child : unionNode.getChildren()) {
            if (child.getType() == NodeConstants.Types.SET_OP) {
            	if (!all && Operation.UNION == child.getProperty(NodeConstants.Info.SET_OPERATION)) {
            		//allow the parent to handle the dup removal
            		child.setProperty(NodeConstants.Info.USE_ALL, Boolean.TRUE);
            	}
                if ((!all || child.hasBooleanProperty(NodeConstants.Info.USE_ALL)) && setOp.equals(child.getProperty(NodeConstants.Info.SET_OPERATION)) && setOp != Operation.EXCEPT) { //keep collecting sources
                    List accessNodes = NodeEditor.findAllNodes(child, NodeConstants.Types.ACCESS);
                    
                    Object id = getModelId(metadata, accessNodes, capabilitiesFinder);
                    
                    if (id != null) {
                        buildModelMap(metadata, capabilitiesFinder, sourceNodes, child, id);
                    } else {
                        collectUnionSources(metadata, capabilitiesFinder, child, sourceNodes, all, setOp);
                    }
                } else { //recursively optimize
                    optimizeUnions(child, metadata, capabilitiesFinder);
                }
            } else {  //this must be a source, see if it has a consistent access node
                List accessNodes = NodeEditor.findAllNodes(child, NodeConstants.Types.ACCESS);
                
                Object id = getModelId(metadata, accessNodes, capabilitiesFinder);
                
                //don't bother optimizing sources that don't support unions
                boolean supportsUnions = true;
                
                if (id != null && !CapabilitiesUtil.supportsSetOp(id, (Operation)unionNode.getProperty(NodeConstants.Info.SET_OPERATION), metadata, capabilitiesFinder)) {
                    supportsUnions = false;
                    id = null;
                }
                
                buildModelMap(metadata, capabilitiesFinder, sourceNodes, child, id);

                if (id == null && supportsUnions) {
                    //recursively optimize below this point
                    optimizeUnions(child, metadata, capabilitiesFinder);
                }
            }
        }
    }

    private Object getModelId(QueryMetadataInterface metadata,
                            List accessNodes, CapabilitiesFinder capFinder) throws QueryMetadataException,
                                             MetaMatrixComponentException {
        Object modelID = null;
        
        for (Iterator k = accessNodes.iterator(); k.hasNext();) {
            PlanNode accessNode = (PlanNode)k.next();
        
            Object accessModelID = RuleRaiseAccess.getModelIDFromAccess(accessNode, metadata);
            
            if (accessModelID == null) {
                return null;
            }
            
            if(modelID == null) {
                modelID = accessModelID;
            } 
            
            if(! CapabilitiesUtil.isSameConnector(modelID, accessModelID, metadata, capFinder)) {
                return null;
            }
        }
        
        return modelID;
    }
    
    /**
     * Builds a mapping of models to access nodes.  The ordering of access nodes will be stable
     * and the model key takes into account whether the same connector is used.
     *  
     * @param metadata
     * @param capFinder
     * @param accessMap
     * @param node
     * @param accessModelID
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    static void buildModelMap(QueryMetadataInterface metadata,
                                   CapabilitiesFinder capFinder,
                                   Map accessMap,
                                   PlanNode node,
                                   Object accessModelID) throws QueryMetadataException,
                                                        MetaMatrixComponentException {
        List accessNodes = null;
        
        for (Iterator i = accessMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Map.Entry)i.next();
            if (accessModelID == entry.getKey() || CapabilitiesUtil.isSameConnector(accessModelID, entry.getKey(), metadata, capFinder)) {
                accessNodes = (List)entry.getValue();
                break;
            }
        }
        
        if (accessNodes == null) {
            accessNodes = new ArrayList();
            accessMap.put(accessModelID, accessNodes);
        }
        accessNodes.add(node);
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "PlanUnions"; //$NON-NLS-1$
    }

}
