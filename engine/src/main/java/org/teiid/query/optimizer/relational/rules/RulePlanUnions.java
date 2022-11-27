/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.util.CommandContext;


/**
 *  Organizes union branches so that push down is possible.  This does not check to actually ensure that push down will happen.
 */
public class RulePlanUnions implements OptimizerRule {

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

        optimizeUnions(plan, metadata, capabilitiesFinder);

        return plan;
    }

    /**
     * @param plan
     * @param metadata
     * @param capabilitiesFinder
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    private void optimizeUnions(PlanNode plan,
                                QueryMetadataInterface metadata,
                                CapabilitiesFinder capabilitiesFinder) throws QueryMetadataException,
                                                                      TeiidComponentException {
        //look for all union branches and their sources
        for (PlanNode unionNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SET_OP, NodeConstants.Types.SET_OP | NodeConstants.Types.ACCESS)) {
            List<PlanNode> accessNodes = NodeEditor.findAllNodes(unionNode, NodeConstants.Types.ACCESS);

            Object id = getModelId(metadata, accessNodes, capabilitiesFinder);

            //check to see if this union is already to the same source
            if (id != null) {
                continue;
            }

            //a linked hashmap is used so that the first entry is logically the first branch
            Map<Object, List<PlanNode>> sourceNodes = new LinkedHashMap<Object, List<PlanNode>>();

            boolean all = unionNode.hasBooleanProperty(NodeConstants.Info.USE_ALL);
            Operation op = (Operation)unionNode.getProperty(NodeConstants.Info.SET_OPERATION);

            collectUnionSources(metadata, capabilitiesFinder, unionNode, sourceNodes, all, op);

            if (sourceNodes.size() == 1) {
                continue;
            }

            //rebuild unions based upon the source map
            boolean shouldRebuild = false;

            for (Map.Entry<Object, List<PlanNode>> entry : sourceNodes.entrySet()) {
                if (entry.getKey() != null
                        && entry.getValue().size() > 1
                        && CapabilitiesUtil.supportsSetOp(entry.getKey(), (Operation)unionNode.getProperty(NodeConstants.Info.SET_OPERATION), metadata, capabilitiesFinder)) {
                    shouldRebuild = true;
                    break;
                }
            }

            if (!shouldRebuild) {
                continue;
            }

            List<PlanNode> sourceUnions = new LinkedList<PlanNode>();

            for (Map.Entry<Object, List<PlanNode>> entry : sourceNodes.entrySet()) {
                List<PlanNode> sources = entry.getValue();

                sourceUnions.add(buildUnionTree(unionNode, sources));
            }

            PlanNode tempRoot = buildUnionTree(unionNode, sourceUnions);

            unionNode.removeAllChildren();
            unionNode.addChildren(tempRoot.removeAllChildren());
        }
    }

    static PlanNode buildUnionTree(PlanNode rootUnionNode,
                                List<PlanNode> sources) {

        PlanNode root = null;

        for (PlanNode source : sources) {
            if (root == null) {
                root = source;
            } else {
                PlanNode union = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
                union.setProperty(NodeConstants.Info.SET_OPERATION, rootUnionNode.getProperty(NodeConstants.Info.SET_OPERATION));
                union.setProperty(NodeConstants.Info.USE_ALL, rootUnionNode.getProperty(NodeConstants.Info.USE_ALL));
                union.addLastChild(root);
                union.addLastChild(source);
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
                                     Map<Object, List<PlanNode>> sourceNodes,
                                     boolean all, Operation setOp) throws QueryMetadataException,
                                                 TeiidComponentException {
        for (PlanNode child : unionNode.getChildren()) {
            boolean directChildren = false;
            if (child.getType() == NodeConstants.Types.SET_OP) {
                if (!all && Operation.UNION == child.getProperty(NodeConstants.Info.SET_OPERATION)) {
                    //allow the parent to handle the dup removal
                    child.setProperty(NodeConstants.Info.USE_ALL, Boolean.TRUE);
                }
                directChildren = ((!all || child.hasBooleanProperty(NodeConstants.Info.USE_ALL)) && setOp.equals(child.getProperty(NodeConstants.Info.SET_OPERATION)) && setOp != Operation.EXCEPT);
            }
            List<PlanNode> accessNodes = NodeEditor.findAllNodes(child, NodeConstants.Types.ACCESS);

            Object id = getModelId(metadata, accessNodes, capabilitiesFinder);

            if (!directChildren || id != null) {
                // this is a far as we need to collect
                buildModelMap(metadata, capabilitiesFinder, sourceNodes, child, id);
            }
            if (id == null) {
                if (directChildren) {
                    // keep collecting children
                    collectUnionSources(metadata, capabilitiesFinder, child, sourceNodes, all, setOp);
                } else {
                    // recursively optimize below this point
                    optimizeUnions(child, metadata, capabilitiesFinder);
                }
            }
        }
    }

    static Object getModelId(QueryMetadataInterface metadata,
                            List<PlanNode> accessNodes, CapabilitiesFinder capFinder) throws QueryMetadataException,
                                             TeiidComponentException {
        Object modelID = null;

        for (PlanNode accessNode : accessNodes) {

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
     * @throws TeiidComponentException
     */
    static void buildModelMap(QueryMetadataInterface metadata,
                                   CapabilitiesFinder capFinder,
                                   Map<Object, List<PlanNode>> accessMap,
                                   PlanNode node,
                                   Object accessModelID) throws QueryMetadataException,
                                                        TeiidComponentException {
        List<PlanNode> accessNodes = accessMap.get(accessModelID);

        if (accessNodes == null) {
            for (Map.Entry<Object, List<PlanNode>> entry : accessMap.entrySet() ) {
                if (accessModelID == entry.getKey() || CapabilitiesUtil.isSameConnector(accessModelID, entry.getKey(), metadata, capFinder)) {
                    accessNodes = entry.getValue();
                    break;
                }
            }

            if (accessNodes == null) {
                accessNodes = new ArrayList<PlanNode>();
                accessMap.put(accessModelID, accessNodes);
            }
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
