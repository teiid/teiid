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

package org.teiid.query.optimizer.relational.plantree;

import java.util.LinkedList;
import java.util.List;

/**
 * This class is no longer really an editor.  Those methods have been moved over to PlanNode.
 * TODO: rename NodeFinder or merge completely with PlanNode
 */
public final class NodeEditor {

    // Can't construct
    private NodeEditor() {
    }

    /**
     * all of child's children become children of parent
     */
    public static final void removeChildNode(PlanNode parent, PlanNode child) {
        if (child.getChildCount() == 0) {
            parent.removeChild(child);
        } else if (child.getChildCount() == 1){
            PlanNode grandChild = child.getFirstChild();
            parent.replaceChild(child, grandChild);
        } else {
            throw new AssertionError("Cannot promote a multinode child"); //$NON-NLS-1$
        }
    }

    public static final PlanNode findNodePreOrder(PlanNode root, int types) {
        return findNodePreOrder(root, types, NodeConstants.Types.NO_TYPE);
    }

    public static final PlanNode findNodePreOrder(PlanNode root, int types, int stopTypes) {
        if (root == null) {
            return null;
        }
        if((types & root.getType()) == root.getType()) {
            return root;
        } else if((stopTypes & root.getType()) == root.getType()) {
            return null;
        } else if(root.getChildCount() > 0) {
            for (PlanNode child : root.getChildren()) {
                PlanNode found = findNodePreOrder(child, types, stopTypes);
                if(found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    public static final PlanNode findParent(PlanNode root,
                                            int types) {
        return findParent(root, types, NodeConstants.Types.NO_TYPE);
    }

    /**
     * Return the first parent node of the given type stopping at the given nodes.
     * The matching will start at the parent of the node passed in.
     * @param root
     * @param types a bitwise and of type values
     * @param stopTypes a bitwise and of type values to stop at
     * @return the matching parent, or null one is not found
     */
    public static final PlanNode findParent(PlanNode root,
                                            int types,
                                            int stopTypes) {
        while (root != null && root.getParent() != null) {
            root = root.getParent();
            if ((types & root.getType()) == root.getType()) {
                return root;
            }
            if ((stopTypes & root.getType()) == root.getType()) {
                return null;
            }
        }
        return null;
    }

    public static final List<PlanNode> findAllNodes(PlanNode root, int types) {
        LinkedList<PlanNode> nodes = new LinkedList<PlanNode>();
        findAllNodesHelper(root, types, nodes, NodeConstants.Types.NO_TYPE);
        return nodes;
    }

    /**
     * Find all nodes of a type, starting at the root of a tree or subtree of
     * PlanNodes and searching downward, but not searching past nodes of type equal
     * to stopType.
     * @param root the top node of the subtree, the point at which searching begins
     * @param types the types of the node to search for
     * @param stopTypes type of nodes not to search past
     * @return Collection of found PlanNodes
     * @since 4.2
     */
    public static final List<PlanNode> findAllNodes(PlanNode root, int types, int stopTypes) {
        LinkedList<PlanNode> nodes = new LinkedList<PlanNode>();
        findAllNodesHelper(root, types, nodes, stopTypes);
        return nodes;
    }

    private static final void findAllNodesHelper(PlanNode node, int types, List<PlanNode> foundNodes, int stopTypes) {
        if((node.getType() & types) == node.getType()) {
            foundNodes.add(node);
        }

        if(node.getChildCount() > 0 && (stopTypes == NodeConstants.Types.NO_TYPE || (stopTypes & node.getType()) != node.getType() ) ) {
            for (PlanNode child : node.getChildren()) {
                findAllNodesHelper(child, types, foundNodes, stopTypes);
            }
        }
    }

}
