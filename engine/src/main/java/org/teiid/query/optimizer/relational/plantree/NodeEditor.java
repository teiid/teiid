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
	 * @param type
	 * @param stopTypes
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
