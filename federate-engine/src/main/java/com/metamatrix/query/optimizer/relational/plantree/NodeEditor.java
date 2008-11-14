/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.optimizer.relational.plantree;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.util.ErrorMessageKeys;

public final class NodeEditor {

	// Can't construct
	private NodeEditor() {
	}

	public static final void attachFirst(PlanNode parent, PlanNode child) {
		if(child != null) {
			parent.addFirstChild(child);
			child.setParent(parent);
		}
	}

	public static final void attachLast(PlanNode parent, PlanNode child) {
		if(child != null) {
			parent.addLastChild(child);
			child.setParent(parent);
		}
	}

	public static final PlanNode cutFirst(PlanNode node) {
		if(node.getChildCount() == 0) {
			return null;
		}
		PlanNode temp = node.getFirstChild();
		Assertion.isNotNull(temp);
		node.removeChild(temp);
		temp.setParent(null);
		return temp;
	}

	public static final PlanNode cutLast(PlanNode node) {
		if(node.getChildCount() == 0) {
			return null;
		}
		PlanNode temp = node.getLastChild();
		Assertion.isNotNull(temp);
		node.removeChild(temp);
		temp.setParent(null);
		return temp;
	}

	// Replace child with insert and make child one of insert's children.
	public static final void insertNode(PlanNode parent, PlanNode child, PlanNode insert) {
		// Replace child with insert in parent
		List children = parent.getChildren();
		int index = children.indexOf(child);
		Assertion.isNonNegative(index);
		children.set(index, insert);
		insert.setParent(parent);

		// Now attach insert to child properly
		child.setParent(insert);
		insert.addLastChild(child);
	}

	// all of child's children become children of parent
	public static final void removeChildNode(PlanNode parent, PlanNode child) {
        // Get all existing children from parent
        List newChildren = new LinkedList(parent.getChildren());

        // get child's children
        List orphans = child.getChildren();

        // Find the child being removed and replace with the child's children
        ListIterator childIter = newChildren.listIterator();
        while(childIter.hasNext()) {
            PlanNode possibleChild = (PlanNode) childIter.next();
            if(possibleChild == child) {
                childIter.remove();
                Iterator orphanIter = orphans.iterator();
                while(orphanIter.hasNext()) {
                    PlanNode orphan = (PlanNode) orphanIter.next();
                    childIter.add(orphan);
                }
            }
        }

        // Remove all children from parent and re-add from newChildren
        Iterator removeIter = new LinkedList(parent.getChildren()).iterator();
        while(removeIter.hasNext()) {
            PlanNode removeNode = (PlanNode) removeIter.next();
            parent.removeChild(removeNode);
        }
        parent.addChildren(newChildren);

		// set new children's parent to new parent
		Iterator iter = newChildren.iterator();
		while(iter.hasNext()) {
			PlanNode newChild = (PlanNode) iter.next();
			newChild.setParent(parent);
		}

        // Remove old children from child
        removeIter = new LinkedList(child.getChildren()).iterator();
        while(removeIter.hasNext()) {
            child.removeChild((PlanNode)removeIter.next());
        }
	}
	
    public static final PlanNode findNodePreOrder(PlanNode root, int type) {
        return findNodePreOrder(root, type, NodeConstants.Types.NO_TYPE);
    }

	public static final PlanNode findNodePreOrder(PlanNode root, int type, int stopTypes) {
		if(root.getType() == type) {
			return root;
		} else if((stopTypes & root.getType()) == root.getType()) {
		    return null;
		} else if(root.getChildCount() > 0) {
		    for (PlanNode child : root.getChildren()) {
				PlanNode found = findNodePreOrder(child, type, stopTypes);
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

	/**
	 * Replace a single node with a another node.  Any children of the original
	 * node become children of the replacement node.
	 * @param originalNode Node to replace
	 * @param replacementNode The replacement node
	 */
	public static final void replaceNode(PlanNode originalNode, PlanNode replacementNode) {
		// Get everything around the original node
		PlanNode parent = originalNode.getParent();
        List children = new LinkedList(parent.getChildren());

        // Detach all children from parent
        Iterator childIter = children.iterator();
        while(childIter.hasNext()) {
            PlanNode child = (PlanNode) childIter.next();
            parent.removeChild(child);
        }

		// Re-attach children, replacing replacementNode for originalNode
        childIter = children.iterator();
        while(childIter.hasNext()) {
            PlanNode child = (PlanNode) childIter.next();
            if(child == originalNode) {
                parent.addLastChild(replacementNode);
            } else {
                parent.addLastChild(child);
            }
        }

		// Attach children to replacement node
		Iterator orphanIter = originalNode.getChildren().iterator();
		while(orphanIter.hasNext()) {
			NodeEditor.attachLast(replacementNode, (PlanNode) orphanIter.next());
		}
	}

	/**
	 * Replace a single node with a left-linear tree of nodes.  Any children of the original
	 * node become children of the last in the tree of replacement nodes.
	 * @param originalNode Node to replace
	 * @param replacementNodes The replacement nodes
	 */
	public static final void replaceNode(PlanNode originalNode, List replacementNodes) {
		Assertion.isPositive(replacementNodes.size());

		// Get everything around the original node
		PlanNode parent = originalNode.getParent();
		List children = new LinkedList(originalNode.getChildren());

		// Disconnect originalNode
		while(originalNode.getChildren().size() > 0) {
			NodeEditor.cutLast(originalNode);
		}
		NodeEditor.removeChildNode(parent, originalNode);

		// Reconstruct plan
		PlanNode top = parent;
		Iterator replacementIter = replacementNodes.iterator();
		while(replacementIter.hasNext()) {
			PlanNode bottom = (PlanNode) replacementIter.next();
			NodeEditor.attachLast(top, bottom);

			// Walk down the tree
			top = bottom;
		}

		// Attach children to last top
		Iterator childIter = children.iterator();
		while(childIter.hasNext()) {
			NodeEditor.attachLast(top, (PlanNode) childIter.next());
		}
	}

    /**
     * Find the sibling of this node.  This assumes that this node has a
     * parent and that the parent has no more than two children.  If the
     * parent has only one child, null is returned.
     * @param Node original node
     * @return Sibling node or null if node has no sibling
     */
    public static final PlanNode getSibling(PlanNode node) {
        // Get parent and check stuff
        PlanNode parentNode = node.getParent();
        Assertion.isNotNull(parentNode);
        if(parentNode.getChildCount() >= 3){
            Assertion.assertTrue(parentNode.getChildCount() < 3, QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0057));
        }

        // Singleton child case
        if(parentNode.getChildCount() == 1) {
            return null;
        }

        // Find sibling
        PlanNode siblingNode = parentNode.getLastChild();
        if(siblingNode == node) {
            siblingNode = parentNode.getFirstChild();
        }
        return siblingNode;
    }
}
