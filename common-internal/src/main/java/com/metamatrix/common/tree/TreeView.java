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

package com.metamatrix.common.tree;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.object.PropertiedObjectView;

/**
 * This interface defines a view of a hierarchy of TreeNode instances.
 */
public interface TreeView extends PropertiedObjectView {

    /**
     * Set the filter that limits the set of TreeNode instances
     * returned from this view.
     * @param filter the filter, or null if the default "pass-through" filter should be used.
     */
    void setFilter(TreeNodeFilter filter);

    /**
     * Set the filter that limits the set of TreeNode instances
     * returned from this view.
     * @return the current filter; never null
     */
    TreeNodeFilter getFilter();

    /**
     * Set the comparator that should be used to order the children.
     * @param comparator the comparator, or null if entry name sorting should be used.
     */
    void setComparator(Comparator comparator);

    /**
     * Set the comparator that provides the order for children
     * returned from this view.
     * @return the current comparator; never null
     */
    Comparator getComparator();

    /**
     * Get the definitions of the properties for the TreeNode instances
     * returned from this view.
     * @return the unmodifiable list of PropertyDefinition instances; never null
     */
    List getPropertyDefinitions();

    /**
     * Returns all root partitians on this TreeNode system.
     * @return the unmodifiable list of TreeNode instances
     * that represent the roots
     */
    List getRoots();

    /**
     * Determine whether the specified TreeNode is a root of the underlying
     * system.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry is a root, or false otherwise.
     */
    boolean isRoot(TreeNode entry);

    /**
     * Determine whether the specified TreeNode may contain children.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry can contain children, or false otherwise.
     */
    boolean allowsChildren(TreeNode entry);

    /**
     * Determine whether the specified parent TreeNode may contain the
     * specified child node.
     * @param parent the TreeNode instance that is to be the parent;
     * may not be null
     * @param potentialChild the TreeNode instance that is to be the child;
     * may not be null
     * @return true if potentialChild can be placed as a child of parent,
     * or false otherwise.
     */
    boolean allowsChild(TreeNode parent, TreeNode potentialChild);

    /**
     * Determine whether the specified TreeNode is hidden.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry is hidden, or false otherwise.
     */
    boolean isHidden(TreeNode entry);

    /**
     * Return the marked state of the specified entry.
     * @return the marked state of the entry.
     */
    boolean isMarked(TreeNode entry);

    /**
     * Set the marked state of the specified entry.
     * @param true if the node is to be marked, or false if it is to be un-marked.
     */
    void setMarked(TreeNode entry, boolean markedState);

    /**
     * Return the set of marked nodes for this view.
     * @param the unmodifiable set of marked nodes; never null
     */
    Set getMarked();

    /**
     * Obtain the TreeNode that represents the home for the underlying
     * system.  If the underlying system does not support a home entry concept,
     * null is returned.
     * @return the entry that represents the home, or null if no home concept
     * is supported.
     */
    TreeNode getHome();

    /**
     * Obtain the abstract path for this TreeNode.
     * @return the string that represents the abstract path of this entry; never null
     */
    String getPath(TreeNode entry);

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the abstract path.
     */
    char getSeparatorChar();

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    String getSeparator();

    /**
     * Determine the parent TreeNode for the specified entry, or null if
     * the specified entry is a root.
     * @param entry the TreeNode instance for which the parent is to be obtained;
     * may not be null
     * @return the parent entry, or null if there is no parent
     */
    TreeNode getParent(TreeNode entry);

    /**
     * Obtain the set of entries that are considered the children of the specified
     * TreeNode.
     * @param parent the TreeNode instance for which the child entries
     * are to be obtained; may not be null
     * @return the unmodifiable list of TreeNode instances that are considered the children
     * of the specified entry; never null but possibly empty
     */
    List getChildren(TreeNode parent);

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    boolean isParentOf(TreeNode parent, TreeNode child);

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    boolean isAncestorOf(TreeNode ancestor, TreeNode descendent);

    /**
     * Return the tree node editor for this view.
     * @return the TreeNodeEditor instance
     */
    TreeNodeEditor getTreeNodeEditor();

    /**
     * Obtain an iterator for this whole view, which navigates the view's
     * nodes using pre-order rules (i.e., it visits a node before its children).
     * @return the iterator that traverses the nodes in this view; never null
     */
    Iterator iterator();

    /**
     * Obtain an iterator for the view starting at the specified node.  This
     * implementation currently navigates the subtree using pre-order rules 
     * (i.e., it visits a node before its children).
     * @param startingPoint the root of the subtree over which the iterator
     * is to navigate; may not be null
     * @return the iterator that traverses the nodes in the subtree starting
     * at the specified node; never null
     */
    Iterator iterator(TreeNode startingPoint);
}


