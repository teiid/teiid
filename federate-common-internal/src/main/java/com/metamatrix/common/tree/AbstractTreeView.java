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

package com.metamatrix.common.tree;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;

abstract public class AbstractTreeView implements TreeView {

    private static TreeNodeFilter DEFAULT_FILTER = new PassThroughTreeNodeFilter();
    private static Comparator DEFAULT_COMPARATOR = new TreeNodePathComparator();
    private TreeNodeFilter filter = DEFAULT_FILTER;
    private Comparator comparator = DEFAULT_COMPARATOR;

    public AbstractTreeView() {
    }

    /**
     * Set the filter that limits the set of TreeNode instances returned from this view.
     * @param filter the filter, or null if the default "pass-through" filter should be used.
     */
    public void setFilter(TreeNodeFilter filter) {
        if ( filter == null ) {
            this.filter = DEFAULT_FILTER;
        } else {
            this.filter = filter;
        }
    }

    /**
     * Return the filter that is being used by this view.
     * @return the current filter; never null
     */
    public TreeNodeFilter getFilter() {
        return this.filter;
    }

    /**
     * Set the comparator that should be used to order the children.
     * @param comparator the comparator, or null if node path sorting should be used.
     */
    public void setComparator(Comparator comparator) {
        if ( comparator == null ) {
            this.comparator = DEFAULT_COMPARATOR;
        } else {
            this.comparator = comparator;
        }
    }

    /**
     * Return the comparator used to order for children returned from this view.
     * @return the current comparator; never null
     */
    public Comparator getComparator() {
        return this.comparator;
    }

    /**
     * Get the definitions of the properties for the TreeNode instances
     * returned from this view.
     * @return the unmodifiable list of PropertyDefinition instances; never null
     */
    public abstract List getPropertyDefinitions();

    /**
     * Returns the single root of this TreeNode system.
     * @return the unmodifiable list of TreeNode instances
     * that represent the root of this view.
     */
    public abstract List getRoots();

    /**
     * Determine whether the specified TreeNode is a root of the underlying system.
     * @param node the TreeNode instance that is to be checked; may not be null
     * @return true if the node is a root, or false otherwise.
     */
    public boolean isRoot(TreeNode node) {
        if ( node == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0001));
        }
        return this.getRoots().contains(node);
    }

    /**
     * Determine whether the specified TreeNode is hidden.
     * @param node the TreeNode instance that is to be checked; may not be null
     * @return true if the node is hidden, or false otherwise.
     */
    public abstract boolean isHidden(TreeNode node);

    /**
     * Return the marked state of the specified node.
     * @return the marked state of the node.
     */
    public abstract boolean isMarked(TreeNode node);

    /**
     * Obtain the DirectoryEntry that represents the home for the underlying
     * system.  If the underlying system does not support a home entry concept,
     * null is returned.
     * @return the entry that represents the home, or null if no home concept
     * is supported.
     */
    public abstract TreeNode getHome();

    /**
     * Obtain the abstract path for this TreeNode.
     * @return the string that represents the abstract path of this node; never null
     */
    public abstract String getPath(TreeNode node);

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the abstract path.
     */
    public abstract char getSeparatorChar();

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    public abstract String getSeparator();

    /**
     * Determine the parent TreeNode for the specified node, or null if
     * the specified node is a root.
     * @param node the TreeNode instance for which the parent is to be obtained;
     * may not be null
     * @return the parent node, or null if there is no parent
     */
    public abstract TreeNode getParent(TreeNode node);

    /**
     * Obtain the set of entries that are considered the children of the specified
     * TreeNode.
     * @param parent the TreeNode instance for which the child entries
     * are to be obtained; may not be null
     * @return the unmodifiable list of TreeNode instances that are considered the children
     * of the specified node; never null but possibly empty
     */
    public abstract List getChildren(TreeNode parent);

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    public abstract boolean isParentOf(TreeNode parent, TreeNode child);

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    public abstract boolean isAncestorOf(TreeNode ancestor, TreeNode descendent);

    /**
     * Return the propertied object editor for this view.
     * @return the PropertiedObjectEditor instance
     */
    public abstract PropertiedObjectEditor getPropertiedObjectEditor();

    /**
     * Return the tree node editor for this view.
     * @return the TreeNodeEditor instance
     */
    public abstract TreeNodeEditor getTreeNodeEditor();

    /**
     * Create a new instance of a UserTransaction that may be used to
     * read information.  Read transactions do not have a source object
     * associated with them (since they never directly modify data).
     * @return the new transaction object
     */
    public abstract UserTransaction createReadTransaction();

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information.  The transaction will <i>not</i> have a source object
     * associated with it.
     * @return the new transaction object
     */
    public abstract UserTransaction createWriteTransaction();

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information. The source object will be used for all events that are
     * fired as a result of or as a product of this transaction.
     * @param source the object that is considered to be the source of the transaction;
     * may be null
     * @return the new transaction object
     */
    public abstract UserTransaction createWriteTransaction(Object source);

    /**
     * Obtain an iterator for this whole view, which navigates the view's
     * nodes using pre-order rules (i.e., it visits a node before its children).
     * @return the view iterator
     */
    public Iterator iterator() {
        return new TreeNodeIterator(this.getRoots(),this);
    }

    /**
     * Obtain an iterator for the view starting at the specified node.  This
     * implementation currently navigates the subtree using pre-order rules
     * (i.e., it visits a node before its children).
     * @param startingPoint the root of the subtree over which the iterator
     * is to navigate; may not be null
     * @return the iterator that traverses the nodes in the subtree starting
     * at the specified node; never null
     */
    public Iterator iterator(TreeNode startingPoint) {
    	ArgCheck.isNotNull(startingPoint);
        return new TreeNodeIterator(startingPoint,this);
    }

}

