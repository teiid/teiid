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

package com.metamatrix.common.tree.basic;

import java.util.List;

import com.metamatrix.common.object.DefaultPropertyAccessPolicy;
import com.metamatrix.common.object.PropertyAccessPolicy;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.TreeNodeSource;
import com.metamatrix.core.id.ObjectIDFactory;
import com.metamatrix.core.util.Assertion;

/**
 * This interface defines an interface to a source of TreeNode information.
 */
public class BasicTreeNodeSource implements TreeNodeSource {
    private ObjectIDFactory idFactory;
    private PropertyAccessPolicy policy;

    public BasicTreeNodeSource(ObjectIDFactory idFactory) {
        this(idFactory, new DefaultPropertyAccessPolicy());
    }

    public BasicTreeNodeSource(ObjectIDFactory idFactory, PropertyAccessPolicy policy) {
        Assertion.isNotNull(idFactory,"The ObjectIDFactory reference may not be null"); //$NON-NLS-1$
        Assertion.isNotNull(policy,"The PropertyAccessPolicy reference may not be null"); //$NON-NLS-1$
        this.idFactory  = idFactory;
        this.policy = policy;
    }

    protected BasicTreeNode assertBasicTreeNode( TreeNode node ) {
        Assertion.isNotNull(node, "The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(node instanceof BasicTreeNode, "The referenced object is not an BasicTreeNode"); //$NON-NLS-1$
        BasicTreeNode basicNode = (BasicTreeNode) node;
        return basicNode;
    }

	// ########################## TreeNodeSource Methods ###################################

    /**
     * Obtain the set of entries that are considered the children of the specified
     * TreeNode.
     * @param parent the TreeNode instance for which the child entries
     * are to be obtained; may not be null
     * @return the list of TreeNode instances that are considered the children
     * of the specified entry; never null but possibly empty
     */
    public List getChildren(TreeNode parent) {
        BasicTreeNode parentNode = this.assertBasicTreeNode(parent);
        return parentNode.getChildren();
    }

    /**
     * Determine the parent TreeNode for the specified entry, or null if
     * the specified entry is a root.
     * @param entry the TreeNode instance for which the parent is to be obtained;
     * may not be null
     * @return the parent entry, or null if there is no parent
     */
    public TreeNode getParent(TreeNode node) {
        BasicTreeNode basicNode = this.assertBasicTreeNode(node);
        return basicNode.getParent();
    }

    /**
     * Create a new instance of TreeNodeEditor, which is an editor that may
     * be used to access and/or modify a single TreeNode object.
     * @return the new editor instance; never null
     */
    public TreeNodeEditor createTreeNodeEditor() {
        return new BasicTreeNodeEditor(this.idFactory, this.policy);
    }

    /**
     * Determine whether the specified TreeNode may contain children.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry can contain children, or false otherwise.
     */
    public boolean allowsChildren(TreeNode node) {
        this.assertBasicTreeNode(node);
        return true;
    }

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
    public boolean allowsChild(TreeNode parent, TreeNode potentialChild) {
        this.assertBasicTreeNode(parent);
        this.assertBasicTreeNode(potentialChild);
        return true;
    }

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    public boolean isParentOf(TreeNode parent, TreeNode child) {
        BasicTreeNode parentNode = this.assertBasicTreeNode(parent);
        BasicTreeNode childNode  = this.assertBasicTreeNode(child);
        return parentNode.isParentOf(childNode);
    }

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    public boolean isAncestorOf(TreeNode ancestor, TreeNode descendent) {
        BasicTreeNode ancestorNode   = this.assertBasicTreeNode(ancestor);
        this.assertBasicTreeNode(descendent);
        return ancestorNode.isAncestorOf(descendent);
    }

    /**
     * Determine whether the specified TreeNode is considered read-only
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry is read-only, or false otherwise.
     */
    public boolean isReadOnly(TreeNode node) {
        this.assertBasicTreeNode(node);
        return false;
    }

	// ########################## Implementation Methods ###################################

    /**
     * Return the ObjectIDFactory instance for this TreeNodeSource
     */
    public ObjectIDFactory getObjectIDFactory() {
        return this.idFactory;
    }

    /**
     * Return the PropertyAccessPolicy instance for this TreeNodeSource
     */
    public PropertyAccessPolicy getPropertyAccessPolicy() {
        return this.policy;
    }

}


