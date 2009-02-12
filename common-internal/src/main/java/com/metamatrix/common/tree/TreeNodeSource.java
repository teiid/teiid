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

import java.util.List;

/**
 * This interface defines an interface to a source of TreeNode information.
 */
public interface TreeNodeSource {

    /**
     * Obtain the set of entries that are considered the children of the specified
     * TreeNode.
     * @param parent the TreeNode instance for which the child entries
     * are to be obtained; may not be null
     * @return the list of TreeNode instances that are considered the children
     * of the specified entry; never null but possibly empty
     */
    List getChildren(TreeNode parent);

    /**
     * Determine the parent TreeNode for the specified entry, or null if
     * the specified entry is a root.
     * @param entry the TreeNode instance for which the parent is to be obtained;
     * may not be null
     * @return the parent entry, or null if there is no parent
     */
    TreeNode getParent(TreeNode node);

    /**
     * Create a new instance of TreeNodeEditor, which is an editor that may
     * be used to access and/or modify a single TreeNode object.
     * @return the new editor instance; never null
     */
    TreeNodeEditor createTreeNodeEditor();

    /**
     * Determine whether the specified TreeNode may contain children.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry can contain children, or false otherwise.
     */
    boolean allowsChildren(TreeNode node);

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
     * Determine whether the specified TreeNode is considered read-only
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry is read-only, or false otherwise.
     */
    boolean isReadOnly(TreeNode node);
}


