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

package com.metamatrix.platform.admin.api;

import com.metamatrix.core.id.ObjectID;
import com.metamatrix.common.tree.TreeNode;

import com.metamatrix.platform.admin.api.exception.PermissionNodeNotActionableException;
import com.metamatrix.platform.security.api.AuthorizationActions;

public interface PermissionNode extends TreeNode {
    /**
     * Display name for node.
     * @return The name of the data node for display.
     */
    String getDisplayName();

    /**
     * Resource name for node (the DataNodeFullName).
     * @return The resource name of the data node.
     */
    String getResourceName();

    /**
     * The node's unique ID.
     * @return The unique ID for the node.
     */
    ObjectID getUUID();

    /**
     * The <code>AuthorizationActions</code> labels currently on this data node.
     * @see com.metamatrix.platform.security.api.StandardAuthorizationActions
     * @return The Array <String> of one or more of {"None", "Create",
     * "Read", "Update", "Delete"}.
     */
    String[] getActionLabels();

    /**
     * The <code>AuthorizationActions</code> currently on this data node.
     * @see com.metamatrix.platform.security.api.StandardAuthorizationActions
     * @return The actions allowed on this data node.
     */
    AuthorizationActions getActions();

    /**
     * The <code>AuthorizationActions</code> allowed on this data node.
     * @see com.metamatrix.platform.security.api.StandardAuthorizationActions
     * @return The actions allowed on this data node.
     */
    AuthorizationActions getAllowedActions();

    /**
     * Set the <code>AuthorizationActions</code> on this data node.
     * @param actions The the actions to set on this data node.
     * @throws PermissionNodeNotActionableException If attempt is made to set actions on a node that can't
     * accept <i>any</code> actions.
     */
    void setActions(AuthorizationActions actions) throws PermissionNodeNotActionableException;

    /**
     * Set the allowed <code>AuthorizationActions</code> on this data node.
     * <br></br>
     * @param actions The the actions to set on this data node.
     * @throws PermissionNodeNotActionableException If attempt is made to set actions on a node that can't
     * accept <i>any</code> actions.
     */
    void setActions(int actions) throws PermissionNodeNotActionableException;

    /**
     * Determine if this node's <code>AuthorizationActions</code> are equal to the given actions. The actions
     * are considered equal if number of actions are the same and all corresponding pairs of actions
     * are equal.
     * @param actions The Array <String> of one or more of {"None", "Create",
     * "Read", "Update", "Delete"}.
     * @return true if the <code>Actions</code> of this node are equal to the given actions.
     */
    boolean actionsAreEqual(String[] actions);

    /**
     * Determine if this node's <code>AuthorizationActions</code> are equal to the given actions. The actions
     * are considered equal if number of actions are the same and all corresponding pairs of actions
     * are equal.
     * @param actions The <code>AuthorizationActions</code> to compare with this node's actions.
     * @return true if the <code>Actions</code> of this node are equal to the given actions.
     */
    boolean actionsAreEqual(AuthorizationActions actions);

    /**
     * Determine if this node's <code>AuthorizationActions</code> are equal to the given node's
     * actions. The actions are considered equal if number of actions are the same and all
     * corresponding pairs of actions are equal.
     * @param node The node whose actions to compare with this node's actions.
     * @return true if the <code>Actions</code> of this node are equal to the given node's actions.
     */
    boolean actionsAreEqual(PermissionNode node);

    /**
     * Is this node a leaf?
     * @return <code>true</code>, if this node has no children.
     */
    boolean isLeafNode();

    /**
     * Check whether or not this node is hidden from the <code>PermissionDataNodeTreeView</code>. The
     * default is <code>false</code>, the node is not hidden.
     * @see PermissionTreeView#setShowHidden
     * @return <code>true</code> if this node may be hidden from the view.
     */
    boolean isHidden();

    /**
     * Set whether or not this node is hidden from the <code>PermissionDataNodeTreeView</code>. The
     * default is <code>false</code>, the node is not hidden.
     * @see PermissionTreeView#setShowHidden
     * @param isHidden If <code>true</code>, this node may be hidden from the view.
     */
    void setHidden(boolean isHidden);

    /**
     * Does this node have <i>any</i> permission associated with it?<br></br>
     * @return True, if this node has an <code>Action</code> other than <code>NONE</code>.
     */
    boolean hasPermission();

}
