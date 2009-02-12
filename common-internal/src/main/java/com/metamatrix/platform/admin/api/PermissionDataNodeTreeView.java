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

import java.util.Collection;

/**
 * Provides a <i>view</i> into the tree of <code>PermissionDataNode</code>s and supports utility
 * methods that perform funcions on the tree as a whole.
 * <p>
 * A property can be set (or toggled) to determine if clients want methods to return nodes marked
 * hidden. See {@link #setShowHidden}.
 * </p>
 */
public interface PermissionDataNodeTreeView extends PermissionTreeView {

    /**
     * Hide all nodes whose resource name starts with "System".
     */
    void hideAllSystemNodes();

    /**
     * Determine whether all descendants of the given node share the <i>exact same</i> actions as the
     * given node. This determination is independant of the state of <code>showHidden</code>.
     * @param startingPoint The root of the subtree to check.
     * @returns <code>true</code> if <i>all</i> of the given node's descendants share the
     * <i>exact same</i> actions, <code>false</code> otherwise.
     */
    boolean allDescendantsShareActions( PermissionNode startingPoint );

    /**
     * Set the permissions on the node containing each permission's resource.
     * @permissions The <code>Collection</code> of <code>BasicAuthorizationPermission</code>s to set
     * (each contains the the resource and an AuthorizationAction.)
     * @return The <code>Collection</code> of <code>DataNodeExceptions</code>s each containing
     * a resource name that was not found in the tree.
     */
    Collection setPermissions(Collection permissions);

}
