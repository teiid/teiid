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

import com.metamatrix.platform.security.api.AuthorizationActions;

/**
 * Defines an interface to tree node for displaying entitlements.
 */
public interface PermissionDataNode extends PermissionNode {

    /**
     * Does this node belong to a physical model?
     * @return <code>true</code> if this node is part of a physical model.
     */
    boolean isPhysical();

    /**
     * Get the <i>type</i> of this <code>PermissionDataNode</code>.
     * <br>This method returns the <i>int</i> type of this node which
     * coresponds to {@link PermissionDataNodeDefinition.TYPE PermissionDataNodeDefinition.TYPE}.</br>
     * @return The type of this <code>PermisionDataNode</code>.
     */
    int getDataNodeType();

    /**
     * Are there <i>any</i> entitled nodes below this point in the tree?
     * @return True, if a decendant is entitled with <i>any</i> AuthorizationActions, False otherwise.
     */
    boolean isDescendantEnabled();

    /**
     * Does this node have any descendants enabled for the given <code>AuthorizationActions</code>?
     * @param actions The actions of interest. 
     * @return True, if a decendant of this node is entitled with the given actions, False otherwise.
     */
    boolean isDescendantEnabledFor(AuthorizationActions actions);

    boolean isGroupNode();
    
    /**
     * Return stringafied representation of the object suitable for debugging.
     */
    String printDebug();
    
}
