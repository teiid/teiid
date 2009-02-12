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

package com.metamatrix.console.ui.views.entitlements;

/**
 * Single-method interface intended to be implemented by a class responsible for
 * correlating the state of the authorizations tree of DataNodesTreeNode
 * nodes and the checkboxes being displayed which show their state.  The
 * implementer is expected to change the state of the checkbox to match
 * the new authorization propagated to a node.
 */
public interface AuthorizationPropagationListener {
    /**
     * Method invoked when a node has had an authorization value changed through
     * propagation, as opposed to being changed through mouse action.

     * @param   node        the node which has had an authorization changed
     * @param   authorizationType   one of AuthorizationsModel.CREATE_COLUMN_NUM, etc.
     */
    void authorizationPropagated(DataNodesTreeNode node,
            int authorizationType);
}
