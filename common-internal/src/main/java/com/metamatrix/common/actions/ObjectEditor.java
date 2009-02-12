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

package com.metamatrix.common.actions;

public interface ObjectEditor {

    /**
     * The command to signify setting of an attribute.
     */
    static final int SET = 0;

    /**
     * The command to signify addition of an attribute.
     */
    static final int ADD = 1;

    /**
     * The command to signify removal of an attribute.
     */
    static final int REMOVE = 2;

    /**
     * Get the action destination for this object.  The modification actions,
     * if used, are created by the <code>modifyObject</code> method.
     * @return the action queue into which the modification actions are placed.
     */
    ModificationActionQueue getDestination();

    /**
     * Set the destination for this object.
     * @param destination the new destination queue for any modification
     * actions created by this editor.
     */
    void setDestination(ModificationActionQueue destination);
}



