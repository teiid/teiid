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

package com.metamatrix.common.tree.directory;

import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;

/**
 * This interface defines a edit DirectoryEntry instances.
 */
public interface DirectoryEntryEditor extends TreeNodeEditor {

    /**
     * Check for the existance of the specified entry, and creates an underlying
     * resource for the entry if one does not exist.
     * @param obj the node to be deleted; may not be null
     * @return true if the entry was successfully created (made to exist); false
     * if the entry already exists  or if the entry could not be created
     * @throws AssertionError if <code>obj</code> is null
     */
    boolean makeExist(DirectoryEntry entry);

    /**
     * Filter the specified PropertyDefinition instances and return the first
     * definition that is mapped to "the description" property for the metadata object.
     * @param obj the tree node; may not be null
     * @return the first PropertyDefinition instance found in the list of
     * PropertyDefinition instances that represents the description property for the object,
     * or null if no such PropertyDefinition is found.
     */
    PropertyDefinition getDescriptionPropertyDefinition(TreeNode obj);

    /**
     * Determine whether the specified name is valid for a file or folder on the
     * current file system.
     * @param newName the new name to be checked
     * @return true if the name is null or contains no invalid characters for a
     * folder or file, or false otherwise
     */
    boolean isNameValid( String newName );

}
