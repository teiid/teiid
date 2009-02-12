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

import com.metamatrix.common.tree.TreeView;

/**
 * This interface defines a view of a hierarchy of DirectoryEntry instances.
 * A directory has a constraint above and beyond tree that requires no two
 * children of a DirectoryEntry may have the same name.
 */
public interface DirectoryEntryView extends TreeView {

    /**
     * Return the directory entry editor for this view.
     * @return the DirectoryEntryEditor instance
     */
    DirectoryEntryEditor getDirectoryEntryEditor();

    /**
     * Lookup the node referenced by the relative path in this view.
     * Depending upon the implementation, this method may return a null
     * reference if a node with the specified path is not found.
     * @param path the path of the desired node specified in terms of this view
     * (i.e., the result of calling <code>getPath()</code> on this view with the
     * returned node as the parameter should result in the same value as <code>path</code>);
     * may not be null or zero-length
     * @return the node referenced by the specified path, or null if no such
     * node exists
     * @throws AssertionError if the path is null or zero-length
     */
    DirectoryEntry lookup( String path );

    /**
     * Lookup the node referenced by the relative path in this view, but
     * specify a separator.  This method allows the lookup of a path
     * with a different separator than used by this view.
     * Depending upon the implementation, this method may return a null
     * reference if a node with the specified path is not found.
     * @param path the path of the desired node specified in terms of this view
     * (i.e., the result of calling <code>getPath()</code> on this view with the
     * returned node as the parameter should result in the same value as <code>path</code>);
     * may not be null or zero-length
     * @param separater the string used to separate the components of a name.
     * @return the node referenced by the specified path, or null if no such
     * node exists
     * @throws AssertionError if the path is null or zero-length
     */
    DirectoryEntry lookup( String path, String separator );
}

