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

package com.metamatrix.common.tree.directory;

import com.metamatrix.common.tree.ChildRules;
import com.metamatrix.common.tree.RuledTreeViewImpl;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.core.util.Assertion;

public class RuledDirectoryEntryViewImpl extends RuledTreeViewImpl implements DirectoryEntryView {

    public RuledDirectoryEntryViewImpl(TreeView treeView, ChildRules rules ) {
        super(treeView,rules);
        Assertion.assertTrue(treeView instanceof DirectoryEntryView,"The TreeView reference is expected to implement DirectoryEntryView"); //$NON-NLS-1$
    }
    
    public RuledDirectoryEntryViewImpl(TreeView treeView, TreeNode root, ChildRules rules ) {
        super(treeView,root,rules);
        Assertion.assertTrue(treeView instanceof DirectoryEntryView,"The TreeView reference is expected to implement DirectoryEntryView"); //$NON-NLS-1$
        Assertion.assertTrue(root instanceof DirectoryEntry,"The TreeNode reference is expected to implement DirectoryEntry"); //$NON-NLS-1$
    }

    protected DirectoryEntryView getDirectoryEntryView() {
        return (DirectoryEntryView)super.getTreeView();
    }

	// ########################## PropertiedObjectView Methods ###################################

    // ########################## TreeView Methods ###################################

    // ########################## DirectoryEntryView Methods ###################################

    /**
     * Return the directory entry editor for this view.
     * @return the DirectoryEntryEditor instance
     */
    public DirectoryEntryEditor getDirectoryEntryEditor() {
        return this.getDirectoryEntryView().getDirectoryEntryEditor();
    }

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
    public DirectoryEntry lookup( String path ) {
        DirectoryEntry result = this.getDirectoryEntryView().lookup(path);
        if ( result != null && this.isHidden(result) ) {
            result = null;    
        }
        return result;
    }

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
    public DirectoryEntry lookup( String path, String separator ) {
        DirectoryEntry result = this.getDirectoryEntryView().lookup(path,separator);
        if ( result != null && this.isHidden(result) ) {
            result = null;    
        }
        return result;
    }
}
