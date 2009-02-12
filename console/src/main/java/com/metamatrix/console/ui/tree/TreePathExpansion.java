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

package com.metamatrix.console.ui.tree;

import javax.swing.tree.TreePath;

import com.metamatrix.console.util.StaticTreeUtilities;

/**
 * Data class which allows saving of whether or not a particular TreePath is expanded.
 */
public class TreePathExpansion {
    private TreePath treePath;
    private boolean expanded;

    public TreePathExpansion(TreePath tp, boolean exp) {
        super();
        treePath = tp;
        expanded = exp;
    }

    public TreePath getTreePath() {
        return treePath;
    }

    public boolean isExpanded() {
        return expanded;
    }

    public String toString() {
        String str = "TreePathExpansion: treePath=" +  //$NON-NLS-1$
                StaticTreeUtilities.treePathToString(getTreePath()) + ",expanded=" +  //$NON-NLS-1$
                isExpanded();
        return str;
    }
}
