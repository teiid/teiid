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

package com.metamatrix.console.ui.util;

import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreePath;

import com.metamatrix.console.util.RuntimeExternalException;

/**
 * LazyBranchListener is used with <code>LazyBranchNode</code> to call populate()
 * on their nodes.
 *
 * To use, instantiate this class and call addTreeWillExpandListener() on the tree.
 *
 * Example:
 * <pre>
 *     public JTree buildTree() {
 *         MyLazyBranchNode root = new MyLazyBranchNode(getRootObject());
 *         JTree myTree = new JTree(root);
 *         myTree.addTreeWillExpandListener(new LazyBranchListener());
 *         return myTree;
 *     }
 * </pre>
 *
 * @see LazyBranchNode
 */
public class LazyBranchListener implements TreeWillExpandListener {

    public void treeWillCollapse(TreeExpansionEvent e) { }

    public void treeWillExpand(TreeExpansionEvent e) {
        TreePath path = e.getPath();
        MutableTreeNode node = (DefaultMutableTreeNode) path.getLastPathComponent();
        if ( node instanceof LazyBranchNode ) {
            try {
                ((LazyBranchNode)node).populate();
            } catch (Exception ex) {
                throw new RuntimeExternalException(ex);
            }
        }
    }

}


