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

package com.metamatrix.console.ui.util;

import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.TreePath;

/**
 * Implementation of TreeSelectionListener that will block the selection of any
 * in a given set of items, where the item is the last node in a selected tree
 * path.  Match is determined by toString().equals() of
 * selected item when compared to input string.
 */
public class ItemsBlockedTreeSelectionListener implements TreeSelectionListener {
    private JTree theTree;
    private ItemsBlockedCallback callback;

    public ItemsBlockedTreeSelectionListener(JTree tree, ItemsBlockedCallback cb) {
        super();
		theTree = tree;
        callback = cb;
    }

    public void valueChanged(TreeSelectionEvent ev) {
        Object[] blockedItems = callback.getBlockedItems();
        int totalBlocked = 0;
        TreePath[] tp = theTree.getSelectionPaths();
        if (tp != null) {
            for (int i = 0; i < tp.length; i++) {
                String lastNode = tp[i].getLastPathComponent().toString();
                int j = 0;
                boolean matchFound = false;
                while ((j < blockedItems.length) && (!matchFound)) {
                    if (lastNode.equals(blockedItems[j].toString())) {
                        theTree.removeSelectionPath(tp[i]);
                        matchFound = true;
                        totalBlocked++;
                    } else {
                        j++;
                    }
                }
            }
            if (tp.length == 1) {
            	if (totalBlocked == 1) {
                	callback.itemSelectionBlocked();
                } else {
                	callback.itemSelectionChanged(tp[0]);
                }
            }
        }
    }
}


