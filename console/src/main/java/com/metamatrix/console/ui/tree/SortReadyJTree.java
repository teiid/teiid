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

package com.metamatrix.console.ui.tree;

import java.util.Collection;

import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreeModel;

import com.metamatrix.console.util.StaticTreeUtilities;

import com.metamatrix.toolbox.ui.widget.TreeWidget;

/**
 * Extension to JTree to always present nodes in a sorted order based on nodes that implement
 * SortsChildren.  Also implements SavesExpansionState.
 */
public class SortReadyJTree extends TreeWidget implements SavesExpansionState {

//Constructors

    public SortReadyJTree() {
        super();
        customizeAppearance();
    }

    private void customizeAppearance()
    {
        putClientProperty( "JTree.lineStyle", "Angled" ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public SortReadyJTree(TreeModel model) {
        super(model);
        //Start out with the model sorted
        customizeAppearance();

        sortModel();
    }

//Overridden Methods

    public void setModel(TreeModel model) {

        super.setModel(model);

        //Sort the new model
        sortModel();
    }

//Processing methods

    /**
     * Sort the entire tree model.
     */
    private void sortModel() {
        if (getModel() != null) {
            Object rootObj = getModel().getRoot();
            if (rootObj instanceof MutableTreeNode) {
                MutableTreeNode root = (MutableTreeNode)rootObj;
                ChildSortingTreeNode.sortTree(root);
            }
        }
    }

    /**
     * Save the tree's current expansion state.
     */
    public TreePathExpansion[] saveExpansionState() throws Exception {
        return StaticTreeUtilities.expansionState(this);
    }

    /**
     * Restore to saved expansion state.
     */
    public void restoreExpansionState(TreePathExpansion[] saved) throws Exception {
        StaticTreeUtilities.restoreExpansionState(this, saved, true);
    }

    /**
     * Form of restoreExpansionState required by SavesExpansionState.
     */
    public void restoreExpansionState(TreePathExpansion[] saved,
            Collection /*<TreeSelectionListener>*/ selectionListeners,
            Collection /*<TreeWillExpandListener>*/ willExpandListeners,
            Collection /*<TreeExpansionListener>*/ expansionListeners)
            throws Exception {
        StaticTreeUtilities.restoreExpansionState(this, saved, true,
                selectionListeners, willExpandListeners, expansionListeners);
    }
}
