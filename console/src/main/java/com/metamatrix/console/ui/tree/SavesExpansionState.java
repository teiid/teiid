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

/**
 * Interface to be implemented by a JTree that is capable of returning to a saved expansion
 * state.
 */
public interface SavesExpansionState {
    TreePathExpansion[] saveExpansionState() throws Exception;
        //returns an array representing the current expansion state of the tree
    void restoreExpansionState(TreePathExpansion[] tpe,
            Collection /*<TreeSelectionListener>*/ selectionListeners,
            Collection /*<TreeWillExpandListener>*/ willExpandListeners,
            Collection /*<TreeExpansionListener>*/ expansionListeners)
            throws Exception;
        //Restores to previous saved expansion state.  Note that in restoring, there may be tree
        //paths in the saved state that no longer exist.  These should be ignored.  There may
        //also be new tree paths.  These should probably be expanded.  The collections of
        //listeners are removed before doing the expansion and reinstated after doing the
        //expansion.

}
