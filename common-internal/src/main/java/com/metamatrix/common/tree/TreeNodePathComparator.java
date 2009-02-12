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

package com.metamatrix.common.tree;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

public class TreeNodePathComparator implements TreeNodeComparator {

//    private TreeNodeEditor editor = null;

    public void setTreeNodeEditor( TreeNodeEditor editor ) {
        if ( editor == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0014));
        }
//        this.editor = editor;
    }

    public int compare(TreeNode entry1, TreeNode entry2){
        if ( entry1 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0015));
        }
        if ( entry2 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0016));
        }
        String value1 = entry1.getFullName();
        String value2 = entry2.getFullName();
        if ( value1 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0017));
        }
        if ( value2 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0018));
        }
        return value1.compareTo(value1);
    }
    public boolean equals(TreeNodePathComparator comparator){
        if ( comparator == null ) {
            return false;
        }
        return ( comparator.getClass() == this.getClass() );
    }
    public int compare(Object entry1, Object entry2){
        throw new ClassCastException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0019));
    }
    public boolean equals(Object entry){
        throw new ClassCastException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0020));
    }
}

