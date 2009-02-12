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

public class TreeNodeNameComparator implements TreeNodeComparator {

    private static final boolean DEFAULT_IGNORE_CASE = false;

//    private TreeNodeEditor editor = null;
    private boolean ignoreCase;

    public TreeNodeNameComparator( boolean ignoreCase ) {
        this.ignoreCase = ignoreCase;
    }

    public TreeNodeNameComparator() {
        this(DEFAULT_IGNORE_CASE);
    }

    public void setTreeNodeEditor( TreeNodeEditor editor ) {
        if ( editor == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0014));
        }
//        this.editor = editor;
    }

    public int compare(Object obj1, Object obj2){
        TreeNode entity1 = (TreeNode) obj1;     // May throw ClassCastException
        TreeNode entity2 = (TreeNode) obj2;     // May throw ClassCastException
        if ( entity1 == null && entity2 == null ) {
            return 0;
        }
        if ( entity1 != null && entity2 == null ) {
            return 1;
        }
        if ( entity1 == null && entity2 != null ) {
            return -1;
        }
        int result = 0;
        if ( this.ignoreCase ) {
            result = entity1.getName().compareToIgnoreCase(entity2.getName());
        } else {
            result = entity1.getName().compareTo(entity2.getName());
        }
//System.out.println("-- Comparing " + entity1.getFullName() + " to " + entity2.getFullName() + " = " + result);
        return result;
    }
    public boolean equals(Object obj){
        // Both classes must match exactly!!!
        // Since this class has no state, there are no attributes to compare
        return ( obj != null && this.getClass() == obj.getClass() );
    }
}

