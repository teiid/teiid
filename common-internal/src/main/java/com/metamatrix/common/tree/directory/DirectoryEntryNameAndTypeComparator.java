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

import java.util.Comparator;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.tree.TreeNodePathComparator;
import com.metamatrix.common.util.ErrorMessageKeys;

public class DirectoryEntryNameAndTypeComparator implements Comparator {

    public int compare(DirectoryEntry entry1, DirectoryEntry entry2){
        if ( entry1 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0030));
        }
        if ( entry2 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0031));
        }

        if ( entry1.getType() != entry2.getType() ) {
            if ( entry1.getType() == DirectoryEntry.TYPE_FOLDER ) {
                return -1;
            }
            return 1;
        }
        String value1 = entry1.getName();
        String value2 = entry2.getName();
        if ( value1 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0032));
        }
        if ( value2 == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0033));
        }
        //return value1.compareTo(value2);
        return value1.compareToIgnoreCase(value2);
    }
    public boolean equals(TreeNodePathComparator comparator){
        if ( comparator == null ) {
            return false;
        }
        return false;
    }
    public int compare(Object entry1, Object entry2){
        throw new ClassCastException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0034));
    }
    public boolean equals(Object entry){
        throw new ClassCastException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0035));
    }
}

