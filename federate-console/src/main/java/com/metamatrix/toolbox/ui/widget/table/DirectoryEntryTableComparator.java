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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.table;

/**
@since Golden Gate
@version Golden Gate
@author K. E. Goring
*/
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.DirectoryEntryNameAndTypeComparator;

public class DirectoryEntryTableComparator
extends DefaultTableComparator
implements TableComparator {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    private static final DirectoryEntryTableComparator INSTANCE = new DirectoryEntryTableComparator();
    private static DirectoryEntryNameAndTypeComparator directoryEntryComparator;

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public static DefaultTableComparator getInstance() {
        directoryEntryComparator = new DirectoryEntryNameAndTypeComparator();
        return INSTANCE;
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public int compare(final Object firstValue, final Object secondValue, final int columnIndex) {
        if (firstValue == null  &&  secondValue == null) {
            return 0;
        }
        if (firstValue == null) {
            return -1;
        }
        if (secondValue == null) {
            return 1;
        }
        if (firstValue instanceof Number  &&  secondValue instanceof Number) {
            final double diff = ((Number)firstValue).doubleValue() - ((Number)secondValue).doubleValue();
            if (diff > 0.0d) {
                return 1;
            }
            if (diff < 0.0d) {
                return -1;
            }
            return 0;
        }
        if (firstValue instanceof String  &&  secondValue instanceof String) {
            return firstValue.toString().compareToIgnoreCase(secondValue.toString());
        }
        if (firstValue instanceof DirectoryEntry && secondValue instanceof DirectoryEntry) {
            DirectoryEntry de1 = (DirectoryEntry)firstValue;
            DirectoryEntry de2 = (DirectoryEntry)secondValue;
            return directoryEntryComparator.compare(de1, de2);
        }
        if (firstValue instanceof Comparable  &&  firstValue.getClass().isInstance(secondValue)) {
            return ((Comparable)firstValue).compareTo(secondValue);
        }
        return firstValue.toString().compareTo(secondValue.toString());
    }
}
