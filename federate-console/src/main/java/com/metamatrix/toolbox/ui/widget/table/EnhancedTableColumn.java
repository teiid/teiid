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

// System imports
import javax.swing.table.TableColumn;

/**
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class EnhancedTableColumn extends TableColumn {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private boolean isHidden = false;
    private boolean isSorted = false;
    private boolean isSortedAscending = true;
    private int sortPriority = 0;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public EnhancedTableColumn(final int modelIndex) {
        super(modelIndex);
        initializeEnhancedTableColumn();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The column's sort priority relative to one if more than one column is sorted, zero otherwise
    @since 2.0
    */
    public int getSortPriority() {
        return sortPriority;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeEnhancedTableColumn() {
        setMaxWidth(Short.MAX_VALUE);
        setPreferredWidth(getMinWidth());
        setWidth(getPreferredWidth());
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if the column is hidden
    @since 2.0
    */
    public boolean isHidden() {
        return isHidden;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if the column is sorted
    @since 2.0
    */
    public boolean isSorted() {
        return isSorted;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if the column is sorted in ascending order
    @since 2.0
    */
    public boolean isSortedAscending() {
        return (isSorted  &&  isSortedAscending);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if the column is sorted in ascending order
    @since 2.0
    */
    public boolean isSortedDescending() {
        return (isSorted  &&  !isSortedAscending);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    void setHidden(final boolean isHidden) {
        this.isHidden = isHidden;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    void setNotSorted() {
        isSorted = false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setMaxWidth(final int width) {
        if (width > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Max column width cannot exceed Short.MAX_VALUE");
        }
        super.setMaxWidth(width);
        if (width < getPreferredWidth()) {
            setPreferredWidth(width);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setMinWidth(final int width) {
        super.setMinWidth(width);
        if (width > getPreferredWidth()) {
            setPreferredWidth(width);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setPreferredWidth(final int width) {
        super.setPreferredWidth(width);
        if (width < getMinWidth()) {
            setMinWidth(width);
        }
        if (width > getMaxWidth()) {
            setMaxWidth(width);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the column's sort priority relative to one
    @since 2.0
    */
    void setSortPriority(final int sortPriority) {
        this.sortPriority = sortPriority;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    void setSortedAscending() {
        isSorted = true;
        isSortedAscending = true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    void setSortedDescending() {
        isSorted = true;
        isSortedAscending = false;
    }
}
