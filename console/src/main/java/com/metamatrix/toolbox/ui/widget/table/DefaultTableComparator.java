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
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTableComparator
implements TableComparator {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    private static final DefaultTableComparator INSTANCE = new DefaultTableComparator();

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public static DefaultTableComparator getInstance() {
        return INSTANCE;
    }

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private boolean ignoresCase;
        
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected int compare(final Comparable firstValue, final Comparable secondValue) {
        if (firstValue instanceof String  &&  ignoresCase) {
            return ((String)firstValue).compareToIgnoreCase((String)secondValue);
        }
        return firstValue.compareTo(secondValue);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int compare(final Object firstValue, 
                        final Object secondValue, 
                        final int columnIndex) { 
                        //final int firstValueRow,
                        //final int secondValueRow) {
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
        if (firstValue instanceof Comparable  &&  firstValue.getClass().isInstance(secondValue)) {
            return compare((Comparable)firstValue, (Comparable)secondValue);
        }
        return compare(firstValue.toString(), secondValue.toString());
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean ignoresCase() {
        return ignoresCase;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setIgnoresCase(final boolean ignoresCase) {
        this.ignoresCase = ignoresCase;
    }
}
