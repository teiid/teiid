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

package com.metamatrix.console.ui.views;

import java.util.Date;

import com.metamatrix.toolbox.ui.widget.table.TableComparator;

public class DefaultConsoleTableComparator implements TableComparator {
    private static DefaultConsoleTableComparator theInstance = null;

    public static DefaultConsoleTableComparator getInstance() {
        if (theInstance == null) {
            theInstance = new DefaultConsoleTableComparator();
        }
        return theInstance;
    }

    private DefaultConsoleTableComparator() {
        super();
    }
    
    public int compare(Object firstValue, Object secondValue, int columnIndex) {
        int result;
        if ((firstValue == null)  &&  (secondValue == null)) {
            result = 0;
        } else if (firstValue == null) {
            result = -1;
        } else if (secondValue == null) {
            result = 1;
        } else if ((firstValue instanceof Number)  &&  (secondValue instanceof Number)) {
            final double diff = ((Number)firstValue).doubleValue() - ((Number)secondValue).doubleValue();
            if (diff > 0.0d) {
                result = 1;
            } else if (diff < 0.0d) {
                result = -1;
            } else {
                result = 0;
            }
        } else if ((firstValue instanceof Boolean) && (secondValue instanceof Boolean)) {
            boolean firstVal = ((Boolean)firstValue).booleanValue();
            boolean secondVal = ((Boolean)secondValue).booleanValue();
            if (firstVal == secondVal) {
                result = 0;
            } else if (firstVal) {
                result = -1;
            } else {
                result = 1;
            }
        } else if ((firstValue instanceof Date) && (secondValue instanceof Date)) {
            long firstTime = ((Date)firstValue).getTime();
            long secondTime = ((Date)secondValue).getTime();
            if (firstTime == secondTime) {
                result = 0;
            } else if (firstTime < secondTime) {
                result = -1;
            } else {
                result = 1;
            }
        } else {
            result = firstValue.toString().compareToIgnoreCase(secondValue.toString());
        }
        return result;
    }
}
