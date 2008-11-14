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

package com.metamatrix.modeler.core.util;

import java.io.Serializable;
import java.util.Comparator;

import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.modeler.core.metadata.runtime.ColumnRecord;

/**
 */
public class ColumnRecordComparator implements Comparator, Serializable {

    /* 
     *  This method compares the objects with respect to their position.
     *  @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    public int compare(Object colRec1, Object colRec2) {
        ArgCheck.isInstanceOf(ColumnRecord.class, colRec1);
        ArgCheck.isInstanceOf(ColumnRecord.class, colRec2);

        int position1 = ((ColumnRecord)colRec1).getPosition();
        int position2 = ((ColumnRecord)colRec2).getPosition();

        return position1 - position2;
    }

    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        
        if(anObject == this) {
            return true;
        }

        if(anObject == null || anObject.getClass() != this.getClass()) {
            return false;
        }

        return true;
    }
}