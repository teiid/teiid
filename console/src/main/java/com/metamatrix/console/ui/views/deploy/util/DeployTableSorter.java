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

//#############################################################################
package com.metamatrix.console.ui.views.deploy.util;

import com.metamatrix.common.namedobject.BaseObject;

import com.metamatrix.toolbox.ui.widget.table.DefaultTableComparator;

/**
 * A table sorter that works with the domain objects of the deployments
 * subsystem. These domain objects implement the {@link Comparable} interface
 * but that doesn't sort by the toString() method.
 * @version 1.0
 * @author Dan Florian
 */
public final class DeployTableSorter
    extends DefaultTableComparator {

    public int compare(
        final Object theFirstValue,
        final Object theSecondValue,
        final int theColumn) {

        if ((theFirstValue == null)  &&  (theSecondValue == null)) {
            return 0;
        }
        if (theFirstValue == null) {
            return -1;
        }
        if (theSecondValue == null) {
            return 1;
        }
        if (theFirstValue instanceof BaseObject) {
            return super.compare(theFirstValue.toString(),
                                 theSecondValue.toString(),
                                 theColumn);
        }
        return super.compare(theFirstValue, theSecondValue, theColumn);
    }

}

