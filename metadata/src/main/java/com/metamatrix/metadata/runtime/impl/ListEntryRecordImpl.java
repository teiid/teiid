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

package com.metamatrix.metadata.runtime.impl;

import com.metamatrix.modeler.core.metadata.runtime.ListEntryRecord;

/**
 * ListEntryRecordImpl
 */
public class ListEntryRecordImpl implements ListEntryRecord {
    
    private String uuid;
    private int position;
    
    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================
    
    public ListEntryRecordImpl(final String uuid, final int position) {
        this.uuid     = uuid;
        this.position = position;
    }

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ListEntryRecord#getPosition()
     */
    public int getPosition() {
        return this.position;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ListEntryRecord#getUUID()
     */
    public String getUUID() {
        return this.uuid;
    }

}
