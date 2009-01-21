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

package com.metamatrix.common.jdbc.metadata;

import com.metamatrix.core.util.ArgCheck;

public class UniqueKey extends ColumnSet {
    private boolean primary;
    private String tableName;

    public UniqueKey() {
        super();
    }

    public UniqueKey(String name) {
        super(name);
    }

    public UniqueKey(String catalogName, String schemaName, String tableName, String name) {
        super(catalogName, schemaName, name);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public boolean generateName() {
        String prefix = null;
        if ( this.isPrimary() ) {
            prefix = "PK_"; //$NON-NLS-1$
        } else {
            prefix = "UL_"; //$NON-NLS-1$
        }
        return super.generateUniqueName(prefix);
    }

    public void print(java.io.PrintStream stream) {
        print(stream, "  "); //$NON-NLS-1$
    }

    public void print(java.io.PrintStream stream, String lead) {
        if(stream == null){
            ArgCheck.isNotNull(stream, "The stream reference may not be null"); //$NON-NLS-1$
        }
        stream.println(lead + this.getName() + (this.isMarked()? " <marked>" : "") + " (" + this.getColumnNames() + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
}



