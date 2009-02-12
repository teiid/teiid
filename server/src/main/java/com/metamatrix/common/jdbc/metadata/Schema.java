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

package com.metamatrix.common.jdbc.metadata;

import com.metamatrix.core.util.ArgCheck;

public class Schema extends JDBCNamespace {
    public Schema() {
        super();
    }

    public Schema(String name) {
        super(name);
    }

    public void add(Table object) {
        super.addContent(object);
    }

    public boolean remove(Table object) {
        return super.removeContent(object);
    }

    public boolean contains(Table object) {
        return super.hasContent(object);
    }

    public void add(Index object) {
        super.addContent(object);
    }

    public boolean remove(Index object) {
        return super.removeContent(object);
    }

    public boolean contains(Index object) {
        return super.hasContent(object);
    }

    public void add(Procedure object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Procedure reference may not be null"); //$NON-NLS-1$
        }
        super.addContent(object);
    }

    public boolean remove(Procedure object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Procedure reference may not be null"); //$NON-NLS-1$
        }
        return super.removeContent(object);
    }

    public boolean contains(Procedure object) {
        if(object == null){
            ArgCheck.isNotNull(object, "The Procedure reference may not be null"); //$NON-NLS-1$
        }
        return super.hasContent(object);
    }
    

    public Table lookupTable(String tableName) {
        return (Table)super.lookupContent(tableName, Table.class);
    }

    public Index lookupIndex(String indexName) {
        return (Index)super.lookupContent(indexName, Index.class);
    }

    public Procedure lookupProcedure(String procedureName) {
        return (Procedure)super.lookupContent(procedureName, Procedure.class);
    }

}



