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

public class IndexColumn extends Column {

    private boolean ascending;
    private Column referencedColumn;

    public IndexColumn() {
        super();
    }

    public IndexColumn(String name) {
        super(name);
    }

    public boolean isAscending() {
        return ascending;
    }


    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    /**
     * Returns the referencedColumn.
     * @return Column
     */
    public Column getReferencedColumn() {
        return referencedColumn;
    }


    /**
     * Sets the referencedColumn.
     * @param referencedColumn The referencedColumn to set
     */
    public void setReferencedColumn(Column referencedColumn) {
        this.referencedColumn = referencedColumn;
    }


}
