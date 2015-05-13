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
package org.teiid.olingo.service;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.teiid.metadata.Table;
import org.teiid.query.sql.symbol.GroupSymbol;

class Lambda {
    static enum Kind {ALL, ANY};
    
    private EdmEntityType type;
    private String name;
    private boolean collection;
    private Kind kind;
    private Table table;
    private GroupSymbol groupSymbol;
    
    public EdmEntityType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public boolean isCollection() {
        return collection;
    }
    
    public Kind getKind() {
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    public void setType(EdmEntityType type) {
        this.type = type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCollection(boolean collection) {
        this.collection = collection;
    } 
    
    public GroupSymbol getGroupSymbol() {
        return this.groupSymbol;
    }

    public Table getTable() {
        return this.table;
    }
    
    public void setTable(Table table) {
        this.table = table;
    }

    public void setGroupSymbol(GroupSymbol groupSymbol) {
        this.groupSymbol = groupSymbol;
    }
}