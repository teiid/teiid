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
package org.teiid.olingo;

import org.apache.olingo.commons.api.edm.EdmType;
import org.teiid.query.sql.symbol.Expression;

public class ProjectedColumn {
    private Expression expr;
    private boolean visible;
    private EdmType edmType;
    private boolean collection;
    private int ordinal;
    
    public ProjectedColumn(Expression expr, boolean visible, EdmType edmType, boolean collection) {
        this.expr = expr; 
        this.visible = visible;
        this.edmType = edmType;
        this.collection = collection;
    }
    
    public Expression getExpression() {
        return this.expr;
    }
    
    public boolean isVisible() {
        return this.visible;
    }
    
    public EdmType getEdmType() {
        return this.edmType;
    }
    
    public boolean isCollection() {
        return collection;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }
    
}