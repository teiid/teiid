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

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmEntityType;

public class ComplexReturnType {
    private Entity entity;
    private String name;
    private boolean expand;
    private EdmEntityType type;
    
    public ComplexReturnType(String name, EdmEntityType type, Entity entity, boolean expand) {
        this.name = name;
        this.type = type;
        this.entity = entity;
        this.expand = expand;
    }

    public Entity getEntity() {
        return entity;
    }

    public String getName() {
        return name;
    }

    public boolean isExpand() {
        return expand;
    }

    public EdmEntityType getEdmEntityType() {
        return type;
    }
}