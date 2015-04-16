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

import java.util.List;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.teiid.olingo.api.ProjectedColumn;

public class ExpandInfo {
    private String navigationName;
    private EdmEntityType entityType;
    private final List<ProjectedColumn> projectedColumns;
    private boolean collection; 
    
    public ExpandInfo(String name, EdmEntityType entityType,
            List<ProjectedColumn> projectedColumns, boolean collection) {
        this.navigationName = name;
        this.entityType = entityType;
        this.projectedColumns = projectedColumns;
        this.collection = collection;
    }

    public String getNavigationName() {
        return navigationName;
    }

    public EdmEntityType getEntityType() {
        return entityType;
    }

    public List<ProjectedColumn> getProjectedColumns() {
        return projectedColumns;
    }

    public boolean isCollection() {
        return collection;
    }
}
