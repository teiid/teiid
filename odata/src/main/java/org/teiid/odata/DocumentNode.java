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
package org.teiid.odata;

import java.util.LinkedHashMap;
import java.util.Map;

import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmType;
import org.odata4j.exceptions.NotFoundException;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.query.sql.symbol.GroupSymbol;

public class DocumentNode {
    private Table entityTable;
    private Procedure entityProcedure;
    private GroupSymbol group;
    private DocumentNode expandNode;
    private DocumentNode parentNode;
    
    private EdmEntitySet edmEntitySet;
    private LinkedHashMap<String, ProjectedColumn> projectedColumns = 
            new LinkedHashMap<String, DocumentNode.ProjectedColumn>();

    public DocumentNode(Table entityTable, GroupSymbol group) {
        this.entityTable = entityTable;
        this.group = group;
    }
    
    public DocumentNode(Procedure procedure, GroupSymbol group) {
        this.entityProcedure = procedure;
        this.group = group;
    }    
    
    public Table getEntityTable() {
        return this.entityTable;
    }
    
    public GroupSymbol getEntityGroup() {
        return this.group;
    }
    
    public EdmEntityType getEntityType(EdmDataServices metadata) {
        return getEntitySet(metadata).getType();
    }
    
    public Map<String, ProjectedColumn> getProjectedColumns(){
        return this.projectedColumns;
    }
    
    public void addProjectColumn(final String columnName, final int ordinal,
            final boolean visible, final EdmType type) {
        this.projectedColumns.put(columnName, new ProjectedColumn() {
            @Override
            public String name() {
                return columnName;
            }
            @Override
            public EdmType type() {
                return type;
            }
            @Override
            public int ordinal() {
                return ordinal;
            }
            @Override
            public boolean visible() {
                return visible;
            }
            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((name() == null) ? 0 : name().hashCode());
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (getClass() != obj.getClass())
                    return false;
                ProjectedColumn other = (ProjectedColumn) obj;
                if (name() == null) {
                    if (other.name() != null)
                        return false;
                } else if (!name().equals(other.name()))
                    return false;
                return true;
            }            
        });
    }
    
    public void setExpandNode(DocumentNode node) {
        this.expandNode = node;
        this.expandNode.parentNode = this;
    }
    
    public DocumentNode getExpandNode() {
        return this.expandNode;
    }
    
    // every expand node has parent
    public DocumentNode getParentNode() {
        return this.parentNode;
    }
    
    public EdmEntitySet getEntitySet(EdmDataServices metadata) {
        assert(metadata != null);
        if (this.edmEntitySet == null) {
            this.edmEntitySet =  metadata.findEdmEntitySet(this.entityTable.getFullName());
            if (this.edmEntitySet == null) {
                throw new NotFoundException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID16011,
                        this.entityTable.getFullName()));
            }
        }
        return this.edmEntitySet;
    }
    
    
    interface ProjectedColumn {
        String name();
        EdmType type();
        int ordinal();
        boolean visible();
    }
}
