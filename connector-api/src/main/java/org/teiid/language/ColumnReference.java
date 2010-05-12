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

package org.teiid.language;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.Column;

/**
 * Represents an element in the language.  An example of an element 
 * would be a column reference in a SELECT clause. 
 */
public class ColumnReference extends BaseLanguageObject implements MetadataReference<Column>, Expression {

    private NamedTable table;
    private String name;
    private Column metadataObject;
    private Class<?> type;
    
    public ColumnReference(NamedTable group, String name, Column metadataObject, Class<?> type) {
        this.table = group;
        this.name = name;
        this.metadataObject = metadataObject;
        this.type = type;
    }
    
    /**
     * Gets the name of the element.
     * @return the name of the element
     */
    public String getName() {
        return this.name;
    }

    /**
     * Return the table that contains this column.  May be null.
     * @return The group reference
     */
    public NamedTable getTable() {
        return table;
    }

    @Override
    public Column getMetadataObject() {
    	return this.metadataObject;
    }
    
    public void setMetadataObject(Column metadataObject) {
		this.metadataObject = metadataObject;
	}
    
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    
    public void setTable(NamedTable group) {
        this.table = group;
    }

    public Class<?> getType() {
        return this.type;
    }

    /**
     * Sets the name of the element.
     * @param name The name of the element
     */
    public void setName(String name) {
        this.name = name;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

}
