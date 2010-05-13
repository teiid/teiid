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
import org.teiid.metadata.Table;

/**
 * Represents a table in the language objects.  An example would 
 * be a table reference in the FROM clause.
 */
public class NamedTable extends BaseLanguageObject implements MetadataReference<Table>, TableReference {

    private String correlationName;
    private String name;    
    private Table metadataObject;
    
    public NamedTable(String name, String correlationName, Table group) {
        this.name = name;
        this.correlationName = correlationName;
        this.metadataObject = group;
    }

    public String getCorrelationName() {
        return correlationName;
    }

    /**
     * Gets the name of the table.  Will typically match the name in the metadata.
     * @return
     */
    public String getName() {
        return this.name;
    }

    @Override
    public Table getMetadataObject() {
    	return this.metadataObject;
    }
    
    public void setMetadataObject(Table metadataObject) {
		this.metadataObject = metadataObject;
	}

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setName(String definition) {
        this.name = definition;
    }

    public void setCorrelationName(String context) {
        this.correlationName = context;
    }
    
}
