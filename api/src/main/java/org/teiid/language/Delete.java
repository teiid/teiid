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

import java.util.Iterator;
import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a DELETE command.
 */
public class Delete extends BaseLanguageObject implements BatchedCommand {

    private NamedTable table;
    private Condition where;
    private Iterator<? extends List<?>> parameterValues;
    
    public Delete(NamedTable group, Condition criteria) {
        this.table = group;
        this.where = criteria;
    }
    
    /**
     * Get group that is being deleted from.
     * @return Insert group
     */
    public NamedTable getTable() {
        return table;
    }

    /** 
     * Get criteria that is being used with the delete - may be null
     * @return Criteria, may be null
     */
    public Condition getWhere() {
        return where;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    
    /**
     * Set group that is being deleted from.
     * @param group Insert group
     */
    public void setTable(NamedTable group) {
        this.table = group;
    }

    /** 
     * Set criteria that is being used with the delete - may be null
     * @param criteria Criteria, may be null
     */
    public void setWhere(Condition criteria) {
        this.where = criteria;
    }
    
    @Override
    public Iterator<? extends List<?>> getParameterValues() {
    	return parameterValues;
    }
    
    public void setParameterValues(Iterator<? extends List<?>> parameterValues) {
		this.parameterValues = parameterValues;
	}

}
