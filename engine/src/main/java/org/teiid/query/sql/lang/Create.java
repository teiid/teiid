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

package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/** 
 * @since 5.5
 */
public class Create extends Command {
    /** Identifies the table to be created. */
    private GroupSymbol table;
    private List<ElementSymbol> primaryKey = new ArrayList<ElementSymbol>();
    private List<Column> columns = new ArrayList<Column>();
    private List<ElementSymbol> columnSymbols;
    
    public GroupSymbol getTable() {
        return table;
    }

    public void setTable(GroupSymbol table) {
        this.table = table;
    }
    
    public List<Column> getColumns() {
        return columns;
    }
    
    public List<ElementSymbol> getPrimaryKey() {
		return primaryKey;
	}
    
    /**
     * Derived ElementSymbol list.  Do not modify without also modifying the columns.
     * @return
     */
    public List<ElementSymbol> getColumnSymbols() {
    	if (columnSymbols == null) {
    		columnSymbols = new ArrayList<ElementSymbol>(columns.size());
    		for (Column column : columns) {
				ElementSymbol es = new ElementSymbol(column.getName());
				es.setType(DataTypeManager.getDataTypeClass(column.getRuntimeType()));
				es.setGroupSymbol(table);
				columnSymbols.add(es);
			}
    	}
		return columnSymbols;
	}
    
    /** 
     * @see org.teiid.query.sql.lang.Command#getType()
     * @since 5.5
     */
    public int getType() {
        return Command.TYPE_CREATE;
    }

    /** 
     * @see org.teiid.query.sql.lang.Command#clone()
     * @since 5.5
     */
    public Object clone() {  
        Create copy = new Create();      
        GroupSymbol copyTable = table.clone();    
        copy.setTable(copyTable);
        copy.columns = new ArrayList<Column>(columns.size());
        for (Column column : columns) {
			Column copyColumn = new Column();
			copyColumn.setName(column.getName());
			copyColumn.setRuntimeType(column.getRuntimeType());
			copyColumn.setAutoIncremented(column.isAutoIncremented());
			copyColumn.setNullType(column.getNullType());
			copy.columns.add(copyColumn);
		}
        copy.primaryKey = LanguageObject.Util.deepClone(primaryKey, ElementSymbol.class);
        copyMetadataState(copy);
        return copy;
    }

    /** 
     * @see org.teiid.query.sql.lang.Command#getProjectedSymbols()
     * @since 5.5
     */
    public List getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }

    /** 
     * @see org.teiid.query.sql.lang.Command#areResultsCachable()
     * @since 5.5
     */
    public boolean areResultsCachable() {
        return false;
    }

    /** 
     * @see org.teiid.query.sql.LanguageObject#acceptVisitor(org.teiid.query.sql.LanguageVisitor)
     * @since 5.5
     */
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public void setElementSymbolsAsColumns(List<ElementSymbol> columns) {
    	this.columns.clear();
    	for (ElementSymbol elementSymbol : columns) {
    		Column c = new Column();
    		c.setName(elementSymbol.getName());
    		c.setRuntimeType(DataTypeManager.getDataTypeName(elementSymbol.getType()));
    		c.setNullType(NullType.Nullable);
    		this.columns.add(c);
		}
    }
    
    public int hashCode() {
        return this.table.hashCode();
    }
    
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
    
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests
        if(!(obj instanceof Create)) {
            return false;
        }

        Create other = (Create) obj;
        
        if (other.columns.size() != this.columns.size()) {
        	return false;
        }
        
        for (int i = 0; i < this.columns.size(); i++) {
        	Column c = this.columns.get(i);
        	Column o = other.columns.get(i);
        	if (!c.getName().equalsIgnoreCase(o.getName()) 
        		|| DataTypeManager.getDataTypeClass(c.getRuntimeType().toLowerCase()) != DataTypeManager.getDataTypeClass(o.getRuntimeType().toLowerCase())
        		|| c.isAutoIncremented() != o.isAutoIncremented()
        		|| c.getNullType() != o.getNullType()) {
        		return false;
        	}
		}
        
        return EquivalenceUtil.areEqual(getTable(), other.getTable()) &&
               EquivalenceUtil.areEqual(getPrimaryKey(), other.getPrimaryKey());
    }
}
