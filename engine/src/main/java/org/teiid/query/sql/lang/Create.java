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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
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
    
    private List<ElementSymbol> columns = new ArrayList<ElementSymbol>();
    
    public GroupSymbol getTable() {
        return table;
    }

    public void setTable(GroupSymbol table) {
        this.table = table;
    }
    
    public List<ElementSymbol> getColumns() {
        return columns;
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
        GroupSymbol copyTable = (GroupSymbol) table.clone();    
        copy.setTable(copyTable);
        copy.setColumns(columns);
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

    public void setColumns(List<ElementSymbol> columns) {
        this.columns = columns;
    }
    
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.table);
        myHash = HashCodeUtil.hashCode(myHash, this.columns);
        return myHash;
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
        
        return EquivalenceUtil.areEqual(getTable(), other.getTable()) &&
               EquivalenceUtil.areEqual(getColumns(), other.getColumns());
    }
}
