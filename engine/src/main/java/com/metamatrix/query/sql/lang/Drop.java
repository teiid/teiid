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

package com.metamatrix.query.sql.lang;

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;


/** 
 * @since 5.5
 */
public class Drop extends Command {
    /** Identifies the table to be dropped. */
    private GroupSymbol table;
    
    public GroupSymbol getTable() {
        return table;
    }

    public void setTable(GroupSymbol table) {
        this.table = table;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#getType()
     * @since 5.5
     */
    public int getType() {
        return Command.TYPE_DROP;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#clone()
     * @since 5.5
     */
    public Object clone() {
        Drop drop =  new Drop();
        GroupSymbol copyTable = (GroupSymbol) table.clone();
        drop.setTable(copyTable);
        copyMetadataState(drop);
        return drop;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#getProjectedSymbols()
     * @since 5.5
     */
    public List getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#areResultsCachable()
     * @since 5.5
     */
    public boolean areResultsCachable() {
        return false;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#updatingModelCount(com.metamatrix.query.metadata.QueryMetadataInterface)
     * @since 5.5
     */
    public int updatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException {
        return 0;
    }

    /** 
     * @see com.metamatrix.query.sql.LanguageObject#acceptVisitor(com.metamatrix.query.sql.LanguageVisitor)
     * @since 5.5
     */
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
    
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.table);
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
        if(!(obj instanceof Drop)) {
            return false;
        }

        Drop other = (Drop) obj;
        
        return EquivalenceUtil.areEqual(getTable(), other.getTable());
    }
}
