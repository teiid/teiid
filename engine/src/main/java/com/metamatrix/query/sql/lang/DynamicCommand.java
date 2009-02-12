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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

public class DynamicCommand extends Command {

    private Expression sql;
    
    private List asColumns;
    
    private GroupSymbol intoGroup;
    
    private int updatingModelCount;
    
    private SetClauseList using;
    
    private boolean asClauseSet;
        
    public DynamicCommand() {
        super();
    }
    
    public DynamicCommand(Expression sql, List columns, GroupSymbol intoGroup, SetClauseList using) {
        super();
        this.sql = sql;
        this.asColumns = columns;
        this.intoGroup = intoGroup;
        this.using = using;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.QueryCommand#clone()
     */
    public Object clone() {
        DynamicCommand clone = new DynamicCommand();
        
        clone.setSql((Expression)getSql().clone());
        if (asColumns != null) {
            List cloneColumns = new ArrayList(asColumns.size());
            Iterator i = asColumns.iterator();
            while (i.hasNext()) {
                cloneColumns.add(((ElementSymbol)i.next()).clone());
            }
            clone.setAsColumns(cloneColumns);
        }
        
        if (intoGroup != null) {
            clone.setIntoGroup((GroupSymbol)intoGroup.clone());
        }

        if (using != null) {
            clone.setUsing((SetClauseList)using.clone());
        }
        
        clone.setUpdatingModelCount(getUpdatingModelCount());
        copyMetadataState(clone);
        clone.setAsClauseSet(isAsClauseSet());
        return clone;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#getType()
     */
    public int getType() {
        return Command.TYPE_DYNAMIC;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#getProjectedSymbols()
     * 
     * Once past resolving, an EMPTY set of project columns indicates that the
     * project columns of the actual command do not need to be checked during
     * processing.
     */
    public List getProjectedSymbols() {
        if (intoGroup != null) {
            return Command.getUpdateCommandSymbol();
        }
        
        if (asColumns != null) {
            return asColumns;
        }
        
        return Collections.EMPTY_LIST;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#areResultsCachable()
     */
    public boolean areResultsCachable() {
        return false;
    }
    
    public void setUpdatingModelCount(int count) {
        if (count < 0) {
            count = 0;
        } else if (count > 2) {
            count = 2;
        }
        this.updatingModelCount = count;
    }
    
    public int getUpdatingModelCount() {
        return this.updatingModelCount;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#updatingModelCount(com.metamatrix.query.metadata.QueryMetadataInterface)
     */
    public int updatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException {
        return updatingModelCount;
    }

    /** 
     * @see com.metamatrix.query.sql.LanguageObject#acceptVisitor(com.metamatrix.query.sql.LanguageVisitor)
     */
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
    
    /** 
     * @return Returns the columns.
     */
    public List getAsColumns() {
        if (this.asColumns == null) {
            return Collections.EMPTY_LIST;
        }
        return this.asColumns;
    }

    /** 
     * @param columns The columns to set.
     */
    public void setAsColumns(List columns) {
        this.asColumns = columns;
    }
    
    /** 
     * @return Returns the intoGroup.
     */
    public GroupSymbol getIntoGroup() {
        return this.intoGroup;
    }
    
    /** 
     * @param intoGroup The intoGroup to set.
     */
    public void setIntoGroup(GroupSymbol intoGroup) {
        this.intoGroup = intoGroup;
    }
    
    /** 
     * @return Returns the sql.
     */
    public Expression getSql() {
        return this.sql;
    }
    
    /** 
     * @param sql The sql to set.
     */
    public void setSql(Expression sql) {
        this.sql = sql;
    }
            
    /** 
     * @return Returns the using.
     */
    public SetClauseList getUsing() {
        return this.using;
    }
    
    /** 
     * @param using The using to set.
     */
    public void setUsing(SetClauseList using) {
        this.using = using;
    }
    
    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (!(obj instanceof DynamicCommand)) {
            return false;
        }
        
        DynamicCommand other = (DynamicCommand)obj;
        
        return this.updatingModelCount == other.updatingModelCount && 
        EquivalenceUtil.areEqual(getAsColumns(), other.getAsColumns()) &&
        EquivalenceUtil.areEqual(getSql(), other.getSql()) &&
        EquivalenceUtil.areEqual(getIntoGroup(), other.getIntoGroup()) &&
        EquivalenceUtil.areEqual(getUsing(), other.getUsing());
    }
    
    /** 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.sql);
        myHash = HashCodeUtil.hashCode(myHash, this.asColumns);
        return myHash;
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    /** 
     * @return Returns the asClauseSet.
     */
    public boolean isAsClauseSet() {
        return this.asClauseSet;
    }

    /** 
     * @param asClauseSet The asClauseSet to set.
     */
    public void setAsClauseSet(boolean asClauseSet) {
        this.asClauseSet = asClauseSet;
    }

}
