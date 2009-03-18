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

package org.teiid.dqp.internal.datamgr.language;

import java.util.Iterator;
import java.util.List;

import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.ISelectSymbol;


public abstract class QueryCommandImpl extends BaseLanguageObject implements org.teiid.connector.language.IQueryCommand {

    private IOrderBy orderBy = null;
    private ILimit limit = null;

    /**
     * @see org.teiid.connector.language.IQuery#getOrderBy()
     */
    public IOrderBy getOrderBy() {
        return orderBy;
    }

    /**
     * @see org.teiid.connector.language.IQuery#getLimit()
     */
    public ILimit getLimit() {
        return limit;
    }
    
    public String[] getColumnNames() {
        List selectSymbols = getProjectedQuery().getSelect().getSelectSymbols();
        String[] columnNames = new String[selectSymbols.size()];
        int symbolIndex = 0;
        for (Iterator i = selectSymbols.iterator(); i.hasNext(); symbolIndex++) {
            columnNames[symbolIndex] = ((ISelectSymbol)i.next()).getOutputName();
        }
        return columnNames;
    }
    
    public Class[] getColumnTypes() {
        List selectSymbols = getProjectedQuery().getSelect().getSelectSymbols();
        Class[] columnTypes = new Class[selectSymbols.size()];
        int symbolIndex = 0;
        for (Iterator i = selectSymbols.iterator(); i.hasNext(); symbolIndex++) {
            ISelectSymbol symbol = (ISelectSymbol)i.next();
            if (symbol.getExpression() == null) {
                columnTypes[symbolIndex] = null;
            } else {
                columnTypes[symbolIndex] = symbol.getExpression().getType();
            }
        }
        return columnTypes;
    }
    
    /* 
     * @see com.metamatrix.data.language.IQuery#setOrderBy(com.metamatrix.data.language.IOrderBy)
     */
    public void setOrderBy(IOrderBy orderBy) {
        this.orderBy = orderBy;
    }
    
    /* 
     * @see com.metamatrix.data.language.IQuery#setOrderBy(com.metamatrix.data.language.IOrderBy)
     */
    public void setLimit(ILimit limit) {
        this.limit = limit;
    }
}
