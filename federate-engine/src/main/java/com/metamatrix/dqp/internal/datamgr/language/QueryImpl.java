/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr.language;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.data.language.ICriteria;
import com.metamatrix.data.language.IFrom;
import com.metamatrix.data.language.IGroupBy;
import com.metamatrix.data.language.IOrderBy;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.language.ISelectSymbol;
import com.metamatrix.data.visitor.framework.LanguageObjectVisitor;

public class QueryImpl extends QueryCommandImpl implements IQuery {

    private ISelect select = null;
    private IFrom from = null;
    private ICriteria where = null;
    private IGroupBy groupBy = null;
    private ICriteria having = null;
    
    public QueryImpl(ISelect select, IFrom from, ICriteria where,
                     IGroupBy groupBy, ICriteria having, IOrderBy orderBy) {
        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.setOrderBy(orderBy);
    }
    /**
     * @see com.metamatrix.data.language.IQuery#getSelect()
     */
    public ISelect getSelect() {
        return select;
    }

    /**
     * @see com.metamatrix.data.language.IQuery#getFrom()
     */
    public IFrom getFrom() {
        return from;
    }

    /**
     * @see com.metamatrix.data.language.IQuery#getWhere()
     */
    public ICriteria getWhere() {
        return where;
    }

    /**
     * @see com.metamatrix.data.language.IQuery#getGroupBy()
     */
    public IGroupBy getGroupBy() {
        return groupBy;
    }

    /**
     * @see com.metamatrix.data.language.IQuery#getHaving()
     */
    public ICriteria getHaving() {
        return having;
    }

    public String[] getColumnNames() {
        List selectSymbols = getSelect().getSelectSymbols();
        String[] columnNames = new String[selectSymbols.size()];
        int symbolIndex = 0;
        for (Iterator i = selectSymbols.iterator(); i.hasNext(); symbolIndex++) {
            columnNames[symbolIndex] = ((ISelectSymbol)i.next()).getOutputName();
        }
        return columnNames;
    }
    
    public Class[] getColumnTypes() {
        List selectSymbols = getSelect().getSelectSymbols();
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
    
    /**
     * @see com.metamatrix.data.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    /* 
     * @see com.metamatrix.data.language.IQuery#setSelect(com.metamatrix.data.language.ISelect)
     */
    public void setSelect(ISelect select) {
        this.select = select;
    }
    /* 
     * @see com.metamatrix.data.language.IQuery#setFrom(com.metamatrix.data.language.IFrom)
     */
    public void setFrom(IFrom from) {
        this.from = from;
    }
    /* 
     * @see com.metamatrix.data.language.IQuery#setWhere(com.metamatrix.data.language.ICriteria)
     */
    public void setWhere(ICriteria criteria) {
        this.where = criteria;
    }
    /* 
     * @see com.metamatrix.data.language.IQuery#setGroupBy(com.metamatrix.data.language.IGroupBy)
     */
    public void setGroupBy(IGroupBy groupBy) {
        this.groupBy = groupBy;
    }
    /* 
     * @see com.metamatrix.data.language.IQuery#setHaving(com.metamatrix.data.language.ICriteria)
     */
    public void setHaving(ICriteria criteria) {
        this.having = criteria;
    }
    
    /** 
     * @see com.metamatrix.dqp.internal.datamgr.language.QueryCommandImpl#getProjectedQuery()
     */
    public IQuery getProjectedQuery() {
        return this;
    }
}
