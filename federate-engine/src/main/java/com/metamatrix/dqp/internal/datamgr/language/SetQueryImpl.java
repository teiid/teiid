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

import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.visitor.framework.LanguageObjectVisitor;

public class SetQueryImpl extends QueryCommandImpl implements com.metamatrix.connector.language.ISetQuery {

    private boolean all;
    private IQueryCommand leftQuery;
    private IQueryCommand rightQuery;
    private Operation operation;
    
    /** 
     * @see com.metamatrix.dqp.internal.datamgr.language.QueryCommandImpl#getProjectedQuery()
     */
    public IQuery getProjectedQuery() {
        if (leftQuery instanceof IQuery) {
            return (IQuery)leftQuery;
        }
        return leftQuery.getProjectedQuery();
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#getLeftQuery()
     */
    public IQueryCommand getLeftQuery() {
        return leftQuery;
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#getOperation()
     */
    public Operation getOperation() {
        return operation;
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#getRightQuery()
     */
    public IQueryCommand getRightQuery() {
        return rightQuery;
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#isAll()
     */
    public boolean isAll() {
        return all;
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#setAll(boolean)
     */
    public void setAll(boolean all) {
        this.all = all;
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#setLeftQuery(com.metamatrix.connector.language.IQueryCommand)
     */
    public void setLeftQuery(IQueryCommand leftQuery) {
        this.leftQuery = leftQuery;
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#setOperation(com.metamatrix.connector.language.ISetQuery.Operation)
     */
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /** 
     * @see com.metamatrix.connector.language.ISetQuery#setRightQuery(com.metamatrix.connector.language.IQueryCommand)
     */
    public void setRightQuery(IQueryCommand rightQuery) {
        this.rightQuery = rightQuery;
    }

    /**
     * @see com.metamatrix.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}
