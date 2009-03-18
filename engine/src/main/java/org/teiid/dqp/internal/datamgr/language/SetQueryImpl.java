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

import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class SetQueryImpl extends QueryCommandImpl implements org.teiid.connector.language.ISetQuery {

    private boolean all;
    private IQueryCommand leftQuery;
    private IQueryCommand rightQuery;
    private Operation operation;
    
    /** 
     * @see org.teiid.dqp.internal.datamgr.language.QueryCommandImpl#getProjectedQuery()
     */
    public IQuery getProjectedQuery() {
        if (leftQuery instanceof IQuery) {
            return (IQuery)leftQuery;
        }
        return leftQuery.getProjectedQuery();
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#getLeftQuery()
     */
    public IQueryCommand getLeftQuery() {
        return leftQuery;
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#getOperation()
     */
    public Operation getOperation() {
        return operation;
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#getRightQuery()
     */
    public IQueryCommand getRightQuery() {
        return rightQuery;
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#isAll()
     */
    public boolean isAll() {
        return all;
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#setAll(boolean)
     */
    public void setAll(boolean all) {
        this.all = all;
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#setLeftQuery(org.teiid.connector.language.IQueryCommand)
     */
    public void setLeftQuery(IQueryCommand leftQuery) {
        this.leftQuery = leftQuery;
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#setOperation(org.teiid.connector.language.ISetQuery.Operation)
     */
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /** 
     * @see org.teiid.connector.language.ISetQuery#setRightQuery(org.teiid.connector.language.IQueryCommand)
     */
    public void setRightQuery(IQueryCommand rightQuery) {
        this.rightQuery = rightQuery;
    }

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}
