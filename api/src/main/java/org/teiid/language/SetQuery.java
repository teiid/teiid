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

public class SetQuery extends QueryExpression {

    public enum Operation {
        /** Represents UNION of two queries */
        UNION,
        /** Represents intersection of two queries */
        INTERSECT,
        /** Represents set difference of two queries */
        EXCEPT
    }
	
    private boolean all;
    private QueryExpression leftQuery;
    private QueryExpression rightQuery;
    private Operation operation;
    
    /** 
     * @see org.teiid.language.QueryExpression#getProjectedQuery()
     */
    public Select getProjectedQuery() {
        if (leftQuery instanceof Select) {
            return (Select)leftQuery;
        }
        return leftQuery.getProjectedQuery();
    }

    /** 
     * @see org.teiid.language.SetQuery#getLeftQuery()
     */
    public QueryExpression getLeftQuery() {
        return leftQuery;
    }

    /** 
     * @see org.teiid.language.SetQuery#getOperation()
     */
    public Operation getOperation() {
        return operation;
    }

    /** 
     * @see org.teiid.language.SetQuery#getRightQuery()
     */
    public QueryExpression getRightQuery() {
        return rightQuery;
    }

    /** 
     * @see org.teiid.language.SetQuery#isAll()
     */
    public boolean isAll() {
        return all;
    }

    /** 
     * @see org.teiid.language.SetQuery#setAll(boolean)
     */
    public void setAll(boolean all) {
        this.all = all;
    }

    /** 
     * @see org.teiid.language.SetQuery#setLeftQuery(org.teiid.language.QueryExpression)
     */
    public void setLeftQuery(QueryExpression leftQuery) {
        this.leftQuery = leftQuery;
    }

    /** 
     * @see org.teiid.language.SetQuery#setOperation(org.teiid.language.SetQuery.Operation)
     */
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /** 
     * @see org.teiid.language.SetQuery#setRightQuery(org.teiid.language.QueryExpression)
     */
    public void setRightQuery(QueryExpression rightQuery) {
        this.rightQuery = rightQuery;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}
