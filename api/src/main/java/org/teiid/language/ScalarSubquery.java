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

/**
 * Represents a scalar subquery.  That is, a query that is evaluated as a scalar
 * expression and returns a single value.  The inner subquery must return exactly
 * 1 column as well.
 */
public class ScalarSubquery extends BaseLanguageObject implements Expression, SubqueryContainer {

    private QueryExpression query;

    public ScalarSubquery(QueryExpression query) {
    	this.query = query;
    }

    @Override
    public QueryExpression getSubquery() {
        return this.query;
    }
    
    @Override
    public void setSubquery(QueryExpression query) {
    	this.query = query;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    
    @Override
    public Class<?> getType() {
    	return query.getProjectedQuery().getDerivedColumns().get(0).getExpression().getType();
    }


}
