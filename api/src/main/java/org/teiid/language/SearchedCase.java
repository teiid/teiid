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

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a searched CASE expression:
 * <br/> CASE WHEN criteria THEN expression ... END
 */
public class SearchedCase extends BaseLanguageObject implements Expression {
	
    private List<SearchedWhenClause> cases;
    private Expression elseExpression;
    private Class<?> type;
    
    public SearchedCase(List<SearchedWhenClause> cases, Expression elseExpression, Class<?> type) {
        this.cases = cases;
        this.elseExpression = elseExpression;
        this.type = type;
    }

    public Expression getElseExpression() {
        return elseExpression;
    }
    
    public List<SearchedWhenClause> getCases() {
		return cases;
	}
    
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setElseExpression(Expression expression) {
        this.elseExpression = expression;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

}
