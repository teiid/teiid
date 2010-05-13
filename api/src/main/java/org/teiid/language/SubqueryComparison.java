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

import org.teiid.language.Comparison.Operator;
import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a quantified comparison criteria.  This criteria has an expression on the left,
 * a comparison operator (such as =, &lt;, etc), a quantification operator (ALL, ANY), 
 * and a subquery.
 */
public class SubqueryComparison extends Condition implements Predicate, SubqueryContainer {

	public enum Quantifier {
		SOME,
		ALL
	}
	
    private Expression leftExpr;
    private Operator operator;
    private Quantifier quantifier;
    private QueryExpression query;
    
    /**
     * 
     */
    public SubqueryComparison(Expression leftExpr, Operator operator, Quantifier quantifier, QueryExpression query) {
        this.leftExpr = leftExpr;
        this.operator = operator;
        this.quantifier = quantifier;
        this.query = query;
    }

    public Expression getLeftExpression() {
        return this.leftExpr;
    }

    public Operator getOperator() {
        return this.operator;
    }

    public Quantifier getQuantifier() {
        return this.quantifier;
    }

    public QueryExpression getSubquery() {
        return this.query;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setLeftExpression(Expression expression) {
        this.leftExpr = expression;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public void setQuantifier(Quantifier quantifier) {
        this.quantifier = quantifier;
    }

    public void setSubquery(QueryExpression query) {
        this.query = query;
    }

}
