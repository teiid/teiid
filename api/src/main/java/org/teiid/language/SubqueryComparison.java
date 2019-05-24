/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
