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

import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a comparison between two expressions connected with
 * one of the following operators: =, &lt;&gt;, &lt;, &lt;=, &gt;, &gt;=.
 */
public class Comparison extends Condition implements Predicate {

    public enum Operator {
        EQ(Tokens.EQ),
        NE(Tokens.NE),
        LT(Tokens.LT),
        LE(Tokens.LE),
        GT(Tokens.GT),
        GE(Tokens.GE);

        private String toString;
        Operator(String toString) {
            this.toString = toString;
        }
        @Override
        public String toString() {
            return toString;
        }
    }

    private Expression leftExpression;
    private Expression rightExpression;
    private Operator operator;

    public Comparison(Expression left, Expression right, Operator operator) {
        leftExpression = left;
        rightExpression = right;
        this.operator = operator;
    }

    /**
     * Get left expression.
     * @return Left expression
     */
    public Expression getLeftExpression() {
        return leftExpression;
    }

    /**
     * Get right expression.
     * @return Right expression
     */
    public Expression getRightExpression() {
        return rightExpression;
    }

    /**
     * Get the operator
     * @return Operator constant
     * @see Operator
     */
    public Operator getOperator() {
        return this.operator;
    }

    /**
     * Set left expression of criteria
     */
    public void setLeftExpression(Expression expression) {
        this.leftExpression = expression;
    }

    /**
     * Set right expression of criteria
     */
    public void setRightExpression(Expression expression) {
        this.rightExpression = expression;
    }

    /**
     * Set the operator
     * @see Operator
     */
    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}
