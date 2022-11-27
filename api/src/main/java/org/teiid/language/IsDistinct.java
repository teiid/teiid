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

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a comparison between two expressions with null equality
 */
public class IsDistinct extends Condition implements Predicate {

    private Expression leftExpression;
    private Expression rightExpression;
    private boolean negated;

    public IsDistinct(Expression left, Expression right, boolean negated) {
        leftExpression = left;
        rightExpression = right;
        this.negated = negated;
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

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setNegated(boolean negated) {
        this.negated = negated;
    }

    public boolean isNegated() {
        return negated;
    }

}
