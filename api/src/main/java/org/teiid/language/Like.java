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

public class Like extends Condition implements Predicate {

    public enum MatchMode {
        LIKE,
        SIMILAR,
        /**
         * The escape char is typically not used in regex mode.
         */
        REGEX
    }

    private Expression leftExpression;
    private Expression rightExpression;
    private Character escapeCharacter;
    private boolean isNegated;
    private MatchMode mode = MatchMode.LIKE;

    public Like(Expression left, Expression right, Character escapeCharacter, boolean negated) {
        leftExpression = left;
        rightExpression = right;
        this.escapeCharacter = escapeCharacter;
        this.isNegated = negated;

    }

    public Expression getLeftExpression() {
        return leftExpression;
    }

    public Expression getRightExpression() {
        return rightExpression;
    }

    public Character getEscapeCharacter() {
        return this.escapeCharacter;
    }

    public boolean isNegated() {
        return this.isNegated;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setLeftExpression(Expression expression) {
        this.leftExpression = expression;
    }

    public void setRightExpression(Expression expression) {
        this.rightExpression = expression;
    }

    public void setEscapeCharacter(Character character) {
        this.escapeCharacter = character;
    }

    public void setNegated(boolean negated) {
        this.isNegated = negated;
    }

    public MatchMode getMode() {
        return mode;
    }

    public void setMode(MatchMode mode) {
        this.mode = mode;
    }

}
