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
 * Represents a logical criteria such as AND, OR, or NOT.
 */
public class AndOr extends Condition {

    public enum Operator {
        AND,
        OR,
    }

    private Condition leftCondition;
    private Condition rightCondition;
    private Operator operator = Operator.AND;

    public AndOr(Condition left, Condition right, Operator operator) {
        this.leftCondition = left;
        this.rightCondition = right;
        this.operator = operator;
    }

    /**
     * Get operator used to connect these criteria.
     * @return Operator constant
     */
    public Operator getOperator() {
        return this.operator;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set operator used to connect these criteria.
     * @param operator Operator constant
     */
    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public Condition getLeftCondition() {
        return leftCondition;
    }

    public Condition getRightCondition() {
        return rightCondition;
    }

    public void setLeftCondition(Condition left) {
        this.leftCondition = left;
    }

    public void setRightCondition(Condition right) {
        this.rightCondition = right;
    }

}
