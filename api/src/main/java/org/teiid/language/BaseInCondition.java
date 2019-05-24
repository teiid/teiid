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

public abstract class BaseInCondition extends Condition implements Predicate {

    private Expression leftExpression;
    private boolean negated;

    public BaseInCondition(Expression leftExpression, boolean negated) {
        this.leftExpression = leftExpression;
        this.negated = negated;
    }

    /**
     * Get left expression of IN criteria
     * @return Left expression
     */
    public Expression getLeftExpression() {
        return leftExpression;
    }

    /**
     * Set left expression of IN criteria
     */
    public void setLeftExpression(Expression leftExpression) {
        this.leftExpression = leftExpression;
    }

    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    public boolean isNegated() {
        return negated;
    }

    /**
     * Sets whether this criteria is negated.
     * @param negated Flag indicating whether this criteria contains a NOT
     */
    public void setNegated(boolean negated) {
        this.negated = negated;
    }
}
