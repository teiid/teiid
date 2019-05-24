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
 * Represents a literal value that is used in
 * an expression.  The value can be obtained and should match
 * the type specified by {@link #getType()}
 */
public class Literal extends BaseLanguageObject implements Expression {

    private Object value;
    private Class<?> type;
    private boolean isBindEligible;

    public Literal(Object value, Class<?> type) {
        this.value = value;
        this.type = type;
    }

    public Object getValue() {
        return this.value;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    /**
     * Set by the optimizer if the literal was created by the evaluation of another expression.
     * Setting to true will not always result in the value being handled as a bind value.
     * @return
     */
    public boolean isBindEligible() {
        return isBindEligible;
    }

    public void setBindEligible(boolean isBindEligible) {
        this.isBindEligible = isBindEligible;
    }

}
