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

public class SortSpecification extends BaseLanguageObject {

    public enum Ordering {
        ASC,
        DESC
    }

    public enum NullOrdering {
        FIRST,
        LAST
    }

    private Ordering ordering;
    private Expression expression;
    private NullOrdering nullOrdering;

    public SortSpecification(Ordering direction, Expression expression) {
        this.ordering = direction;
        this.expression = expression;
    }

    public Ordering getOrdering() {
        return ordering;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setOrdering(Ordering direction) {
        this.ordering = direction;
    }

    public Expression getExpression() {
        return this.expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public void setNullOrdering(NullOrdering nullOrdering) {
        this.nullOrdering = nullOrdering;
    }

    public NullOrdering getNullOrdering() {
        return nullOrdering;
    }

}
