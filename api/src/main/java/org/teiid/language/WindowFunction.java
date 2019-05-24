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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.language.visitor.LanguageObjectVisitor;

public class WindowFunction extends BaseLanguageObject implements Expression {

    private AggregateFunction function;
    private WindowSpecification windowSpecification;

    public WindowFunction() {

    }

    public AggregateFunction getFunction() {
        return function;
    }

    public void setFunction(AggregateFunction expression) {
        this.function = expression;
    }

    public WindowSpecification getWindowSpecification() {
        return windowSpecification;
    }

    public void setWindowSpecification(WindowSpecification windowSpecification) {
        this.windowSpecification = windowSpecification;
    }

    @Override
    public Class<?> getType() {
        return function.getType();
    }

    @Override
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(function.hashCode(), windowSpecification);
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof WindowFunction)) {
            return false;
        }
        WindowFunction other = (WindowFunction)obj;
        return EquivalenceUtil.areEqual(this.function, other.function) &&
        EquivalenceUtil.areEqual(this.windowSpecification, other.windowSpecification);
    }

}
