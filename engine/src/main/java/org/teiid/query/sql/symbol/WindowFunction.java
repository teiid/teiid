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

 package org.teiid.query.sql.symbol;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class WindowFunction implements LanguageObject, DerivedExpression, NamedExpression {

    private AggregateSymbol function;
    private WindowSpecification windowSpecification;

    public AggregateSymbol getFunction() {
        return function;
    }

    public void setFunction(AggregateSymbol expression) {
        this.function = expression;
        this.function.setWindowed(true);
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
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(function.hashCode(), windowSpecification);
    }

    @Override
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

    @Override
    public WindowFunction clone() {
        WindowFunction clone = new WindowFunction();
        clone.setFunction((AggregateSymbol) this.function.clone());
        clone.setWindowSpecification(this.windowSpecification.clone());
        return clone;
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public String getName() {
        return function.getName();
    }

}
