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

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.visitor.LanguageObjectVisitor;

public class Array implements Expression {

    private Class<?> baseType;
    private List<Expression> expressions;

    public Array(Class<?> baseType, List<Expression> expresssions) {
        this.baseType = baseType;
        this.expressions = expresssions;
    }

    @Override
    public Class<?> getType() {
        return DataTypeManager.getArrayType(baseType);
    }

    @Override
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public Class<?> getBaseType() {
        return baseType;
    }

    public void setBaseType(Class<?> baseType) {
        this.baseType = baseType;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

}
