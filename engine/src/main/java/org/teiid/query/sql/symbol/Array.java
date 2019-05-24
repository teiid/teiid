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

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class Array implements Expression {

    private Class<?> type;
    private List<Expression> expressions;
    private boolean implicit;

    public Array(List<Expression> expressions) {
        this.expressions = expressions;
    }

    public Array(Class<?> baseType, List<Expression> expresssions) {
        setComponentType(baseType);
        this.expressions = expresssions;
    }

    @Override
    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        if (type != null) {
            Assertion.assertTrue(type.isArray());
        }
        this.type = type;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Array clone() {
        Array clone = new Array(LanguageObject.Util.deepClone(getExpressions(), Expression.class));
        clone.type = type;
        clone.implicit = implicit;
        return clone;
    }

    public Class<?> getComponentType() {
        if (this.type != null) {
            return this.type.getComponentType();
        }
        return null;
    }

    public void setComponentType(Class<?> baseType) {
        if (baseType != null) {
            this.type = DataTypeManager.getArrayType(baseType);
        } else {
            this.type = null;
        }
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.expHashCode(0, getExpressions());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Array)) {
            return false;
        }
        Array other = (Array) obj;
        return EquivalenceUtil.areEqual(type, other.type) && EquivalenceUtil.areEqual(expressions, other.expressions);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public void setImplicit(boolean implicit) {
        this.implicit = implicit;
    }

    /**
     * If the array has been implicitly constructed, such as with vararg parameters
     * @return
     */
    public boolean isImplicit() {
        return implicit;
    }

}
