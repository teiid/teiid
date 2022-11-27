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
package org.teiid.odata.api;

import org.teiid.core.util.EquivalenceUtil;

public class SQLParameter {
    final Object value;
    final Integer sqlType;
    final String name;

    public SQLParameter(Object value, Integer sqlType) {
        this.name = null;
        this.value = value;
        this.sqlType = sqlType;
    }

    public SQLParameter(String name, Object value, Integer sqlType) {
        this.name = name;
        this.value = value;
        this.sqlType = sqlType;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public Integer getSqlType() {
        return sqlType;
    }

    @Override
    public int hashCode() {
        return value == null ? 0 : value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SQLParameter)) {
            return false;
        }
        return EquivalenceUtil.areEqual(value, ((SQLParameter)obj).value);
    }
}