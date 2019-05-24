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

public class Parameter extends BaseLanguageObject implements Expression {

    private Class<?> type;
    private int valueIndex;
    private String dependentValueId;

    @Override
    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    @Override
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    /**
     * 0-based index of the parameter values in the {@link BulkCommand#getParameterValues()} row value
     * @return
     */
    public int getValueIndex() {
        return valueIndex;
    }

    /**
     * The id of the dependent values this parameter references.  Dependent values are available via {@link Select#getDependentValues()}
     * Will only be set for dependent join pushdown.
     * @return
     */
    public String getDependentValueId() {
        return dependentValueId;
    }

    public void setDependentValueId(String dependentValueId) {
        this.dependentValueId = dependentValueId;
    }

}
