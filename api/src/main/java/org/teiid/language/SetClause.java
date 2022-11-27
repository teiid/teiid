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

public class SetClause extends BaseLanguageObject {

    private ColumnReference symbol;
    private Expression value;

    public SetClause(ColumnReference symbol, Expression value) {
        this.symbol = symbol;
        this.value = value;
    }

    public ColumnReference getSymbol() {
        return symbol;
    }

    public Expression getValue() {
        return value;
    }

    public void setSymbol(ColumnReference symbol) {
        this.symbol = symbol;
    }

    public void setValue(Expression value) {
        this.value = value;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}
