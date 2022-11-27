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

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a searched CASE expression:
 * <br> CASE WHEN criteria THEN expression ... END
 */
public class SearchedCase extends BaseLanguageObject implements Expression {

    private List<SearchedWhenClause> cases;
    private Expression elseExpression;
    private Class<?> type;

    public SearchedCase(List<SearchedWhenClause> cases, Expression elseExpression, Class<?> type) {
        this.cases = cases;
        this.elseExpression = elseExpression;
        this.type = type;
    }

    public Expression getElseExpression() {
        return elseExpression;
    }

    public List<SearchedWhenClause> getCases() {
        return cases;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setElseExpression(Expression expression) {
        this.elseExpression = expression;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

}
