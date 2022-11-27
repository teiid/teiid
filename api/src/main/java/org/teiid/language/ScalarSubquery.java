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
 * Represents a scalar subquery.  That is, a query that is evaluated as a scalar
 * expression and returns a single value.  The inner subquery must return exactly
 * 1 column as well.
 */
public class ScalarSubquery extends BaseLanguageObject implements Expression, SubqueryContainer {

    private QueryExpression query;

    public ScalarSubquery(QueryExpression query) {
        this.query = query;
    }

    @Override
    public QueryExpression getSubquery() {
        return this.query;
    }

    @Override
    public void setSubquery(QueryExpression query) {
        this.query = query;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Class<?> getType() {
        return query.getProjectedQuery().getDerivedColumns().get(0).getExpression().getType();
    }


}
