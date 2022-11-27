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

public class SetQuery extends QueryExpression {

    public enum Operation {
        /** Represents UNION of two queries */
        UNION,
        /** Represents intersection of two queries */
        INTERSECT,
        /** Represents set difference of two queries */
        EXCEPT
    }

    private boolean all;
    private QueryExpression leftQuery;
    private QueryExpression rightQuery;
    private Operation operation;

    /**
     * @see org.teiid.language.QueryExpression#getProjectedQuery()
     */
    public Select getProjectedQuery() {
        if (leftQuery instanceof Select) {
            return (Select)leftQuery;
        }
        return leftQuery.getProjectedQuery();
    }

    /**
     * @see org.teiid.language.SetQuery#getLeftQuery()
     */
    public QueryExpression getLeftQuery() {
        return leftQuery;
    }

    /**
     * @see org.teiid.language.SetQuery#getOperation()
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * @see org.teiid.language.SetQuery#getRightQuery()
     */
    public QueryExpression getRightQuery() {
        return rightQuery;
    }

    /**
     * @see org.teiid.language.SetQuery#isAll()
     */
    public boolean isAll() {
        return all;
    }

    /**
     * @see org.teiid.language.SetQuery#setAll(boolean)
     */
    public void setAll(boolean all) {
        this.all = all;
    }

    /**
     * @see org.teiid.language.SetQuery#setLeftQuery(org.teiid.language.QueryExpression)
     */
    public void setLeftQuery(QueryExpression leftQuery) {
        this.leftQuery = leftQuery;
    }

    /**
     * @see org.teiid.language.SetQuery#setOperation(org.teiid.language.SetQuery.Operation)
     */
    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /**
     * @see org.teiid.language.SetQuery#setRightQuery(org.teiid.language.QueryExpression)
     */
    public void setRightQuery(QueryExpression rightQuery) {
        this.rightQuery = rightQuery;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}
