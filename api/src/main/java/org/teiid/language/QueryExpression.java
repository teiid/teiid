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

public abstract class QueryExpression extends BaseLanguageObject implements Command, InsertValueSource {

    private OrderBy orderBy;
    private Limit limit;
    private With with;

    public abstract Select getProjectedQuery();

    /**
     * Get ORDER BY clause, may be null.
     * @return An ORDER BY object
     */
    public OrderBy getOrderBy() {
        return orderBy;
    }

    /**
     * Get LIMIT clause, may be null.
     * @return A LIMIT object
     */
    public Limit getLimit() {
        return limit;
    }

    /**
     * Get the derived column names.  Note this only reports alias names.
     * Any other names may not be consistent throughout the translation process.
     * @return a String[] containing the column names
     * @since 4.3
     */
    public String[] getColumnNames() {
        List<DerivedColumn> selectSymbols = getProjectedQuery().getDerivedColumns();
        String[] columnNames = new String[selectSymbols.size()];
        int symbolIndex = 0;
        for (DerivedColumn column : selectSymbols) {
            columnNames[symbolIndex++] = column.getAlias();
        }
        return columnNames;
    }

    /**
     * Get the column types of the output columns for this query
     * @return a Class[] containing the column names
     * @since 4.3
     */
    public Class<?>[] getColumnTypes() {
        List<DerivedColumn> selectSymbols = getProjectedQuery().getDerivedColumns();
        Class<?>[] columnTypes = new Class[selectSymbols.size()];
        int symbolIndex = 0;
        for (DerivedColumn column : selectSymbols) {
            columnTypes[symbolIndex++] = column.getExpression().getType();
        }
        return columnTypes;
    }

    /**
     * Set ORDER BY clause, may be null.
     * @param orderBy An ORDER BY object
     */
    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    /**
     * Set LIMIT clause, may be null.
     * @param limit A LIMIT object
     */
    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public With getWith() {
        return with;
    }

    public void setWith(With with) {
        this.with = with;
    }
}
