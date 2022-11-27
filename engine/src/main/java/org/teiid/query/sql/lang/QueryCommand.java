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

package org.teiid.query.sql.lang;

import java.util.List;



/**
 * This is a common super class for the two types of query commands: Query and SetQuery.
 * This class provides some useful commonalities when the type of query command
 * is not known.
 */
public abstract class QueryCommand extends Command {

    /** The order in which to sort the results */
    private OrderBy orderBy;

    /** Limit on returned rows */
    private Limit limit;

    private List<WithQueryCommand> with;

    /**
     * Get the order by clause for the query.
     * @return order by clause
     */
    public OrderBy getOrderBy() {
        return orderBy;
    }

    /**
     * Set the order by clause for the query.
     * @param orderBy New order by clause
     */
    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public List<WithQueryCommand> getWith() {
        return with;
    }

    public void setWith(List<WithQueryCommand> with) {
        this.with = with;
    }

    public abstract Query getProjectedQuery();

    @Override
    public boolean returnsResultSet() {
        return true;
    }
}
