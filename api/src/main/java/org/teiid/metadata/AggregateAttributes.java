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

package org.teiid.metadata;

import java.io.Serializable;

/**
 * Holds metadata related to user defined aggregate functions.
 */
public class AggregateAttributes implements Serializable {

    private static final long serialVersionUID = 5398000844375944790L;

    private boolean analytic;
    private boolean usesDistinctRows;
    private boolean allowsOrderBy;
    private boolean allowsDistinct;
    private boolean decomposable;
    /**
     * @return true if the aggregate allows an order by clause
     */
    public boolean allowsOrderBy() {
        return allowsOrderBy;
    }

    public void setAllowsOrderBy(boolean allowsOrderBy) {
        this.allowsOrderBy = allowsOrderBy;
    }

    /**
     * @return true if the aggregate can only be used as a windowed function
     */
    public boolean isAnalytic() {
        return analytic;
    }

    public void setAnalytic(boolean analytic) {
        this.analytic = analytic;
    }

    /**
     *
     * @return True if the aggregate function specified without the
     * distinct keyword effectively uses only distinct rows.
     * For example min/max would return true
     * and avg would return false.
     */
    public boolean usesDistinctRows() {
        return usesDistinctRows;
    }

    public void setUsesDistinctRows(boolean usesDistinctRows) {
        this.usesDistinctRows = usesDistinctRows;
    }

    /**
     * @return true if the aggregate function may be decomposed as
     * agg(agg(x)) for non-partitioned aggregate pushdown.
     * This is only meaningful for single argument aggregate
     * functions.
     */
    public boolean isDecomposable() {
        return decomposable;
    }

    public void setDecomposable(boolean decomposable) {
        this.decomposable = decomposable;
    }

    /**
     * @return true if the aggregate function can use the DISTINCT keyword
     */
    public boolean allowsDistinct() {
        return allowsDistinct;
    }

    public void setAllowsDistinct(boolean allowsDistinct) {
        this.allowsDistinct = allowsDistinct;
    }

}
