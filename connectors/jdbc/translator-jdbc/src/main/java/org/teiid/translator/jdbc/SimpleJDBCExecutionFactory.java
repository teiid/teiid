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

package org.teiid.translator.jdbc;

import java.util.List;

import org.teiid.translator.Translator;


/**
 * This is a "simple" capabilities class that allows criteria but no
 * complicated joins, subqueries, etc to be passed to the connector.
 * This capabilities class may come in handy for testing and for
 * sources that support JDBC but don't support extended JDBC capabilities.
 */
@Translator(name="jdbc-simple", description="An extended JDBC ANSI translator with some simple capabilities")
public class SimpleJDBCExecutionFactory extends JDBCExecutionFactory {

    public SimpleJDBCExecutionFactory() {
        setSupportsOuterJoins(false);
        setSupportsFullOuterJoins(false);
        setSupportsOrderBy(false);
        setMaxInCriteriaSize(250);
        setMaxDependentInPredicates(10);
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return true;
    }

    @Override
    public boolean supportsLikeCriteria() {
        return true;
    }

    @Override
    public boolean supportsNotCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }

    @Override
    public boolean supportsAliasedTable() {
        return false;
    }

    @Override
    public boolean supportsSelfJoins() {
        return false;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return false;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
        return false;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return false;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return false;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return false;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return false;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return false;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return false;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
        return false;
    }

    @Override
    public boolean supportsScalarSubqueries() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return false;
    }

    @Override
    public List getSupportedFunctions() {
        return null;
    }

    public boolean supportsInlineViews() {
        return false;
    }

    @Override
    public boolean supportsUnions() {
        return false;
    }

    @Override
    public boolean supportsInsertWithQueryExpression() {
        return false;
    }
}
