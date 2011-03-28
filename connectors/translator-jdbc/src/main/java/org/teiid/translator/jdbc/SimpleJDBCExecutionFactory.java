/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
    public boolean supportsBetweenCriteria() {
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
    public boolean supportsCaseExpressions() {
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
