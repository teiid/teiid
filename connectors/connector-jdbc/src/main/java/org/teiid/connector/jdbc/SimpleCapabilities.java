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

package org.teiid.connector.jdbc;

import java.util.List;


/**
 * This is a "simple" capabilities class that allows criteria but no 
 * complicated joins, subqueries, etc to be passed to the connector.
 * This capabilities class may come in handy for testing and for 
 * sources that support JDBC but don't support extended JDBC capabilities.  
 */
public class SimpleCapabilities extends JDBCCapabilities {

    public SimpleCapabilities() {
    }
    
    @Override
    public int getMaxInCriteriaSize() {
    	return 250;
    }
    
    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsCompareCriteriaEquals()
     */
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsInCriteria()
     */
    public boolean supportsInCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsIsNullCriteria()
     */
    public boolean supportsIsNullCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsLikeCriteria()
     */
    public boolean supportsLikeCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsNotCriteria()
     */
    public boolean supportsNotCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsOrCriteria()
     */
    public boolean supportsOrCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsSelectDistinct()
     */
    public boolean supportsSelectDistinct() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAliasedGroup()
     */
    public boolean supportsAliasedGroup() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsSelfJoins()
     */
    public boolean supportsSelfJoins() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsOuterJoins()
     */
    public boolean supportsOuterJoins() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsFullOuterJoins()
     */
    public boolean supportsFullOuterJoins() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsBetweenCriteria()
     */
    public boolean supportsBetweenCriteria() {
        return false;
    }


    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsLikeCriteriaEscapeCharacter()
     */
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsInCriteriaSubquery()
     */
    public boolean supportsInCriteriaSubquery() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsExistsCriteria()
     */
    public boolean supportsExistsCriteria() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsQuantifiedCompareCriteriaSome()
     */
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsQuantifiedCompareCriteriaAll()
     */
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsOrderBy()
     */
    public boolean supportsOrderBy() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAggregatesSum()
     */
    public boolean supportsAggregatesSum() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAggregatesAvg()
     */
    public boolean supportsAggregatesAvg() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAggregatesMin()
     */
    public boolean supportsAggregatesMin() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAggregatesMax()
     */
    public boolean supportsAggregatesMax() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAggregatesCount()
     */
    public boolean supportsAggregatesCount() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAggregatesCountStar()
     */
    public boolean supportsAggregatesCountStar() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsAggregatesDistinct()
     */
    public boolean supportsAggregatesDistinct() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsScalarSubqueries()
     */
    public boolean supportsScalarSubqueries() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsCorrelatedSubqueries()
     */
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsCaseExpressions()
     */
    public boolean supportsCaseExpressions() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsSearchedCaseExpressions()
     */
    public boolean supportsSearchedCaseExpressions() {
        return false;
    }

    /**
     * Return null to indicate no functions are supported.
     * @return null 
     * @see org.teiid.connector.api.ConnectorCapabilities#getSupportedFunctions()
     */
    public List getSupportedFunctions() {
        return null;
    }

    public boolean supportsInlineViews() {
        return false;
    }       

    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsUnions()
     * @since 4.2
     */
    public boolean supportsUnions() {
        return false;
    }
    
    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return false;
    }

}
