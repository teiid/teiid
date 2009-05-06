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

package com.metamatrix.connector.loopback;

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.basic.BasicConnectorCapabilities;


/**
 * Specifies the capabilities of this connector.  Since we want this 
 * connector to be able to emulate most other connectors, these 
 * capabilities support everything.
 */
public class LoopbackCapabilities extends BasicConnectorCapabilities {

    /**
     * Construct the capabilities class 
     */
    public LoopbackCapabilities() {
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#getSupportedFunctions()
     */
    public List getSupportedFunctions() {
        List functions = Arrays.asList(new String[] {
            "+", "-", "*", "/", "abs", "acos", "asin", "atan", "atan2", "ceiling", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$
            "bitand", "bitnot", "bitor", "bitxor", "cos", "cot", "degrees", "cos", "cot", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "degrees", "exp", "floor", "log", "log10", "mod", "pi", "power", "radians",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "round", "sign", "sin", "sqrt", "tan",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            "ascii", "chr", "char", "concat", "initcap", "insert", "lcase", "left", "length", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "locate", "lower", "lpad", "ltrim", "repeat", "replace", "right", "rpad", "rtrim", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
            "substring", "translate", "ucase", "upper",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            "curdate", "curtime", "now", "dayname", "dayofmonth", "dayofweek", "dayofyear",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
            "hour", "minute", "month", "monthname", "quarter", "second", "timestampadd",  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
            "timestampdiff", "week", "year", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            "cast", "convert", "ifnull", "nvl"  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        });
        return functions;
    }
    
    @Override
    public boolean supportsGroupBy() {
    	return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAggregatesAvg()
     */
    public boolean supportsAggregatesAvg() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAggregatesCount()
     */
    public boolean supportsAggregatesCount() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAggregatesCountStar()
     */
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAggregatesDistinct()
     */
    public boolean supportsAggregatesDistinct() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAggregatesMax()
     */
    public boolean supportsAggregatesMax() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAggregatesMin()
     */
    public boolean supportsAggregatesMin() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAggregatesSum()
     */
    public boolean supportsAggregatesSum() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsAliasedGroup()
     */
    public boolean supportsAliasedGroup() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsBetweenCriteria()
     */
    public boolean supportsBetweenCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsCaseExpressions()
     */
    public boolean supportsCaseExpressions() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsCompareCriteriaEquals()
     */
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsCorrelatedSubqueries()
     */
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsExistsCriteria()
     */
    public boolean supportsExistsCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsFullOuterJoins()
     */
    public boolean supportsFullOuterJoins() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsInCriteria()
     */
    public boolean supportsInCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsInCriteriaSubquery()
     */
    public boolean supportsInCriteriaSubquery() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsIsNullCriteria()
     */
    public boolean supportsIsNullCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsLikeCriteria()
     */
    public boolean supportsLikeCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsLikeCriteriaEscapeCharacter()
     */
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsNotCriteria()
     */
    public boolean supportsNotCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsOrCriteria()
     */
    public boolean supportsOrCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsOrderBy()
     */
    public boolean supportsOrderBy() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsOuterJoins()
     */
    public boolean supportsOuterJoins() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsQuantifiedCompareCriteriaAll()
     */
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsScalarSubqueries()
     */
    public boolean supportsScalarSubqueries() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsSearchedCaseExpressions()
     */
    public boolean supportsSearchedCaseExpressions() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsSelectDistinct()
     */
    public boolean supportsSelectDistinct() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsSelfJoins()
     */
    public boolean supportsSelfJoins() {
        return true;
    }

    public boolean supportsInlineViews() {
        return true;
    }
        
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return true;
    }

    public boolean supportsRowLimit() {
        return true;
    }
    
    @Override
    public boolean supportsSelectExpression() {
    	return true;
    }
        
    @Override
    public boolean supportsSetQueryOrderBy() {
    	return true;
    }
    
    @Override
    public boolean supportsUnions() {
    	return true;
    }
    
    @Override
    public boolean supportsCompareCriteriaOrdered() {
    	return true;
    }
    
    @Override
    public boolean supportsInnerJoins() {
    	return true;
    }
    
    @Override
    public boolean supportsExcept() {
    	return true;
    }
    
    @Override
    public boolean supportsHaving() {
    	return true;
    }
    
    @Override
    public boolean supportsIntersect() {
    	return true;
    }
    
}
