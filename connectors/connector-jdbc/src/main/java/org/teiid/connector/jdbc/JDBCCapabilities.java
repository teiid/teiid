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

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.basic.BasicConnectorCapabilities;


/**
 */
public class JDBCCapabilities extends BasicConnectorCapabilities {
    
    /**
     * 
     */
    public JDBCCapabilities() {
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#getSupportedFunctions()
     */
    public List<String> getSupportedFunctions() {
        return Arrays.asList(new String[] { "+", "-", "*", "/" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    /** 
     * @see org.teiid.connector.basic.BasicConnectorCapabilities#getMaxInCriteriaSize()
     * @since 4.2
     */
    public int getMaxInCriteriaSize() {
        return 1000;
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
    
    @Override
    public boolean supportsOrderByUnrelated() {
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

    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsInlineViews()
     */
    public boolean supportsInlineViews() {
        return false;
    }       
    
    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsQuantifiedCompareCriteriaSome()
     */
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return true;
    }
    
    /** 
     * @see org.teiid.connector.basic.BasicConnectorCapabilities#supportsSetQueryOrderBy()
     */
    @Override
    public boolean supportsSetQueryOrderBy() {
        return true;
    }
    
    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsUnions()
     */
    public boolean supportsUnions() {
        return true;
    }
    
    @Override
    public boolean supportsBulkUpdate() {
    	return true;
    }
    
    @Override
    public boolean supportsBatchedUpdates() {
    	return true;
    }
    
    @Override
    public boolean supportsCompareCriteriaOrdered() {
    	return true;
    }
    
    @Override
    public SupportedJoinCriteria getSupportedJoinCriteria() {
    	return SupportedJoinCriteria.ANY;
    }
    
    @Override
    public boolean supportsHaving() {
    	return true;
    }
    
    @Override
    public boolean supportsInnerJoins() {
    	return true;
    }
    
    @Override
    public boolean supportsSelectExpression() {
    	return true;
    }
    
    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return true;
    }

}
