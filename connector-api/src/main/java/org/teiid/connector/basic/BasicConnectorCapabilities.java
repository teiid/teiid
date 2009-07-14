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

package org.teiid.connector.basic;

import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;


/**
 * This class is a base implementation of the ConnectorCapabilities interface.
 * It is implemented to return false for all capabilities.  Subclass this base
 * class and override any methods necessary to specify capabilities the
 * connector actually supports.  
 */
public class BasicConnectorCapabilities implements ConnectorCapabilities {
    
    /**
     * Construct the basic capabilities class.
     */
    public BasicConnectorCapabilities() {
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsSelectDistinct()
     */
    public boolean supportsSelectDistinct() {
        return false;
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
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsCompareCriteriaEquals()
     */
    public boolean supportsCompareCriteriaEquals() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsLikeCriteria()
     */
    public boolean supportsLikeCriteria() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsLikeCriteriaEscapeCharacter()
     */
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsInCriteria()
     */
    public boolean supportsInCriteria() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsInCriteriaSubquery()
     */
    public boolean supportsInCriteriaSubquery() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsIsNullCriteria()
     */
    public boolean supportsIsNullCriteria() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsOrCriteria()
     */
    public boolean supportsOrCriteria() {
        return false;
    }

    /* 
     * @see com.metamatrix.data.api.ConnectorCapabilities#supportsNotCriteria()
     */
    public boolean supportsNotCriteria() {
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
    public List<String> getSupportedFunctions() {
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

    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#getMaxInCriteriaSize()
     * @since 4.2
     */
    public int getMaxInCriteriaSize() {
        return -1;
    }
    
    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsFunctionsInGroupBy()
     * @since 5.0
     */
    public boolean supportsFunctionsInGroupBy() {
        return false;
    }

    public boolean supportsRowLimit() {
        return false;
    }

    public boolean supportsRowOffset() {
        return false;
    }

    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#getMaxFromGroups()
     */
    public int getMaxFromGroups() {
        return -1; //-1 indicates no max
    }

    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsExcept()
     */
    public boolean supportsExcept() {
        return false;
    }

    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsIntersect()
     */
    public boolean supportsIntersect() {
        return false;
    }

    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsSetQueryOrderBy()
     */
    public boolean supportsSetQueryOrderBy() {
        return false;
    }    
    
    @Override
    public boolean useAnsiJoin() {
    	return false;
    }

    @Override
    public boolean requiresCriteria() {
    	return false;
    }

	@Override
	public boolean supportsBatchedUpdates() {
		return false;
	}

	@Override
	public boolean supportsGroupBy() {
		return false;
	}

	@Override
	public boolean supportsHaving() {
		return false;
	}

	@Override
	public boolean supportsInnerJoins() {
		return false;
	}

	@Override
	public boolean supportsSelectExpression() {
		return false;
	}
	
	@Override
	public SupportedJoinCriteria getSupportedJoinCriteria() {
		return SupportedJoinCriteria.ANY;
	}
	
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return false;
	}

	@Override
	public boolean supportsInsertWithQueryExpression() {
		return false;
	}
	
	@Override
	public boolean supportsBulkUpdate() {
		return false;
	}
	
	@Override
	public boolean supportsOrderByUnrelated() {
		return false;
	}
	
}
