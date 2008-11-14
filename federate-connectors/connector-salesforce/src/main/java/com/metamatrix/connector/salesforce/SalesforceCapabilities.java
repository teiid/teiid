/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package com.metamatrix.connector.salesforce;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.basic.BasicConnectorCapabilities;

public class SalesforceCapabilities extends BasicConnectorCapabilities {
	
    public int getMaxInCriteriaSize() {
    	return maxInCriteriaSize;
    }
    
    public void setMaxInCriteriaSize(int size) {
    	maxInCriteriaSize = size;
    }
    
    public int getCapabilitiesScope() {
        return ConnectorCapabilities.SCOPE.GLOBAL;
    }

    
    public boolean supportsExecutionMode(int executionMode) {
        return (executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY);        
    }

   
    public List getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.add("includes");
        supportedFunctions.add("excludes");
    	return supportedFunctions;
    }

   
    @Override
	public boolean supportsScalarFunctions() {
		return true;
	}

	public boolean supportsCompareCriteria() {
        return true;
    }

    
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

   
    public boolean supportsCriteria() {
        return true;
    }

   
    public boolean supportsAndCriteria() {
        return true;
    }


    public boolean supportsInCriteria() {
        return true;
    }


	public boolean supportsLikeCriteria() {
		return true;
	}

	public boolean supportsRowLimit() {
		return true;
	}

	// http://jira.jboss.org/jira/browse/JBEDSP-306
	// Salesforce supports ORDER BY, but not on all column types
	public boolean supportsOrderBy() {
		return false;
	}

	public boolean supportsCompareCriteriaGreaterThan() {
		return true;
	}

	public boolean supportsCompareCriteriaGreaterThanOrEqual() {
		return true;
	}

	public boolean supportsCompareCriteriaLessThan() {
		return true;
	}

	public boolean supportsCompareCriteriaLessThanOrEqual() {
		return true;
	}

	public boolean supportsCompareCriteriaNotEquals() {
		return true;
	}
}
