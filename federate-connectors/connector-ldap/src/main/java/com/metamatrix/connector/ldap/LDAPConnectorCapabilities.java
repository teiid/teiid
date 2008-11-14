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
package com.metamatrix.connector.ldap;

import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.basic.BasicConnectorCapabilities;

/**
 * This class extends the BasicConnectorCapabilities class, and establishes
 * the capabilities that are supported by the LDAPConnector.  
 */
public class LDAPConnectorCapabilities extends BasicConnectorCapabilities {
	
	public void setInCriteriaSize(int maxInCriteriaSize) {
		this.maxInCriteriaSize = maxInCriteriaSize;
	}
	
	public int getCapabilitiesScope() {
		return ConnectorCapabilities.SCOPE.GLOBAL;
	}

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsExecutionMode(int)
     */
	public boolean supportsExecutionMode(int executionMode) {
		 if((executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY) || (executionMode == ConnectorCapabilities.EXECUTION_MODE.UPDATE)) {
	            return true;
	     }
	     return false;
	}

    public boolean supportsAndCriteria() {
		return true;
	}

	public boolean supportsCompareCriteria() {
		return true;
	}

	public boolean supportsCompareCriteriaEquals() {
		return true;
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

	public boolean supportsCriteria() {
		return true;
	}

	public boolean supportsInCriteria() {
		return true;
	}

	public boolean supportsLikeCriteria() {
		return true;
	}

	public boolean supportsOrCriteria() {
		return true;
	}

	public boolean supportsOrderBy() {
		// Removed this support -- see LDAPSyncQueryExecution comments for details.
		return false;
	}

	public boolean supportsRowLimit() {
		// GHH 20080408 - turned this on, because I fixed issue
		// in nextBatch that was causing this to fail
		return true;
	}

	public boolean supportsRowOffset() {
		// TODO This might actually be possible in future releases,
		// when using virtual list views/Sun. note that this requires the ability
		// to set the count limit, as well as an offset, so setCountLimit::searchControls
		// won't do it alone.
		return false;
	}

}
