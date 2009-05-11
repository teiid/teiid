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
package com.metamatrix.connector.ldap;

import org.teiid.connector.basic.BasicConnectorCapabilities;

/**
 * This class extends the BasicConnectorCapabilities class, and establishes
 * the capabilities that are supported by the LDAPConnector.  
 */
public class LDAPConnectorCapabilities extends BasicConnectorCapabilities {
	
	@Override
	public int getMaxInCriteriaSize() {
		return 1000;
	}
	
    public boolean supportsCompareCriteriaEquals() {
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
	
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return true;
	}
	
	@Override
	public boolean supportsNotCriteria() {
		return true;
	}

}
