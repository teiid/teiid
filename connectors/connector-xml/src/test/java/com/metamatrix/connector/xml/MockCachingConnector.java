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


package com.metamatrix.connector.xml;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.xml.cache.IDocumentCache;
import com.metamatrix.connector.xml.cache.MockDocumentCache;

public class MockCachingConnector implements CachingConnector {

	IDocumentCache mockCache = new MockDocumentCache();
	
	public void createCacheObjectRecord(String requestID, String partID, String executionID, String sourceRequestID, String cacheKey) throws ConnectorException {
		// TODO Auto-generated method stub
	}

	public void deleteCacheItems(String requestIdentifier, String partIdentifier, String executionIdentifier) {
		// TODO Auto-generated method stub
	}

	public IDocumentCache getCache() {
		return mockCache;
	}

	public IDocumentCache getStatementCache() {
		// TODO Auto-generated method stub
		return null;
	}

	public ConnectorEnvironment getEnvironment() {
		// TODO Auto-generated method stub
		return null;
	}

	public ConnectorLogger getLogger() {
		// TODO Auto-generated method stub
		return null;
	}

	public XMLConnectorState getState() {
		// TODO Auto-generated method stub
		return null;
	}

	public Connection getConnection(ExecutionContext arg0) throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	public void stop() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public ConnectorCapabilities getCapabilities() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void start(ConnectorEnvironment environment)
			throws ConnectorException {
		// TODO Auto-generated method stub
		
	}


}
