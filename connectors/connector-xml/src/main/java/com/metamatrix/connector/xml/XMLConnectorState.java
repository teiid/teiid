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

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;

public interface XMLConnectorState extends BaseXMLConnectorState {

	public static final String STATE_CLASS_PROP = "ConnectorStateClass"; //$NON-NLS-1$

	/**
	 * @return Returns the m_cacheTimeout.
	 */
	public abstract int getCacheTimeoutSeconds();

	public abstract int getCacheTimeoutMillis();

	public abstract boolean isPreprocess();

	public abstract ConnectorCapabilities getConnectorCapabilities();

	public abstract int getMaxMemoryCacheSizeByte();

	public abstract int getMaxMemoryCacheSizeKB();

	public abstract int getMaxInMemoryStringSize();

	public abstract int getMaxFileCacheSizeKB();

	public abstract int getMaxFileCacheSizeByte();

	public abstract String getCacheLocation();

	public abstract boolean isLogRequestResponse();

	public abstract SAXFilterProvider getSAXFilterProvider();

	public abstract IQueryPreprocessor getPreprocessor();

	public abstract Connection getConnection(CachingConnector connector,
			ExecutionContext context, ConnectorEnvironment environment)
			throws ConnectorException;

	public String getPluggableInputStreamFilterClass();

	public boolean isCaching();

}