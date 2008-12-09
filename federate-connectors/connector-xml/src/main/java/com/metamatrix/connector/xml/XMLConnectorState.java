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


package com.metamatrix.connector.xml;

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;

public interface XMLConnectorState extends BaseXMLConnectorState {

	public static final String STATE_CLASS_PROP = "ConnectorStateClass"; //$NON-NLS-1$

	public DocumentProducer makeExecutor(XMLExecution info)
			throws ConnectorException;

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
			SecurityContext context, ConnectorEnvironment environment)
			throws ConnectorException;

	public String getPluggableInputStreamFilterClass();

}