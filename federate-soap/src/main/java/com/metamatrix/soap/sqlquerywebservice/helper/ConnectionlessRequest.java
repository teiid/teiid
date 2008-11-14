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

package com.metamatrix.soap.sqlquerywebservice.helper;

/**
 * Class containing all required information to create a
 * connection and submit a SQL statement. This class is used 
 * for executeBlocking() in SQLQueryWebService. 
 */
public class ConnectionlessRequest {

	protected boolean includeMetadata;

	public boolean isIncludeMetadata() {
		return includeMetadata;
	}

	public void setIncludeMetadata(boolean includeMetadata) {
		this.includeMetadata = includeMetadata;
	}

	protected int maxRowsReturned;

	public int getMaxRowsReturned() {
		return maxRowsReturned;
	}

	public void setMaxRowsReturned(int maxRowsReturned) {
		this.maxRowsReturned = maxRowsReturned;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.LogInParameters parameters;

	public com.metamatrix.soap.sqlquerywebservice.helper.LogInParameters getParameters() {
		return parameters;
	}

	public void setParameters(
			com.metamatrix.soap.sqlquerywebservice.helper.LogInParameters parameters) {
		this.parameters = parameters;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo requestInfo;

	public com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo getRequestInfo() {
		return requestInfo;
	}

	public void setRequestInfo(
			com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo requestInfo) {
		this.requestInfo = requestInfo;
	}

	protected int timeToWait;

	public int getTimeToWait() {
		return timeToWait;
	}

	public void setTimeToWait(int timeToWait) {
		this.timeToWait = timeToWait;
	}

}