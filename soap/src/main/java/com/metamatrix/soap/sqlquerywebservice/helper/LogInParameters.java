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
 * Url values for a MetaMatrix VDB connection.
 */
public class LogInParameters {

	protected java.lang.String connectionPayload;

	public java.lang.String getConnectionPayload() {
		return connectionPayload;
	}

	public void setConnectionPayload(java.lang.String connectionPayload) {
		this.connectionPayload = connectionPayload;
	}

	protected java.lang.String mmServerUrl;

	public java.lang.String getMmServerUrl() {
		return mmServerUrl;
	}

	public void setMmServerUrl(java.lang.String mmServerUrl) {
		this.mmServerUrl = mmServerUrl;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.Property[] optionalProperties;

	public com.metamatrix.soap.sqlquerywebservice.helper.Property[] getOptionalProperties() {
		return optionalProperties;
	}

	public void setOptionalProperties(
			com.metamatrix.soap.sqlquerywebservice.helper.Property[] optionalProperties) {
		this.optionalProperties = optionalProperties;
	}

	protected java.lang.String vdbName;

	public java.lang.String getVdbName() {
		return vdbName;
	}

	public void setVdbName(java.lang.String vdbName) {
		this.vdbName = vdbName;
	}

	protected java.lang.String vdbVersion;

	public java.lang.String getVdbVersion() {
		return vdbVersion;
	}

	public void setVdbVersion(java.lang.String vdbVersion) {
		this.vdbVersion = vdbVersion;
	}

}


