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
 * Container for returned results values.
 */
public class Results {

	protected boolean hasData;

	public boolean isHasData() {
		return hasData;
	}

	public void setHasData(boolean hasData) {
		this.hasData = hasData;
	}

	protected java.lang.String[] outputParameters = new java.lang.String[0];

	public java.lang.String[] getOutputParameters() {
		return outputParameters;
	}

	public void setOutputParameters(java.lang.String[] outputParameters) {
		this.outputParameters = outputParameters;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.SqlWarning[] sqlWarnings;

	public com.metamatrix.soap.sqlquerywebservice.helper.SqlWarning[] getSqlWarnings() {
		return sqlWarnings;
	}

	public void setSqlWarnings(
			com.metamatrix.soap.sqlquerywebservice.helper.SqlWarning[] sqlWarnings) {
		this.sqlWarnings = sqlWarnings;
	}

	protected java.lang.Integer beginRow;

	public java.lang.Integer getBeginRow() {
		return beginRow;
	}

	public void setBeginRow(java.lang.Integer beginRow) {
		this.beginRow = beginRow;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.Data data;

	public com.metamatrix.soap.sqlquerywebservice.helper.Data getData() {
		return data;
	}

	public void setData(com.metamatrix.soap.sqlquerywebservice.helper.Data data) {
		this.data = data;
	}

	protected java.lang.Integer endRow;

	public java.lang.Integer getEndRow() {
		return endRow;
	}

	public void setEndRow(java.lang.Integer endRow) {
		this.endRow = endRow;
	}

	protected java.lang.Integer updateCount;

	public java.lang.Integer getUpdateCount() {
		return updateCount;
	}

	public void setUpdateCount(java.lang.Integer updateCount) {
		this.updateCount = updateCount;
	}

}


