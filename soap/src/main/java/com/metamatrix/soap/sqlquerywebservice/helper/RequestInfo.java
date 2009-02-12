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

package com.metamatrix.soap.sqlquerywebservice.helper;

/**
 * Request information used for a specific query.
 */
public class RequestInfo {

	protected java.lang.String[] bindParameters;

	public java.lang.String[] getBindParameters() {
		return bindParameters;
	}

	public void setBindParameters(java.lang.String[] bindParameters) {
		this.bindParameters = bindParameters;
	}

	protected java.lang.String commandPayload;

	public java.lang.String getCommandPayload() {
		return commandPayload;
	}

	public void setCommandPayload(java.lang.String commandPayload) {
		this.commandPayload = commandPayload;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.CursorType cursorType;

	public com.metamatrix.soap.sqlquerywebservice.helper.CursorType getCursorType() {
		return cursorType;
	}

	public void setCursorType(
			com.metamatrix.soap.sqlquerywebservice.helper.CursorType cursorType) {
		this.cursorType = cursorType;
	}

	protected int fetchSize;

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	protected boolean partialResults;

	public boolean isPartialResults() {
		return partialResults;
	}

	public void setPartialResults(boolean partialResults) {
		this.partialResults = partialResults;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.RequestType requestType;

	public com.metamatrix.soap.sqlquerywebservice.helper.RequestType getRequestType() {
		return requestType;
	}

	public void setRequestType(
			com.metamatrix.soap.sqlquerywebservice.helper.RequestType requestType) {
		this.requestType = requestType;
	}

	protected java.lang.String sqlString;

	public java.lang.String getSqlString() {
		return sqlString;
	}

	public void setSqlString(java.lang.String sqlString) {
		this.sqlString = sqlString;
	}

	protected com.metamatrix.soap.sqlquerywebservice.helper.TransactionAutoWrapType transactionAutoWrapMode;

	public com.metamatrix.soap.sqlquerywebservice.helper.TransactionAutoWrapType getTransactionAutoWrapMode() {
		return transactionAutoWrapMode;
	}

	public void setTransactionAutoWrapMode(
			com.metamatrix.soap.sqlquerywebservice.helper.TransactionAutoWrapType transactionAutoWrapMode) {
		this.transactionAutoWrapMode = transactionAutoWrapMode;
	}

	protected boolean useResultSetCache;

	public boolean isUseResultSetCache() {
		return useResultSetCache;
	}

	public void setUseResultSetCache(boolean useResultSetCache) {
		this.useResultSetCache = useResultSetCache;
	}

	protected java.lang.String xmlFormat;

	public java.lang.String getXmlFormat() {
		return xmlFormat;
	}

	public void setXmlFormat(java.lang.String xmlFormat) {
		this.xmlFormat = xmlFormat;
	}

	protected java.lang.String xmlStyleSheet;

	public java.lang.String getXmlStyleSheet() {
		return xmlStyleSheet;
	}

	public void setXmlStyleSheet(java.lang.String xmlStyleSheet) {
		this.xmlStyleSheet = xmlStyleSheet;
	}

	protected boolean xmlValidationMode;

	public boolean isXmlValidationMode() {
		return xmlValidationMode;
	}

	public void setXmlValidationMode(boolean xmlValidationMode) {
		this.xmlValidationMode = xmlValidationMode;
	}

}


