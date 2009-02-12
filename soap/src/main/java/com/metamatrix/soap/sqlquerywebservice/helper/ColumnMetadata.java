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
 * This class contains metamdata values for a given result set.
 */
public class ColumnMetadata {

	protected boolean autoIncrement;

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}

	protected boolean caseSensitive;

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public void setCaseSensitive(boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}

	protected java.lang.String columnClassName;

	public java.lang.String getColumnClassName() {
		return columnClassName;
	}

	public void setColumnClassName(java.lang.String columnClassName) {
		this.columnClassName = columnClassName;
	}

	protected java.lang.String columnDataType;

	public java.lang.String getColumnDataType() {
		return columnDataType;
	}

	public void setColumnDataType(java.lang.String columnDataType) {
		this.columnDataType = columnDataType;
	}

	protected int columnDisplaySize;

	public int getColumnDisplaySize() {
		return columnDisplaySize;
	}

	public void setColumnDisplaySize(int columnDisplaySize) {
		this.columnDisplaySize = columnDisplaySize;
	}

	protected java.lang.String columnName;

	public java.lang.String getColumnName() {
		return columnName;
	}

	public void setColumnName(java.lang.String columnName) {
		this.columnName = columnName;
	}

	protected boolean currency;

	public boolean isCurrency() {
		return currency;
	}

	public void setCurrency(boolean currency) {
		this.currency = currency;
	}

	protected java.lang.String getColumnLabel;

	public java.lang.String getGetColumnLabel() {
		return getColumnLabel;
	}

	public void setGetColumnLabel(java.lang.String getColumnLabel) {
		this.getColumnLabel = getColumnLabel;
	}

	protected boolean nullable;

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	protected int precision;

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	protected boolean readOnly;

	public boolean isReadOnly() {
		return readOnly;
	}

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	protected int scale;

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

	protected boolean searchable;

	public boolean isSearchable() {
		return searchable;
	}

	public void setSearchable(boolean searchable) {
		this.searchable = searchable;
	}

	protected boolean signed;

	public boolean isSigned() {
		return signed;
	}

	public void setSigned(boolean signed) {
		this.signed = signed;
	}

	protected java.lang.String tableName;

	public java.lang.String getTableName() {
		return tableName;
	}

	public void setTableName(java.lang.String tableName) {
		this.tableName = tableName;
	}

	protected java.lang.String virtualDatabaseName;

	public java.lang.String getVirtualDatabaseName() {
		return virtualDatabaseName;
	}

	public void setVirtualDatabaseName(java.lang.String virtualDatabaseName) {
		this.virtualDatabaseName = virtualDatabaseName;
	}

	protected java.lang.String virtualDatabaseVersion;

	public java.lang.String getVirtualDatabaseVersion() {
		return virtualDatabaseVersion;
	}

	public void setVirtualDatabaseVersion(
			java.lang.String virtualDatabaseVersion) {
		this.virtualDatabaseVersion = virtualDatabaseVersion;
	}

}

