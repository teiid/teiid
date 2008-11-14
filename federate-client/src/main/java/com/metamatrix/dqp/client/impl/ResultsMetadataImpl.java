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

package com.metamatrix.dqp.client.impl;

import java.util.Map;

import com.metamatrix.common.types.MMJDBCSQLTypeInfo;
import com.metamatrix.dqp.client.ResultsMetadata;
import com.metamatrix.dqp.metadata.ResultsMetadataConstants;


/** 
 * @since 4.2
 */
class ResultsMetadataImpl implements ResultsMetadata {
    
    private Map[] columnMetadata;
    
    ResultsMetadataImpl(Map[] columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getColumnCount()
     * @since 4.2
     */
    public int getColumnCount() {
        return columnMetadata.length;
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getVirtualDatabaseName(int)
     * @since 4.2
     */
    public String getVirtualDatabaseName(int index) {
        return getString(index, ResultsMetadataConstants.VIRTUAL_DATABASE_NAME);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getVirtualDatabaseVersion(int)
     * @since 4.2
     */
    public String getVirtualDatabaseVersion(int index) {
        return getString(index, ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getTableName(int)
     * @since 4.2
     */
    public String getTableName(int index) {
        return getString(index, ResultsMetadataConstants.GROUP_NAME);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getColumnName(int)
     * @since 4.2
     */
    public String getColumnName(int index) {
        return getString(index, ResultsMetadataConstants.ELEMENT_NAME);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getColumnTypeName(int)
     * @since 4.2
     */
    public String getColumnTypeName(int index) {
        return getString(index, ResultsMetadataConstants.DATA_TYPE);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#isAutoIncrement(int)
     * @since 4.2
     */
    public boolean isAutoIncrement(int index) {
        return getBoolean(index, ResultsMetadataConstants.AUTO_INCREMENTING);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#isCaseSensitive(int)
     * @since 4.2
     */
    public boolean isCaseSensitive(int index) {
        return getBoolean(index, ResultsMetadataConstants.CASE_SENSITIVE);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#isSearchable(int)
     * @since 4.2
     */
    public boolean isSearchable(int index) {
        int searchability = getInt(index, ResultsMetadataConstants.SEARCHABLE);
        return searchability != ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE.intValue();
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#isCurrency(int)
     * @since 4.2
     */
    public boolean isCurrency(int index) {
        return getBoolean(index, ResultsMetadataConstants.CURRENCY);
    }
    
    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#isNullable(int)
     * @since 4.2
     */
    public boolean isNullable(int index) {
        int nullability = getInt(index, ResultsMetadataConstants.NULLABLE);
        return nullability != ResultsMetadataConstants.NULL_TYPES.NOT_NULL.intValue();
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#isSigned(int)
     * @since 4.2
     */
    public boolean isSigned(int index) {
        return getBoolean(index, ResultsMetadataConstants.SIGNED);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getColumnDisplaySize(int)
     * @since 4.2
     */
    public int getColumnDisplaySize(int index) {
        return getInt(index, ResultsMetadataConstants.DISPLAY_SIZE);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getColumnLabel(int)
     * @since 4.2
     */
    public String getColumnLabel(int index) {
        return getString(index, ResultsMetadataConstants.ELEMENT_LABEL);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getPrecision(int)
     * @since 4.2
     */
    public int getPrecision(int index) {
        return getInt(index, ResultsMetadataConstants.PRECISION);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getScale(int)
     * @since 4.2
     */
    public int getScale(int index) {
        return getInt(index, ResultsMetadataConstants.SCALE);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#isReadOnly(int)
     * @since 4.2
     */
    public boolean isReadOnly(int index) {
        return !getBoolean(index, ResultsMetadataConstants.WRITABLE);
    }

    /** 
     * @see com.metamatrix.dqp.client.ResultsMetadata#getColumnClassName(int)
     * @since 4.2
     */
    public String getColumnClassName(int index) {
        return MMJDBCSQLTypeInfo.getJavaClassName(MMJDBCSQLTypeInfo.getSQLType(getColumnTypeName(index)));
    }
    
    private String getString(int index, Object key) {
        return (String)columnMetadata[index-1].get(key);
    }
    private boolean getBoolean(int index, Object key) {
        return ((Boolean)columnMetadata[index-1].get(key)).booleanValue();
    }
    private int getInt(int index, Object key) {
        return ((Number)columnMetadata[index-1].get(key)).intValue();
    }
    
}
