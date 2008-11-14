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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.dqp.metadata.ResultsMetadataConstants;


/** 
 * @since 4.3
 */
public class TestResultsMetadataImpl extends TestCase {

    public void testConstructor_MetaDataMessage() {
        Map[] metadata = new Map[4];
        
        metadata[0] = new HashMap();
        metadata[0].put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.FALSE);
        metadata[0].put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.FALSE);
        metadata[0].put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        metadata[0].put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.DefaultDataTypes.INTEGER);
        metadata[0].put(ResultsMetadataConstants.DISPLAY_SIZE, new Integer(11));
        metadata[0].put(ResultsMetadataConstants.ELEMENT_LABEL, "Element1Label"); //$NON-NLS-1$
        metadata[0].put(ResultsMetadataConstants.ELEMENT_NAME, "Element1"); //$NON-NLS-1$
        metadata[0].put(ResultsMetadataConstants.GROUP_NAME, "Group1"); //$NON-NLS-1$
        metadata[0].put(ResultsMetadataConstants.NULLABLE, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadata[0].put(ResultsMetadataConstants.PRECISION, new Integer(11));
        metadata[0].put(ResultsMetadataConstants.RADIX, new Integer(10));
        metadata[0].put(ResultsMetadataConstants.SCALE, new Integer(0));
        metadata[0].put(ResultsMetadataConstants.SEARCHABLE, ResultsMetadataConstants.SEARCH_TYPES.ALLEXCEPTLIKE);
        metadata[0].put(ResultsMetadataConstants.SIGNED, Boolean.FALSE);
        metadata[0].put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, "VDB"); //$NON-NLS-1$
        metadata[0].put(ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, "3"); //$NON-NLS-1$
        metadata[0].put(ResultsMetadataConstants.WRITABLE, Boolean.FALSE);

        metadata[1] = new HashMap();
        metadata[1].put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.TRUE);
        metadata[1].put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.TRUE);
        metadata[1].put(ResultsMetadataConstants.CURRENCY, Boolean.TRUE);
        metadata[1].put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.DefaultDataTypes.LONG);
        metadata[1].put(ResultsMetadataConstants.DISPLAY_SIZE, new Integer(19));
        metadata[1].put(ResultsMetadataConstants.ELEMENT_LABEL, "Element2Label"); //$NON-NLS-1$
        metadata[1].put(ResultsMetadataConstants.ELEMENT_NAME, "Element2"); //$NON-NLS-1$
        metadata[1].put(ResultsMetadataConstants.GROUP_NAME, "Group1"); //$NON-NLS-1$
        metadata[1].put(ResultsMetadataConstants.NULLABLE, ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        metadata[1].put(ResultsMetadataConstants.PRECISION, new Integer(19));
        metadata[1].put(ResultsMetadataConstants.RADIX, new Integer(16));
        metadata[1].put(ResultsMetadataConstants.SCALE, new Integer(0));
        metadata[1].put(ResultsMetadataConstants.SEARCHABLE, ResultsMetadataConstants.SEARCH_TYPES.LIKE_ONLY);
        metadata[1].put(ResultsMetadataConstants.SIGNED, Boolean.TRUE);
        metadata[1].put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, "VDB"); //$NON-NLS-1$
        metadata[1].put(ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, "3"); //$NON-NLS-1$
        metadata[1].put(ResultsMetadataConstants.WRITABLE, Boolean.TRUE);

        metadata[2] = new HashMap();
        metadata[2].put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.FALSE);
        metadata[2].put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.FALSE);
        metadata[2].put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        metadata[2].put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.DefaultDataTypes.FLOAT);
        metadata[2].put(ResultsMetadataConstants.DISPLAY_SIZE, new Integer(11));
        metadata[2].put(ResultsMetadataConstants.ELEMENT_LABEL, "Element3Label"); //$NON-NLS-1$
        metadata[2].put(ResultsMetadataConstants.ELEMENT_NAME, "Element3"); //$NON-NLS-1$
        metadata[2].put(ResultsMetadataConstants.GROUP_NAME, "Group1"); //$NON-NLS-1$
        metadata[2].put(ResultsMetadataConstants.NULLABLE, ResultsMetadataConstants.NULL_TYPES.UNKNOWN);
        metadata[2].put(ResultsMetadataConstants.PRECISION, new Integer(15));
        metadata[2].put(ResultsMetadataConstants.RADIX, new Integer(10));
        metadata[2].put(ResultsMetadataConstants.SCALE, new Integer(10));
        metadata[2].put(ResultsMetadataConstants.SEARCHABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE);
        metadata[2].put(ResultsMetadataConstants.SIGNED, Boolean.FALSE);
        metadata[2].put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, "VDB"); //$NON-NLS-1$
        metadata[2].put(ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, "3"); //$NON-NLS-1$
        metadata[2].put(ResultsMetadataConstants.WRITABLE, Boolean.FALSE);
        
        metadata[3] = new HashMap();
        metadata[3].put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.FALSE);
        metadata[3].put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.FALSE);
        metadata[3].put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        metadata[3].put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.DefaultDataTypes.DOUBLE);
        metadata[3].put(ResultsMetadataConstants.DISPLAY_SIZE, new Integer(21));
        metadata[3].put(ResultsMetadataConstants.ELEMENT_LABEL, "Element4Label"); //$NON-NLS-1$
        metadata[3].put(ResultsMetadataConstants.ELEMENT_NAME, "Element4"); //$NON-NLS-1$
        metadata[3].put(ResultsMetadataConstants.GROUP_NAME, "Group1"); //$NON-NLS-1$
        metadata[3].put(ResultsMetadataConstants.NULLABLE, ResultsMetadataConstants.NULL_TYPES.NOT_NULL);
        metadata[3].put(ResultsMetadataConstants.PRECISION, new Integer(10));
        metadata[3].put(ResultsMetadataConstants.RADIX, new Integer(10));
        metadata[3].put(ResultsMetadataConstants.SCALE, new Integer(16));
        metadata[3].put(ResultsMetadataConstants.SEARCHABLE, ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE);
        metadata[3].put(ResultsMetadataConstants.SIGNED, Boolean.FALSE);
        metadata[3].put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, "VDB"); //$NON-NLS-1$
        metadata[3].put(ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, "3"); //$NON-NLS-1$
        metadata[3].put(ResultsMetadataConstants.WRITABLE, Boolean.FALSE);

        ResultsMetadataImpl rm = new ResultsMetadataImpl(metadata);
        
        assertEquals(4, rm.getColumnCount());
        
        assertEquals("java.lang.Integer", rm.getColumnClassName(1)); //$NON-NLS-1$
        assertEquals(11, rm.getColumnDisplaySize(1));
        assertEquals("Element1Label", rm.getColumnLabel(1)); //$NON-NLS-1$
        assertEquals("Element1", rm.getColumnName(1)); //$NON-NLS-1$
        assertEquals("integer", rm.getColumnTypeName(1)); //$NON-NLS-1$
        assertEquals(11, rm.getPrecision(1));
        assertEquals(0, rm.getScale(1));
        assertEquals("Group1", rm.getTableName(1)); //$NON-NLS-1$
        assertEquals("VDB", rm.getVirtualDatabaseName(1)); //$NON-NLS-1$
        assertEquals("3", rm.getVirtualDatabaseVersion(1)); //$NON-NLS-1$
        assertEquals(false, rm.isAutoIncrement(1));
        assertEquals(false, rm.isCaseSensitive(1));
        assertEquals(false, rm.isCurrency(1));
        assertEquals(false, rm.isNullable(1));
        assertEquals(true, rm.isReadOnly(1));
        assertEquals(true, rm.isSearchable(1));
        assertEquals(false, rm.isSigned(1));
        
        assertEquals("java.lang.Long", rm.getColumnClassName(2)); //$NON-NLS-1$
        assertEquals(19, rm.getColumnDisplaySize(2));
        assertEquals("Element2Label", rm.getColumnLabel(2)); //$NON-NLS-1$
        assertEquals("Element2", rm.getColumnName(2)); //$NON-NLS-1$
        assertEquals("long", rm.getColumnTypeName(2)); //$NON-NLS-1$
        assertEquals(19, rm.getPrecision(2));
        assertEquals(0, rm.getScale(2));
        assertEquals("Group1", rm.getTableName(2)); //$NON-NLS-1$
        assertEquals("VDB", rm.getVirtualDatabaseName(2)); //$NON-NLS-1$
        assertEquals("3", rm.getVirtualDatabaseVersion(2)); //$NON-NLS-1$
        assertEquals(true, rm.isAutoIncrement(2));
        assertEquals(true, rm.isCaseSensitive(2));
        assertEquals(true, rm.isCurrency(2));
        assertEquals(true, rm.isNullable(2));
        assertEquals(false, rm.isReadOnly(2));
        assertEquals(true, rm.isSearchable(2));
        assertEquals(true, rm.isSigned(2));

        assertEquals("java.lang.Float", rm.getColumnClassName(3)); //$NON-NLS-1$
        assertEquals(11, rm.getColumnDisplaySize(3));
        assertEquals("Element3Label", rm.getColumnLabel(3)); //$NON-NLS-1$
        assertEquals("Element3", rm.getColumnName(3)); //$NON-NLS-1$
        assertEquals("float", rm.getColumnTypeName(3)); //$NON-NLS-1$
        assertEquals(15, rm.getPrecision(3));
        assertEquals(10, rm.getScale(3));
        assertEquals("Group1", rm.getTableName(3)); //$NON-NLS-1$
        assertEquals("VDB", rm.getVirtualDatabaseName(3)); //$NON-NLS-1$
        assertEquals("3", rm.getVirtualDatabaseVersion(3)); //$NON-NLS-1$
        assertEquals(false, rm.isAutoIncrement(3));
        assertEquals(false, rm.isCaseSensitive(3));
        assertEquals(false, rm.isCurrency(3));
        assertEquals(true, rm.isNullable(3));
        assertEquals(true, rm.isReadOnly(3));
        assertEquals(true, rm.isSearchable(3));
        assertEquals(false, rm.isSigned(3));

        assertEquals("java.lang.Double", rm.getColumnClassName(4)); //$NON-NLS-1$
        assertEquals(21, rm.getColumnDisplaySize(4));
        assertEquals("Element4Label", rm.getColumnLabel(4)); //$NON-NLS-1$
        assertEquals("Element4", rm.getColumnName(4)); //$NON-NLS-1$
        assertEquals("double", rm.getColumnTypeName(4)); //$NON-NLS-1$
        assertEquals(10, rm.getPrecision(4));
        assertEquals(16, rm.getScale(4));
        assertEquals("Group1", rm.getTableName(4)); //$NON-NLS-1$
        assertEquals("VDB", rm.getVirtualDatabaseName(4)); //$NON-NLS-1$
        assertEquals("3", rm.getVirtualDatabaseVersion(4)); //$NON-NLS-1$
        assertEquals(false, rm.isAutoIncrement(4));
        assertEquals(false, rm.isCaseSensitive(4));
        assertEquals(false, rm.isCurrency(4));
        assertEquals(false, rm.isNullable(4));
        assertEquals(true, rm.isReadOnly(4));
        assertEquals(false, rm.isSearchable(4));
        assertEquals(false, rm.isSigned(4));

    }
}
