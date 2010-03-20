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

package org.teiid.client.metadata;

/**
 * This constants in this class indicate the column positions of different
 * metadata columns in queries against Runtime metadata. These constants are
 * used in ResultsMetadataImpl object and in the JDBC driver to obtain column specific
 * metadata information.
 */

public interface ResultsMetadataConstants {
    
    // constant indicating the position of catalog or Virtual database name.
    public static final Integer VIRTUAL_DATABASE_NAME = new Integer(0);
    // constant indicating the position of schema or Virtual database version.
    public static final Integer VIRTUAL_DATABASE_VERSION = new Integer(1);
    // constant indicating the position of table or group name.
    public static final Integer GROUP_NAME = new Integer(2);
    // constant indicating the position of column or element name.
    public static final Integer ELEMENT_NAME = new Integer(3);
    // constant indicating the position of column lable used for display purposes.
    public static final Integer ELEMENT_LABEL = new Integer(4);
    // constant indicating the position of datatype of the column.
    public static final Integer DATA_TYPE = new Integer(6);
    // constant indicating the position of precision of the column.
    public static final Integer PRECISION = new Integer(7);
    // constant indiacting the position of radix of a column.
    public static final Integer RADIX = new Integer(8);
    // constant indicating scale of the column.
    public static final Integer SCALE = new Integer(9);
    // constant indiacting the position of auto-incrementable property of a column.
    public static final Integer AUTO_INCREMENTING = new Integer(10);
    // constant indiacting the position of columns case sensitivity.
    public static final Integer CASE_SENSITIVE = new Integer(11);
    // constant indicating the position of nullable property of a column.
    public static final Integer NULLABLE = new Integer(12);
    // constant indicating the position of searchable property of a column.
    public static final Integer SEARCHABLE = new Integer(13);
    // constant indicating the position of signed property of a column.
    public static final Integer SIGNED = new Integer(14);
    // constant indicating the position of updatable property of a column.
    public static final Integer WRITABLE = new Integer(15);
    // constant indicating if a column is a currency value
    public static final Integer CURRENCY = new Integer(16);
    // constant indicating the display size for a column
    public static final Integer DISPLAY_SIZE = new Integer(17);
    
    /**
     * These types are associated with a DataType or an Element needing the indication of null types. 
     */
    public static final class NULL_TYPES {
        public static final Integer NOT_NULL = new Integer(1);
        public static final Integer NULLABLE = new Integer(2);
        public static final Integer UNKNOWN = new Integer(3);
    }

    /**
     * These types are associated with the Element having valid search types. 
     */
    public static final class SEARCH_TYPES {
        public static final Integer SEARCHABLE = new Integer(1);
        public static final Integer ALLEXCEPTLIKE = new Integer(2);
        public static final Integer LIKE_ONLY = new Integer(3);
        public static final Integer UNSEARCHABLE = new Integer(4);
    }

}
