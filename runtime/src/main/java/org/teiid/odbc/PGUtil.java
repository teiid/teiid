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
package org.teiid.odbc;

import java.sql.Types;

import org.teiid.deployers.PgCatalogMetadataStore;

public class PGUtil {

	public static final int PG_TYPE_VARCHAR = 1043;

	public static final int PG_TYPE_BOOL = 16;
	public static final int PG_TYPE_BYTEA = 17;
	public static final int PG_TYPE_BPCHAR = 1042;
	public static final int PG_TYPE_INT8 = 20;
	public static final int PG_TYPE_INT2 = 21;
	public static final int PG_TYPE_INT4 = 23;
	public static final int PG_TYPE_TEXT = 25;
    //private static final int PG_TYPE_OID = 26;
	public static final int PG_TYPE_FLOAT4 = 700;
	public static final int PG_TYPE_FLOAT8 = 701;
	public static final int PG_TYPE_UNKNOWN = 705;
    
	public static final int PG_TYPE_OIDVECTOR = PgCatalogMetadataStore.PG_TYPE_OIDVECTOR;
	public static final int PG_TYPE_OIDARRAY = PgCatalogMetadataStore.PG_TYPE_OIDARRAY;
	public static final int PG_TYPE_CHARARRAY = PgCatalogMetadataStore.PG_TYPE_CHARARRAY;
	public static final int PG_TYPE_TEXTARRAY = PgCatalogMetadataStore.PG_TYPE_TEXTARRAY;
    
	public static final int PG_TYPE_DATE = 1082;
	public static final int PG_TYPE_TIME = 1083;
	public static final int PG_TYPE_TIMESTAMP_NO_TMZONE = 1114;
	public static final int PG_TYPE_NUMERIC = 1700;
    //private static final int PG_TYPE_LO = 14939;
    
	public static class PgColInfo {
		public String name;
		public int reloid;
		public short attnum;
		public int type;
		public int precision;
	}
		
	/**
	 * Types.ARRAY is not supported
	 */
	public static int convertType(final int type) {
        switch (type) {
        case Types.BIT:
        case Types.BOOLEAN:
            return PG_TYPE_BOOL;
        case Types.VARCHAR:
            return PG_TYPE_VARCHAR;        
        case Types.CHAR:
            return PG_TYPE_BPCHAR;
        case Types.TINYINT:
        case Types.SMALLINT:
        	return PG_TYPE_INT2;
        case Types.INTEGER:
            return PG_TYPE_INT4;
        case Types.BIGINT:
            return PG_TYPE_INT8;
        case Types.NUMERIC:
        case Types.DECIMAL:
            return PG_TYPE_NUMERIC;
        case Types.FLOAT:
        case Types.REAL:
            return PG_TYPE_FLOAT4;
        case Types.DOUBLE:
            return PG_TYPE_FLOAT8;
        case Types.TIME:
            return PG_TYPE_TIME;
        case Types.DATE:
            return PG_TYPE_DATE;
        case Types.TIMESTAMP:
            return PG_TYPE_TIMESTAMP_NO_TMZONE;
            
        case Types.BLOB:            
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        	return PG_TYPE_BYTEA;
        	
        case Types.LONGVARCHAR:
        case Types.CLOB:            
        	return PG_TYPE_TEXT;
        
        case Types.SQLXML:        	
            return PG_TYPE_TEXT;
            
        default:
            return PG_TYPE_UNKNOWN;
        }
	}	
}
