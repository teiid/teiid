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

package com.metamatrix.vdb.materialization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.core.util.StringUtil;

/**
 * Enumeration representing a type of database. 
 * @since 4.2
 */
public final class DatabaseDialect implements Serializable {
    
    private static final int NUMBER_OF_TYPES = 6;
    private static final Map DATABASE_DIALECT_MAP = new HashMap(NUMBER_OF_TYPES);
    private static final List DATABASE_DIALECTS = new ArrayList(NUMBER_OF_TYPES);
    
    private static final String ORACLE_TYPE = "Oracle"; //$NON-NLS-1$
    private static final String DB2_TYPE = "DB2"; //$NON-NLS-1$
    private static final String SQL_SERVER_TYPE = "SqlServer"; //$NON-NLS-1$
    private static final String SQL_SERVER_TYPE2 = "sql_server"; //$NON-NLS-1$
    private static final String SYBASE_TYPE = "Sybase"; //$NON-NLS-1$
    private static final String MYSQL_TYPE = "MySQL"; //$NON-NLS-1$
    
    
    private static final String METAMATRIX_TYPE = "MetaMatrix"; //$NON-NLS-1$    
    private static final String CONNECTION_PROPS_TYPE = "ConnectionProps"; //$NON-NLS-1$    
    
    private static final String ORACLE_DRIVER = "com.metamatrix.jdbc.oracle.OracleDriver"; //$NON-NLS-1$
    private static final String DB2_DRIVER = "com.metamatrix.jdbc.db2.DB2Driver"; //$NON-NLS-1$
    private static final String SQLSERVER_DRIVER = "com.metamatrix.jdbc.sqlserver.SQLServerDriver"; //$NON-NLS-1$
    private static final String SYBASE_DRIVER = "com.metamatrix.jdbc.sybase.SybaseDriver"; //$NON-NLS-1$
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver"; //$NON-NLS-1$
    private static final String METAMATRIX_DRIVER = "com.metamatrix.jdbc.MMDriver";  //$NON-NLS-1$
  
    // Represents supported RDBMS types
    public static final DatabaseDialect ORACLE = newDatabaseDialect(ORACLE_TYPE, ORACLE_DRIVER);
    public static final DatabaseDialect DB2 = newDatabaseDialect(DB2_TYPE, DB2_DRIVER);
    public static final DatabaseDialect SQL_SERVER = newDatabaseDialect(SQL_SERVER_TYPE, SQLSERVER_DRIVER);
    public static final DatabaseDialect SYBASE = newDatabaseDialect(SYBASE_TYPE, SYBASE_DRIVER);
    public static final DatabaseDialect MYSQL = newDatabaseDialect(MYSQL_TYPE, MYSQL_DRIVER);

    // Used only internally for generating internal script types
    public static final DatabaseDialect METAMATRIX = new DatabaseDialect(METAMATRIX_TYPE, METAMATRIX_DRIVER);
    public static final DatabaseDialect CONNECTION_PROPS = new DatabaseDialect(CONNECTION_PROPS_TYPE);
    
    private String type;
    private String driverClassname;
    
    /**
     * Get the named type of this <code>DatabaseDialect</code>. 
     * @return The named type.
     */
    public String getType() {
        return this.type;
    }

    /**
     * @return Returns the driverClassname.
     */
    public String getDriverClassname() {
        return driverClassname;
    }
    
    /**
     * Get a collection of all known DatabaseDialects that may be iterated over, etc. 
     * @return All known DatabaseDialect instances.
     * @since 4.2
     */
    public static Collection getAllDialects() {
        return DATABASE_DIALECTS;
    }
    
    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.2
     */
    public boolean equals(Object obj) {
        if ( ! (obj instanceof DatabaseDialect) ) {
            return false;
        }
        return ((DatabaseDialect)obj).type == type;
    }

    /** 
     * @see java.lang.Object#hashCode()
     * @since 4.2
     */
    public int hashCode() {
        return type.hashCode();
    }

    /** 
     * @see java.lang.Object#toString()
     * @since 4.2
     */
    public String toString() {
        return type;
    }

    /**
     * Don't allow outside instantiation. 
     * @param type
     * @since 4.2
     */
    private DatabaseDialect(String type) {
        this(type, null);
    }    
    private DatabaseDialect(String type, String driver) {
        this.type = type;
        this.driverClassname = driver;
    }
    
    /** 
     * @param type
     * @return
     * @since 4.2
     */
    private static DatabaseDialect newDatabaseDialect(final String type, final String driver) {
        final DatabaseDialect result = new DatabaseDialect(type, driver);
        DATABASE_DIALECT_MAP.put(type.toLowerCase(), result);
        DATABASE_DIALECTS.add(result);
        return result;
    }

    /**
     * Implemented so that deserialization of this object
     * produces the same value as the serialized.   
     * @return A new instance of the equivalent serialized object.
     * @throws java.io.ObjectStreamException
     */
    private Object readResolve () throws java.io.ObjectStreamException {
        DatabaseDialect aType = (DatabaseDialect)DATABASE_DIALECT_MAP.get(this.type.toLowerCase());
        if ( aType != null ) {
            return aType;
        }
        return newDatabaseDialect(this.type, this.driverClassname);
    }

    /**
     * Based on the Database type supplied get the dialect supported. If the 
     * type supplied not a registered dialect a null returned
     * @param type - type of database like oracle, sqlserver, db2 mmx etc.
     * @return dialect if match found; null otherwise.
     */
    public static DatabaseDialect getDatabaseDialect(String type) {
        if (type.equalsIgnoreCase("mmx")) { //$NON-NLS-1$
            type = METAMATRIX_TYPE;
        }
        return (DatabaseDialect)DATABASE_DIALECT_MAP.get(type.toLowerCase());        
    }
    
    
    /**
     * Based on the Database type supplied get the dialect supported. If the 
     * type supplied not a registered dialect a null returned
     * @param type - type of database like oracle, sqlserver, db2 mmx etc.
     * @return dialect if match found; null otherwise.
     */
    public static DatabaseDialect getDatabaseDialectByDDLName(String ddlName) {
        
        if (StringUtil.indexOfIgnoreCase(ddlName, ORACLE_TYPE) != -1) {
            return ORACLE;
        }
        else if (StringUtil.indexOfIgnoreCase(ddlName, DB2_TYPE) != -1) {
            return DB2;
        }
        else if ((StringUtil.indexOfIgnoreCase(ddlName, SQL_SERVER_TYPE) != -1) ||
                (StringUtil.indexOfIgnoreCase(ddlName, SQL_SERVER_TYPE2) != -1)) {
            return SQL_SERVER;
        }
        else if (StringUtil.indexOfIgnoreCase(ddlName, SYBASE_TYPE) != -1) {
            return SYBASE;
        }          
        else if (StringUtil.indexOfIgnoreCase(ddlName, MYSQL_TYPE) != -1) {
            return MYSQL;
        }          
        else {
            return METAMATRIX;
        }
    }    
}
