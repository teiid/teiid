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

package com.metamatrix.common.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.HashMap;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * DO NOT USE LogManager in this class
 * 
 * Here's the matrix for determining which platform to use:
 * PN = Product Name, which means the product name from the metadata will help determine the platform
 * Protocol = means the Protocol (mmx:oracle, oracle, sequelink) will help determine the platform
 * 
 * 
 * Driver |             Oracle      DB2     SQLServer       Sybase      Informix
 * ------------------------------------------------------------------------------
 * Native               PN          PN          PN              PN      PN
 * 
 * MMX (DD)             Protocol    PN          PN              PN      PN
 *                    (mmx:oracle)
 *                    
 * MetaMatrix JDBC      Protocol -->(SAME)   -->(SAME)   ----->(SAME) --> (SAME)   
 *
 * ------------------------------------------------------------------------------ 
 * 
 * The process will look 1st at the protocol and then the product name to further 
 * deliniate the type of platform that should be used.
 * 
 */
public class JDBCPlatformFactory {

    /**
    * These are the platforms supported
    */
    public interface Supported {
        public static final String ORACLE = "oracle"; //$NON-NLS-1$
        public static final String SYBASE = "sybase"; //$NON-NLS-1$
        public static final String DB2 = "db2"; //$NON-NLS-1$
        public static final String MSSQL = "microsoft"; //$NON-NLS-1$
        public static final String INFORMIX = "informix"; //$NON-NLS-1$
        public static final String METAMATRIX = "metamatrix"; //$NON-NLS-1$
        public static final String MM_ORACLE = "mmx:oracle"; //$NON-NLS-1$
        public static final String MYSQL = "mysql"; //$NON-NLS-1$
        public static final String POSTGRES = "postgres"; //$NON-NLS-1$       

        // default
        public static final String DEFAULT = "default"; //$NON-NLS-1$
        public static final String DERBY = "derby"; //$NON-NLS-1$
    }


    /**
    *  The use of platforms is a secondary search option
    *  in case the supported platforms don't match
    *  to the product name
    */
    protected interface Protocol {
        public static final String MSSQL = "mssql"; //$NON-NLS-1$
        public static final String SQLSERVER = "sqlserver"; //$NON-NLS-1$
        public static final String ORACLE = "oracle"; //$NON-NLS-1$
        public static final String DB2 = "db2"; //$NON-NLS-1$
        public static final String SYBASE = "sybase"; //$NON-NLS-1$
        public static final String INFORMIX = "informix-sqli"; //$NON-NLS-1$
        public static final String METAMATRIX = "metamatrix"; //$NON-NLS-1$
        public static final String MM_ORACLE = "mmx:oracle"; //$NON-NLS-1$
        public static final String DERBY = "derby"; //$NON-NLS-1$
        public static final String MYSQL = "mysql"; //$NON-NLS-1$
        public static final String POSTGRES = "postgres"; //$NON-NLS-1$
        
    }

    protected interface PlatformClass {
        public static final String MSSQL = "com.metamatrix.common.jdbc.db.MSSQLPlatform"; //$NON-NLS-1$
        public static final String DB2 = "com.metamatrix.common.jdbc.db.DB2Platform"; //$NON-NLS-1$
        public static final String ORACLE = "com.metamatrix.common.jdbc.db.OraclePlatform"; //$NON-NLS-1$
        public static final String SYBASE = "com.metamatrix.common.jdbc.db.SybasePlatform"; //$NON-NLS-1$
        public static final String INFORMIX = "com.metamatrix.common.jdbc.db.InformixPlatform"; //$NON-NLS-1$
        public static final String METAMATRIX = "com.metamatrix.common.jdbc.db.MetaMatrixPlatform"; //$NON-NLS-1$
        public static final String DEFAULT = "com.metamatrix.common.jdbc.JDBCPlatform"; //$NON-NLS-1$

        public static final String MMORACLE = "com.metamatrix.common.jdbc.db.MMOraclePlatform"; //$NON-NLS-1$
        public static final String DERBY = "com.metamatrix.common.jdbc.db.DerbyPlatform"; //$NON-NLS-1$
        public static final String MYSQL = "com.metamatrix.common.jdbc.db.MySQLPlatform"; //$NON-NLS-1$
        public static final String POSTGRES = "com.metamatrix.common.jdbc.db.PostgresPlatform"; //$NON-NLS-1$

    }

    private static final String DEFAULT_PLATFORM = "default"; //$NON-NLS-1$
    private static Map classMap;

    private static Map platformCache;


    static {
          platformCache = new HashMap(10);

        classMap = new HashMap(10);
        classMap.put(Supported.DB2, PlatformClass.DB2);
        classMap.put(Supported.MSSQL, PlatformClass.MSSQL);
        classMap.put(Supported.ORACLE, PlatformClass.ORACLE);
        classMap.put(Supported.SYBASE, PlatformClass.SYBASE);
        classMap.put(Supported.INFORMIX, PlatformClass.INFORMIX);
        classMap.put(Supported.METAMATRIX, PlatformClass.METAMATRIX);
        classMap.put(Supported.MM_ORACLE, PlatformClass.MMORACLE);
        classMap.put(Supported.MYSQL, PlatformClass.MYSQL);
        classMap.put(Supported.POSTGRES, PlatformClass.POSTGRES);
        
 
        classMap.put(DEFAULT_PLATFORM, PlatformClass.DEFAULT);
        classMap.put(Supported.DERBY, PlatformClass.DERBY);
        
     
    }

    public static JDBCPlatform getPlatform(Connection jdbcConnection) throws MetaMatrixException {
        try {
            DatabaseMetaData metadata = jdbcConnection.getMetaData();
            String productName = metadata.getDatabaseProductName();
            String driverName = metadata.getDriverName();

            JDBCPlatform p = getPlatform(metadata.getURL(), driverName, productName);
            if (p != null) {
                p.setConnection(jdbcConnection);
            }
            return p;
        } catch (Exception sqle) {
            throw new MetaMatrixException(sqle, ErrorMessageKeys.JDBC_ERR_0003, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0003));
        }

    }   
    
    public static JDBCPlatform getPlatform(String url, String driverName) throws MetaMatrixException {
        
        String productName = getSupportedByProtocol(url);
        if (productName == null) {
            return null;
        }
        return getPlatform(url, driverName, productName );
    }
    
        
        
    static JDBCPlatform getPlatform(String url, String driverName, String productName) throws MetaMatrixException {
        try {
            boolean isSecure = false;
             
            JDBCURL jdbcurl = new JDBCURL(url);
            
            String dbplatform = getSupportedByProductName(productName); 
            
            String supported =  getSupportedByProtocol(jdbcurl.getProtocol());
            
           
            if (supported == null) {
                supported = dbplatform;
            }
            
            if (supported == null) {
                supported = Supported.DEFAULT;
            }
            
            String platformClass = getPlatformClass(supported); 

            
            
            String key = driverName + " - " + jdbcurl.getProtocol(); //$NON-NLS-1$
            
            JDBCPlatform p = createPlatform(dbplatform, platformClass, key, isSecure);
            
            return p;
        } catch (Exception sqle) {
             throw new MetaMatrixException(sqle, ErrorMessageKeys.JDBC_ERR_0003, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0003));
        }
    }


    private static JDBCPlatform createPlatform(String productName, String platformClass, String key, boolean isSecure) throws MetaMatrixException {
        try {
            
            JDBCPlatform p = getAvailablePlatform(platformClass, key);

            if (p != null) {
                return p;
            }
            Object o = Class.forName(platformClass).newInstance();
            if (o instanceof JDBCPlatform) {
                p = (JDBCPlatform) o;
            } else {
               throw new MetaMatrixException(ErrorMessageKeys.JDBC_ERR_0005, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0005));
            }
            
            if (isSecure) {
                p.setIsSecure(isSecure);
            }

            p.setPlatformName(productName);
            p.initializePlatform();

            addAvailablePlatform(platformClass, key, p);

            return p;

        } catch (Exception cnfe) {
             throw new MetaMatrixException(cnfe, ErrorMessageKeys.JDBC_ERR_0003, CommonPlugin.Util.getString(ErrorMessageKeys.JDBC_ERR_0003));
        }
    }

    private static JDBCPlatform getAvailablePlatform(String platform, String key) {

        JDBCPlatform p =null;
        Map platformInstances = null;
        if (platformCache.containsKey(platform)) {
            platformInstances = (Map) platformCache.get(platform);
            if (platformInstances.containsKey(key)) {
                p = (JDBCPlatform) platformInstances.get(key);
            }
        } else {
            platformInstances = new HashMap(10);
            platformCache.put(platform, platformInstances);
        }

        return p;
    }

    private static void addAvailablePlatform(String platform, String key, JDBCPlatform jdbc) {

        Map platformInstances = (Map) platformCache.get(platform);
        platformInstances.put(key, jdbc);
    }

    static String getSupportedByProtocol(String value) {
//        System.out.println("==== Look for platform by product " + value);
        String lower = value.toLowerCase();
        
        if (lower.indexOf(Protocol.METAMATRIX) >= 0) {
            return Supported.METAMATRIX;
        } else if (lower.indexOf(Protocol.MM_ORACLE) >= 0) {
            return Supported.MM_ORACLE;
        } else if (lower.indexOf(Protocol.MSSQL) >= 0 || lower.indexOf(Protocol.SQLSERVER) >= 0) {
            return Supported.MSSQL;
        } else if (lower.indexOf(Protocol.DB2) >= 0) {
            return Supported.DB2;
        } else if (lower.indexOf(Protocol.ORACLE) >= 0) {
            return Supported.ORACLE;
        } else if (lower.indexOf(Protocol.SYBASE) >= 0) {
            return Supported.SYBASE;
        } else if (lower.indexOf(Protocol.INFORMIX) >= 0) {
            return Supported.INFORMIX;
        } else if (lower.indexOf(Protocol.DERBY) >= 0) {
            return Supported.DERBY;
        } else if (lower.indexOf(Protocol.MYSQL) >= 0) {
            return Supported.MYSQL;
        } else if (lower.indexOf(Protocol.POSTGRES) >= 0) {
            return Supported.POSTGRES;
        } 

        return null;
    }
    
    static String getSupportedByProductName(String value) {
//        System.out.println("==== Look for platform by product " + value);
        String lower = value.toLowerCase();
        
        if (lower.indexOf(Supported.MSSQL) >= 0) {
            return Supported.MSSQL;
        } else if (lower.indexOf(Supported.DB2) >= 0) {
            return Supported.DB2;
        } else if (lower.indexOf(Supported.ORACLE) >= 0) {
            return Supported.ORACLE;
        } else if (lower.indexOf(Supported.SYBASE) >= 0) {
            return Supported.SYBASE;
        } else if (lower.indexOf(Supported.INFORMIX) >= 0) {
            return Supported.INFORMIX;
        } else  if (lower.indexOf(Supported.METAMATRIX) >= 0) {
            return Supported.METAMATRIX;
        } else  if (lower.indexOf(Supported.DERBY) >= 0) {
            return Supported.DERBY;
        } else  if (lower.indexOf(Supported.POSTGRES) >= 0) {
            return Supported.POSTGRES;
        } else  if (lower.indexOf(Supported.MYSQL) >= 0) {
            return Supported.MYSQL;
        } 
        
        return Supported.DEFAULT;

    }
    
    static String getPlatformClass(String supported) {
        String platformClass = (String) classMap.get(supported);
        if (platformClass == null) {
            platformClass = (String) classMap.get(Supported.DEFAULT);
        }
        return platformClass;
        
    }
}
