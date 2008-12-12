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

package com.metamatrix.common.log.reader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.jdbc.JDBCUtil;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.common.pooling.jdbc.JDBCConnectionResource;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.DateUtil;


/**
 * Reads log entries from the repository database.
 */
public class DBLogReader implements LogReader {
    /**
     * The name of the System property that contains the name of the LogMessageFormat
     * class that is used to format messages sent to the file destination.
     * This is an optional property; if not specified and the file destination
     * is used, then the {@link com.metamatrix.common.logging.format.DelimitedLogMessageFormat DelimitedLogMessageFormat}
     * is used.
     */
    static final String PROPERTY_PREFIX    = "metamatrix.log."; //$NON-NLS-1$

    
    /**
     * The name of the property that contains the name of the table
     * to which log messages are to be recorded.
     * This is an optional property that defaults to "log".
     */
    public static final String TABLE_PROPERTY_NAME = PROPERTY_PREFIX + "jdbcTable"; //$NON-NLS-1$

    public static final String DEFAULT_TABLE_NAME = "LOGENTRIES";//$NON-NLS-1$


   
	
    private Properties connectionProperties;

    protected String tableName;

  
    
    
    public DBLogReader() throws MetaMatrixException {
        init();
    }
    
    public void init() throws MetaMatrixException {
        
        // Read the necessary properties
        Properties globalProperties = CurrentConfiguration.getProperties();
        Properties resourceProperties = CurrentConfiguration.getResourceProperties(ResourceNames.LOGGING);

        Properties props = new Properties();
        props.putAll(globalProperties);
        props.putAll(resourceProperties);
                
        Properties systemProperties = PropertiesUtils.clone(props,System.getProperties(),true,false);
        

        this.connectionProperties = createLogViewerConnectionProperties(systemProperties);
        if ( !(this.connectionProperties instanceof UnmodifiableProperties) ) {
            this.connectionProperties = new UnmodifiableProperties(this.connectionProperties);
        }

        // Get the table name
        this.tableName = systemProperties.getProperty(TABLE_PROPERTY_NAME, DEFAULT_TABLE_NAME );
     }


   
    protected Connection getConnection() throws ManagedConnectionException {
        try {
            Connection connection = JDBCUtil.createJDBCConnection(this.connectionProperties);

            return connection;
        } catch (Exception e) {
            throw new ManagedConnectionException(e, ErrorMessageKeys.LOG_ERR_0009, CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0009));
        }
    }
    

    protected void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
            }
        }
    }
    private void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (Exception e) {
            }
        }
    }

	
    
    /**
     * Due to the change in using ResourcePooling, the database destination for the LogManager
     * does not need to have its properties processed as before in the 
     * original method createConnectionProperties.
     * However, the DirectLogViewer used the original method to obtain its connection properties.  
     * Therefore, the original method was split into two methods to satisfy both needs.
     */

    private static Properties createLogViewerConnectionProperties( Properties props ) throws MetaMatrixException {
        Properties jdbcProps = new Properties();
        // Decrypt connection password
        
        // get the logging connection properties from configuration based on the resource pool defined 
        // for the logging resource.
        ResourceDescriptor rd = null;
        
       
        Properties resourceProps = CurrentConfiguration.getResourceProperties(ResourceNames.LOGGING);
        
        if (resourceProps == null || resourceProps.isEmpty()) {
            String msg = CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0019);
            throw new MetaMatrixException(msg);                
        }
        
        String poolName = resourceProps.getProperty(ResourcePool.RESOURCE_POOL, null);
        
        if (poolName == null) {
            String msg = CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0020);
            throw new MetaMatrixException(msg);                
        }
        
        rd = CurrentConfiguration.getResourceDescriptor(poolName);
        if (rd == null) {
            String msg = CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0021);
            throw new MetaMatrixException(msg);

        }
            
        
               
        String pwd = null;
        try {
            String password = (rd.getProperty(JDBCConnectionResource.PASSWORD));
            if (password == null) {
                String msg = CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0023);
                throw new MetaMatrixException(msg);                
            }
            pwd = CryptoUtil.stringDecrypt(password);
        } catch (CryptoException e) {
            String msg = CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0024);
            throw new MetaMatrixException(msg);                
        }
        jdbcProps.put(JDBCUtil.PASSWORD, pwd);
        jdbcProps.put(JDBCUtil.DRIVER, rd.getProperty(JDBCConnectionResource.DRIVER));
        jdbcProps.put(JDBCUtil.USERNAME, rd.getProperty(JDBCConnectionResource.USERNAME));
        if (rd.getProperty(JDBCConnectionResource.PROTOCOL) != null && rd.getProperty(JDBCConnectionResource.PROTOCOL).trim().length() > 0)  {
        	jdbcProps.put(JDBCUtil.PROTOCOL, rd.getProperty(JDBCConnectionResource.PROTOCOL));
        }
        jdbcProps.put(JDBCUtil.DATABASE, rd.getProperty(JDBCConnectionResource.DATABASE));  
       
        return jdbcProps;
    }
    
    
    
    
    
    
    /** 
     * @see com.metamatrix.platform.admin.api.RuntimeStateAdminAPI#getLogEntries(java.util.Date, java.util.Date, java.util.List, java.util.List, int)
     * @since 4.3
     */
    public List getLogEntries(Date startTime,
                       Date endTime,
                       List levels,
                       List contexts,
                       int maxRows) throws MetaMatrixComponentException {
    
        Connection connection = null;
        Statement statement = null;
        String sqlString = null;
        try {
            //get connection
            connection = getConnection();
            
            //get sql
            sqlString = createSQL(startTime, endTime, levels, contexts, maxRows);
            
            //execute
            statement = connection.createStatement();
            statement.execute(sqlString);

            //convert results
            ResultSet result = statement.getResultSet();
            return convertResults(result, maxRows);
            
        } catch (ManagedConnectionException e) { 
        	throw new MetaMatrixComponentException(e, ErrorMessageKeys.LOG_ERR_0032, CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0032, sqlString));
        } catch (SQLException e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.LOG_ERR_0032, CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0032, sqlString));
        } finally {
            close(statement);
            close(connection);
        }
    }

    
    
    private String createSQL(Date startTime,
                             Date endTime,
                             List levels,
                             List contexts,
                             int maxRows) {
    
        //generate select clause
        StringBuffer sql = new StringBuffer("select "); //$NON-NLS-1$
        
        List columnNames = LogEntryPropertyNames.COLUMN_NAMES;
        for (Iterator iter = columnNames.iterator(); iter.hasNext(); ) {
            String colName = (String) iter.next();
            // Hack for Derby support, in Deryb EXCEPTION is a reserved word
            // so we must enclose in quotes. We have to do it here because
            // if we do it in LogEntryPropertyNames.ColumnName.EXCEPTION then
            // the console breaks.
            if (colName.equals(LogEntryPropertyNames.ColumnName.EXCEPTION)) {
            	sql.append('"'+colName+'"');
            } else {
            	sql.append(colName);
            }
            if (iter.hasNext()) {
                sql.append(", "); //$NON-NLS-1$
            }                
        }
        
        
        //generate from clause            
        sql.append(" from "); //$NON-NLS-1$
        sql.append(tableName);
    
        //generate where clause            
        sql.append(" where "); //$NON-NLS-1$
        sql.append(createTimeSql(startTime, endTime));
        sql.append(" and "); //$NON-NLS-1$
        sql.append(createLevelSql(levels));
    
        String contextSql = createContextSql(contexts);
        if (contextSql != null) {
            sql.append(" and "); //$NON-NLS-1$
            sql.append(contextSql);
        }
    
        
        //create order by clause.  this makes sure we get the latest entries, even if we exceed maxrows
        sql.append(" order by "); //$NON-NLS-1$
        sql.append(LogEntryPropertyNames.ColumnName.TIMESTAMP);
        sql.append(" desc, "); //$NON-NLS-1$
        sql.append(LogEntryPropertyNames.ColumnName.LEVEL);
        
        
        return sql.toString();
    }
    
    private String createTimeSql(Date startTime, Date endTime) {
        StringBuffer sql = new StringBuffer("("); //$NON-NLS-1$
        sql.append(LogEntryPropertyNames.ColumnName.TIMESTAMP);
        sql.append(" >= "); //$NON-NLS-1$
        sql.append("'").append(DateUtil.getDateAsString(startTime)).append("'"); //$NON-NLS-1$ //$NON-NLS-2$
        if (endTime != null) {
            sql.append(" and "); //$NON-NLS-1$
            sql.append(LogEntryPropertyNames.ColumnName.TIMESTAMP);
            sql.append(" <= "); //$NON-NLS-1$
            sql.append("'").append(DateUtil.getDateAsString(endTime)).append("'"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        sql.append(")"); //$NON-NLS-1$
        return sql.toString();
    }
    
    private String createLevelSql(List levels) {
        StringBuffer sql = new StringBuffer("("); //$NON-NLS-1$

        for (Iterator iter = levels.iterator(); iter.hasNext(); ) {
            Integer level = (Integer) iter.next();
            sql.append(LogEntryPropertyNames.ColumnName.LEVEL);
            sql.append(" = "); //$NON-NLS-1$
            sql.append(level);
            
            if (iter.hasNext()) {
                sql.append(" or "); //$NON-NLS-1$
            }        
        }

        sql.append(")"); //$NON-NLS-1$

        return sql.toString();
    }
    
    
    private String createContextSql(List selectedContexts) {
        //null means don't include contexts in the where clause: the query will return entries with all contexts.
        if (selectedContexts == null) {
            return null;
        }

        StringBuffer sql = new StringBuffer("("); //$NON-NLS-1$

        // create sql for context where clause
        for (Iterator iter = selectedContexts.iterator(); iter.hasNext(); ) {
            String context = (String)iter.next();
            
            sql.append(LogEntryPropertyNames.ColumnName.CONTEXT);
            sql.append("='"); //$NON-NLS-1$
            sql.append(context);
            sql.append("'"); //$NON-NLS-1$
            
            if (iter.hasNext()) {
                sql.append(" or "); //$NON-NLS-1$
            } 
        }

        sql.append(")"); //$NON-NLS-1$
        return sql.toString();
    }


    /**
     * Convert ResultSet into List of LogEntries 
     * @param results
     * @param maxRows
     * @throws SQLException
     * @since 4.3
     */
   
    private List convertResults(ResultSet results, int maxRows) throws SQLException {
        
        
        List entries = new ArrayList();
        int nrows = 0;
        while (results.next() && nrows < maxRows) {
            LogEntry entry = new LogEntry();
            
            entry.setContext(results.getString(LogEntryPropertyNames.ColumnName.CONTEXT));
            entry.setException(results.getString(LogEntryPropertyNames.ColumnName.EXCEPTION));
            entry.setHostName(results.getString(LogEntryPropertyNames.ColumnName.HOST));
            entry.setLevel(results.getInt(LogEntryPropertyNames.ColumnName.LEVEL));
            entry.setProcessName(results.getString(LogEntryPropertyNames.ColumnName.VM));
            entry.setThreadName(results.getString(LogEntryPropertyNames.ColumnName.THREAD));
            
            try {
                entry.setDate(DateUtil.convertStringToDate(results.getString(LogEntryPropertyNames.ColumnName.TIMESTAMP)));
            } catch (ParseException e) {                
            }
            
            String message = results.getString(LogEntryPropertyNames.ColumnName.MESSAGE);
            if (message.trim().equalsIgnoreCase("null")) { //$NON-NLS-1$
                message = "<null>"; //$NON-NLS-1$
            }
            entry.setMessage(message);
            
            
            entries.add(entry);
            ++nrows;
        }
        
        return entries;
    }

}
