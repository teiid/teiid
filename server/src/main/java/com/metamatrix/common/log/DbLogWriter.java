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

package com.metamatrix.common.log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.StringUtil;

/**
 *  - flag for turning off logging after # times unsuccessful writes (2) or can't connect (1 retry)
 * -  timestamp when turned off - determine period to reset flags
 * -  on msg write - check flag or if period for resume retry
 * -  any messages sent during the down time will not written out by this logger
 * -  add System.err messages when restarting and stopping the logging
 * 
 */
public class DbLogWriter {

	/**
     * Static String to use as the user name when checking out connections from the pool
     */
    static final String LOGGING = "LOGGING";//$NON-NLS-1$

	/**
	 * The name of the System property that contains the name of the LogMessageFormat
	 * class that is used to format messages sent to the file destination.
	 * This is an optional property; if not specified and the file destination
	 * is used
	 */
	static final String PROPERTY_PREFIX    = "metamatrix.log."; //$NON-NLS-1$  


	/**
	 * The name of the property that contains the name
	 * JDBC database to which log messages are to be recorded.
	 * This is a required property that has no default.
	 */
	public static final String DATABASE_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcDatabase"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the protocol that should
	 * be used to connect to the JDBC database to which log messages are to be recorded.
	 * This is a required property that has no default.
	 */
	public static final String PROTOCOL_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcProtocol"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the JDBC driver of the
	 * JDBC database to which log messages are to be recorded.
	 * This is a required property that has no default.
	 */
	public static final String DRIVER_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcDriver"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the principal of the
	 * JDBC database to which log messages are to be recorded.
	 * This is a required property that has no default.
	 */
	public static final String PRINCIPAL_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcUsername"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the password of the
	 * JDBC database to which log messages are to be recorded.
	 * This is a required property that has no default.
	 */
	public static final String PASSWORD_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcPassword"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the name of the table
	 * to which log messages are to be recorded.
	 * This is an optional property that defaults to "log".
	 */
	public static final String TABLE_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcTable"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the maximum length allowed
	 * for the column that contains the message portion.
	 * This is an optional property that defaults to "2000"; if supplied
	 * value is 0 then the length is not checked for each message prior to insertion.
	 */
	public static final String MAX_MESSAGE_LENGTH_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcMaxMsgLength"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the maximum length allowed
	 * for the general column (exception message and exception).
	 * This is an optional property that defaults to "64"; if supplied
	 * value is 0 then the length is not checked for each message prior to insertion.
	 */
	public static final String MAX_GENERAL_LENGTH_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcMaxContextLength"; //$NON-NLS-1$

	/**
	 * The name of the property that contains the name of the table
	 * to which log messages are to be recorded.
	 * This is an optional property that defaults to "4000"; if supplied
	 * value is 0 then the length is not checked for each message prior to insertion.
	 */
	public static final String MAX_EXCEPTION_LENGTH_PROPERTY_NAME    = PROPERTY_PREFIX + "jdbcMaxExceptionLength"; //$NON-NLS-1$

	public static final String DEFAULT_TABLE_NAME = "LOGENTRIES";  //$NON-NLS-1$
	public static final int DEFAULT_MAX_GENERAL_LENGTH = 64;
	public static final int DEFAULT_MAX_EXCEPTION_LENGTH = 4000;
	public static final int DEFAULT_MAX_MSG_LENGTH = 2000;
	private static final String NULL = "Null";    //$NON-NLS-1$
	
	public static final String PLUGIN_PREFIX = "com.metamatrix.";  //$NON-NLS-1$

	public static final class ColumnName {
		public static final String TIMESTAMP        = "TIMESTAMP"; //$NON-NLS-1$
		public static final String SEQUENCE_NUMBER  = "VMSEQNUM"; //$NON-NLS-1$
		public static final String CONTEXT          = "CONTEXT"; //$NON-NLS-1$
		public static final String LEVEL            = "MSGLEVEL"; //$NON-NLS-1$
		public static final String EXCEPTION        = "EXCEPTION"; //$NON-NLS-1$
		public static final String MESSAGE          = "MESSAGE"; //$NON-NLS-1$
		public static final String HOST             = "HOSTNAME"; //$NON-NLS-1$
		public static final String VM               = "VMID"; //$NON-NLS-1$
		public static final String THREAD           = "THREADNAME"; //$NON-NLS-1$
	}
    
    private static final int WRITE_RETRIES = 5; // # of retries before stop writing
    private static final int RESUME_LOGGING_AFTER_TIME =  300 * 1000; // 5 mins 
    
    private boolean isLogSuspended=false;
    private long resumeTime=-1;
    
	private short sequenceNumber;
	private long lastSequenceStart;
	private int maxMsgLength        = DEFAULT_MAX_MSG_LENGTH;
	private int maxGeneralLength    = DEFAULT_MAX_GENERAL_LENGTH;
	private int maxExceptionLength  = DEFAULT_MAX_EXCEPTION_LENGTH;

	private Properties connProps;
	private String insert;
       
    private boolean shutdown = false;
    
    private String quote = null;


	public DbLogWriter(Properties properties) {
		connProps = properties;
	}


	/* (non-Javadoc)
	 * @see com.metamatrix.core.log.LogListener#shutdown()
	 */
    
    public synchronized void shutdown() {
        shutdown = true;
    }
	
	public String getTableName(Properties props) {
		String tableName = props.getProperty(TABLE_PROPERTY_NAME, DEFAULT_TABLE_NAME);
		return tableName;
	}
	
	private static final String INSERT_INTO = "INSERT INTO "; //$NON-NLS-1$
	private static final String LEFT_PAREN = " ("; //$NON-NLS-1$
	private static final String COMMA = ","; //$NON-NLS-1$
	private static final String VALUES = ") VALUES (?,?,?,?,?,?,?,?,?)"; //$NON-NLS-1$
	/**
	 * Initialize this destination with the specified properties.
	 * @param props the properties that this destination should use to initialize
	 * itself.
	 * @throws LogDestinationInitFailedException if there was an error during initialization.
	 */
	public void initialize() {
		sequenceNumber = 0;
		lastSequenceStart = 0;

		try {
			int max = Integer.parseInt(connProps.getProperty(MAX_MESSAGE_LENGTH_PROPERTY_NAME));
			if( max > 0 ) {
				maxMsgLength = max;
			}
		} catch(Exception e) {
			// ignore and use default
		}
		try {
			
			int max = Integer.parseInt(connProps.getProperty(MAX_GENERAL_LENGTH_PROPERTY_NAME));
			if( max > 0 ) {
				maxGeneralLength = max;
			}
		} catch(Exception e) {
			// ignore and use default
		}
		try {
			int max = Integer.parseInt(connProps.getProperty(MAX_EXCEPTION_LENGTH_PROPERTY_NAME));
			if( max > 0 ) {
				maxExceptionLength = max;
			}
		} catch(Exception e) {
			// ignore and use default
		}
		
        Connection connection = null;
        try {
        	connection = JDBCConnectionPoolHelper.getInstance().getConnection();
        	quote = JDBCPlatform.getIdentifierQuoteString(connection); 
        	
        } catch (SQLException e) {
         } finally {
			if( connection != null ) {   
				try {
					connection.close(); 
				} catch (SQLException e) {
					
				}
			}
		}

	}

	public synchronized void logMessage(int level, String context,Object msg, Throwable t) {
		write(level, context, msg, t);	
	}
			
	private void write(int level, String context, Object message, Throwable t) {
		// put this in a while to so that as long a 
        int retrycnt = 0;
        if (isLogSuspended && System.currentTimeMillis() > resumeTime) {             
            resumeLogging();
        }
		while (!isLogSuspended && !shutdown) {
			try {
				printMsg(level, context, message, t);
				return;

			} catch (Exception ex) {

                if (retrycnt >= WRITE_RETRIES) {
                    suspendLogging();
                } else {
                    resumeLogging();
                }
                ++retrycnt;                
			} 
		}
	}
	
	private String getInsertStr(Connection c) throws SQLException {
		if (this.insert == null) {
			// construct the insert string
			StringBuffer insertStr = new StringBuffer(INSERT_INTO);
			insertStr.append(getTableName(connProps));
			insertStr.append(LEFT_PAREN);
			insertStr.append( ColumnName.TIMESTAMP );
			insertStr.append(COMMA);
			insertStr.append( ColumnName.SEQUENCE_NUMBER );
			insertStr.append(COMMA);
			insertStr.append( ColumnName.CONTEXT );
			insertStr.append(COMMA);
			insertStr.append( ColumnName.LEVEL );
			insertStr.append(COMMA);
			insertStr.append( ColumnName.MESSAGE );
			insertStr.append(COMMA);
			insertStr.append( ColumnName.HOST );
			insertStr.append(COMMA);
			insertStr.append( ColumnName.VM );
			insertStr.append(COMMA);
			insertStr.append( ColumnName.THREAD );
			insertStr.append(COMMA);
			insertStr.append( quote+ ColumnName.EXCEPTION +quote );
			insertStr.append(VALUES);
			this.insert = insertStr.toString();
		}
		return this.insert;
	}
	
	private void printMsg(int level, String context, Object message, Throwable t) throws SQLException {
        if (this.shutdown) {
            return;
        }
        
		long msgTimeStamp = System.currentTimeMillis();
		if (lastSequenceStart != msgTimeStamp) {
			lastSequenceStart = msgTimeStamp;
			sequenceNumber = 0;
		}
		Connection connection = null;
		PreparedStatement stmt = null;
		try {
			connection = JDBCConnectionPoolHelper.getInstance().getConnection();
			stmt = connection.prepareStatement(getInsertStr(connection));  

			// Add values to Prepared statement
            
			// Timestamp column
			stmt.setString(1, DateUtil.getDateAsString(new Timestamp(msgTimeStamp)));

			// VM Sequence number
			stmt.setShort(2, sequenceNumber);

			// Message context column
			stmt.setString(3, StringUtil.truncString(context, maxGeneralLength));

			// Message type column
			stmt.setInt(4, level);

			// Message text column
			stmt.setString(5, StringUtil.truncString(message.toString(), maxMsgLength));

			// Message hostname column
			stmt.setString(6, StringUtil.truncString(CurrentConfiguration.getInstance().getConfigurationName(), maxGeneralLength)); 

			// Message VM ID column
			stmt.setString(7, StringUtil.truncString(CurrentConfiguration.getInstance().getProcessName(), maxGeneralLength));

			// Message thread name column
			stmt.setString(8, StringUtil.truncString(Thread.currentThread().getName(), maxGeneralLength));

			// Exception column
			if(t != null) {
				String eMsg = t.getMessage();
				if ( eMsg == null ) {
					eMsg = NULL;
				} else {
					eMsg = StringUtil.truncString(eMsg, maxExceptionLength);
				}
				stmt.setString(9, eMsg);
			} else {
				stmt.setString(9, NULL);
			}


			// Insert the row into the table
			stmt.executeUpdate();
		} finally   {         

			// Increment VM sequence number
			++sequenceNumber;
			
			try {
				if( stmt != null ) {
					stmt.close();
				}
			} catch(SQLException ex) {
				System.err.println(CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0027) + ex.getMessage());
			}
	        
			try {
				if( connection != null ) {     
					connection.close();                
				}
			} catch(SQLException ex) {
				System.err.println(CommonPlugin.Util.getString(ErrorMessageKeys.LOG_ERR_0027) + ex.getMessage());
			}
		}
            
	}
    
    private void suspendLogging() {
        // suspend logging until the resumeTime has passed
        isLogSuspended=true;
        resumeTime = System.currentTimeMillis() + RESUME_LOGGING_AFTER_TIME;
        Date rd = new Date(resumeTime);
        String stringDate = DateUtil.getDateAsString(rd);
        System.err.println(CommonPlugin.Util.getString("DBLogWriter.Database_Logging_has_been_suspended", stringDate)); //$NON-NLS-1$
        
    }
    
    private void resumeLogging() {
        // if the resume time has passed, then set the suspended flag to false
        // so that logging will resume
        isLogSuspended=false;
        resumeTime=-1;
        
        Date rd = new Date(System.currentTimeMillis());
        String stringDate = DateUtil.getDateAsString(rd);
        
        System.err.println(CommonPlugin.Util.getString("DBLogWriter.Database_Logging_has_been_resumed", stringDate)); //$NON-NLS-1$
    }

}
