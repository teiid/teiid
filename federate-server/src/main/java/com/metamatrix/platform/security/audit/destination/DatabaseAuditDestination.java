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

package com.metamatrix.platform.security.audit.destination;

import java.sql.*;
import java.util.*;

import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.JDBCPlatformFactory;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.audit.AuditMessage;
import com.metamatrix.platform.security.util.LogSecurityConstants;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class DatabaseAuditDestination extends AbstractAuditDestination {

    /**
     * The name of the property that contains the name
     * JDBC database to which log messages are to be recorded.
     * This is a required property that has no default.
     */
    public static final String DATABASE_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcDatabase"; //$NON-NLS-1$

    /**
     * The name of the property that contains the protocol that should
     * be used to connect to the JDBC database to which log messages are to be recorded.
     * This is a required property that has no default.
     */
    public static final String PROTOCOL_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcProtocol"; //$NON-NLS-1$

    /**
     * The name of the property that contains the JDBC driver of the
     * JDBC database to which log messages are to be recorded.
     * This is a required property that has no default.
     */
    public static final String DRIVER_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcDriver"; //$NON-NLS-1$

    /**
     * The name of the property that contains the principal of the
     * JDBC database to which log messages are to be recorded.
     * This is a required property that has no default.
     */
    public static final String PRINCIPAL_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcUsername"; //$NON-NLS-1$

    /**
     * The name of the property that contains the password of the
     * JDBC database to which log messages are to be recorded.
     * This is a required property that has no default.
     */
    public static final String PASSWORD_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcPassword"; //$NON-NLS-1$

    /**
     * The name of the property that contains the name of the table
     * to which log messages are to be recorded.
     * This is an optional property that defaults to "log".
     */
    public static final String TABLE_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcTable"; //$NON-NLS-1$

    /**
     * The name of the property that contains the delimiter used between resources.
     * This is an optional property that defaults to ";".
     */
    public static final String RESOURCE_DELIM_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcResourceDelim"; //$NON-NLS-1$

    /**
     * The name of the property that contains the maximum length allowed
     * for the column that contains the resources portion.
     * This is an optional property that defaults to "4000"; if supplied
     * value is 0 then the length is not checked for each message prior to insertion.
     */
    public static final String MAX_RESOURCE_LENGTH_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcMaxResourceLength"; //$NON-NLS-1$

    /**
     * The name of the property that contains the maximum length allowed
     * for the general column (exception message and exception).
     * This is an optional property that defaults to "64"; if supplied
     * value is 0 then the length is not checked for each message prior to insertion.
     */
    public static final String MAX_GENERAL_LENGTH_PROPERTY_NAME    = AuditDestination.PROPERTY_PREFIX + "jdbcMaxContextLength"; //$NON-NLS-1$

    protected static final String DEFAULT_TABLE_NAME = "AuditEntries"; //$NON-NLS-1$
	protected static final String DEFAULT_RESOURCE_DELIMITER = ";"; //$NON-NLS-1$
	protected static final int DEFAULT_MAX_GENERAL_LENGTH = 64;
	protected static final int DEFAULT_MAX_RESOURCE_LENGTH = 4000;

    public static final class ColumnName {
        public static final String TIMESTAMP = "TimeStamp"; //$NON-NLS-1$
        public static final String CONTEXT   = "Context"; //$NON-NLS-1$
        public static final String ACTIVITY  = "Activity"; //$NON-NLS-1$
        public static final String RESOURCES = "Resources"; //$NON-NLS-1$
        public static final String PRINCIPAL = "Principal"; //$NON-NLS-1$
        public static final String HOST      = "Hostname"; //$NON-NLS-1$
        public static final String VM        = "VMID"; //$NON-NLS-1$
    }


    private String tableName;
     private String resourceDelim    = DEFAULT_RESOURCE_DELIMITER;
	private int maxResourceLength   = DEFAULT_MAX_RESOURCE_LENGTH;
	private int maxGeneralLength    = DEFAULT_MAX_GENERAL_LENGTH;
    private JDBCPlatform jdbcPlatform;
    private StringBuffer insertStr;
    private Properties connProps;

    private Connection con;
    private PreparedStatement stmt;

    public DatabaseAuditDestination() {
        super();
    }

	/**
	 * Return description
	 * @return Description
	 */
	public String getDescription() {
        return "JDBC Pool: " + ResourcePool.JDBC_SHARED_CONNECTION_POOL; //$NON-NLS-1$
	}

	/**
	 * Initialize this destination with the specified properties.
     * @param props the properties that this destination should use to initialize
     * itself.
     * @throws AuditDestinationInitFailedException if there was an error during initialization.
	 */
	public void initialize(Properties props) throws AuditDestinationInitFailedException {
        super.initialize(props);
        connProps = props;
 
        tableName = props.getProperty(TABLE_PROPERTY_NAME, DEFAULT_TABLE_NAME);
        resourceDelim = props.getProperty(RESOURCE_DELIM_PROPERTY_NAME, DEFAULT_RESOURCE_DELIMITER);

		try {
			int max = Integer.parseInt(props.getProperty(MAX_RESOURCE_LENGTH_PROPERTY_NAME));
			if( max > 0 ) {
				maxResourceLength = max;
			}
		} catch(Exception e) {
			// ignore and use default
		}
		try {
			int max = Integer.parseInt(props.getProperty(MAX_GENERAL_LENGTH_PROPERTY_NAME));
			if( max > 0 ) {
				maxGeneralLength = max;
			}
		} catch(Exception e) {
			// ignore and use default
		}

        // construct the insert string
        insertStr = new StringBuffer("INSERT INTO "); //$NON-NLS-1$
        insertStr.append(tableName);
        insertStr.append(" ("); //$NON-NLS-1$
        insertStr.append( ColumnName.TIMESTAMP );
        insertStr.append(',');
        insertStr.append( ColumnName.CONTEXT );
        insertStr.append(',');
        insertStr.append( ColumnName.ACTIVITY );
        insertStr.append(',');
        insertStr.append( ColumnName.RESOURCES );
        insertStr.append(',');
        insertStr.append( ColumnName.PRINCIPAL );
        insertStr.append(',');
        insertStr.append( ColumnName.HOST );
        insertStr.append(',');
        insertStr.append( ColumnName.VM );
        insertStr.append(") VALUES (?,?,?,?,?,?,?)"); //$NON-NLS-1$

		try {

            con = getConnection();

        } catch(Exception ex) {
            throw new AuditDestinationInitFailedException(ex, ErrorMessageKeys.SEC_AUDIT_0022,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0022));
        }

	}

    protected Connection getConnection() throws Exception {

        // Establish connection and prepare statement

          Connection connection = JDBCConnectionPoolHelper.getConnection(this.connProps, "AUDITING"); //$NON-NLS-1$

          stmt = connection.prepareStatement(insertStr.toString());

          jdbcPlatform = JDBCPlatformFactory.getPlatform(connection);

          return connection;

    }




	/**
	 * Get names of all properties used for this destination.  The property name
	 * is "simple" and does not include the standard logger prefix.
	 */
	public List getPropertyNames() {
	    List pnames = new ArrayList();

		pnames.add(TABLE_PROPERTY_NAME);
		pnames.add(RESOURCE_DELIM_PROPERTY_NAME);
		pnames.add(MAX_RESOURCE_LENGTH_PROPERTY_NAME);
		pnames.add(MAX_GENERAL_LENGTH_PROPERTY_NAME);
		return pnames;
	}

    public void record(AuditMessage message) {
       // put this in a while to so that as long a
       while(true) {
            try {
                recordMsg(message);
                return;

            } catch(SQLException ex) {

                try {
                    // if the exception occurred and the connection was open
                    // then report exception and return, no need
                    // retrying to print message and getting same message
                    if (isConnectionOpen()){
                        I18nLogManager.logError(LogSecurityConstants.CTX_AUDIT, ErrorMessageKeys.SEC_AUDIT_0019, ex,
                                   PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0019));
                        ex.printStackTrace();
                        return;
                    }
                    // if an exception is returned from validate connection then
                    // the connection could never be obtained, after a certain period of time,
                    // therefore do not continue
                } catch (SQLException sqle) {
                    I18nLogManager.logError(LogSecurityConstants.CTX_AUDIT, ErrorMessageKeys.SEC_AUDIT_0019, ex,
                            PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0019));
                    return;
                }

            }
       }
    }



	/**
	 * Print to the file writer
	 * @param message Message to print
	 */
	public void recordMsg(AuditMessage message) throws SQLException {
            // Add values to Prepared statement

			// Timestamp column
            stmt.setString(1, DateUtil.getDateAsString(new Timestamp(message.getTimestamp())));

			// Message context column
            stmt.setString(2, StringUtil.truncString(message.getContext(), maxGeneralLength));

			// Message activity column
            stmt.setString(3, StringUtil.truncString(message.getActivity(), maxGeneralLength));

			// Resources column
            stmt.setString(4, StringUtil.truncString(message.getText(resourceDelim), maxResourceLength));

			// Message principal column
            stmt.setString(5, StringUtil.truncString(message.getPrincipal(), maxGeneralLength));

			// Message hostname column
            stmt.setString(6, StringUtil.truncString(message.getHostName(), maxGeneralLength));

			// Message VM ID column
            stmt.setString(7, StringUtil.truncString(message.getVMName(), maxGeneralLength));

			// Insert the row into the table
			stmt.executeUpdate();

	}

    private static final int WAIT_TIME = 60; // 6000 mins
    private static final int RETRY_TIME = 5; // 5 sec

    private static final String MSG1 = "Auditing database connection could not be obtained, reason: "; //$NON-NLS-1$
    private static final String MSG2 = "Auditing database connection is closed, must obtain another."; //$NON-NLS-1$
    private static final String MSG3 = "Error closing auditing database connection."; //$NON-NLS-1$
    protected synchronized boolean isConnectionOpen() throws SQLException {
        if (con != null) {
            if (jdbcPlatform.isClosed(con)) {
                System.err.println(LogSecurityConstants.CTX_AUDIT + " " + MSG2); //$NON-NLS-1$

                // close to return to the pool so that is can be thrown away
                try {
                    con.close();
                } catch (SQLException ce) {
                    System.err.println(LogSecurityConstants.CTX_AUDIT + " " + MSG3); //$NON-NLS-1$
                }
                con = null;
            } else {
                // connection is valid, so return
                return true;
            }
        }
        long waitTime = (WAIT_TIME*1000);
        long endTime = System.currentTimeMillis() + waitTime;

        while (true) {
        // get new connection
            try {
                con = getConnection();
                return false;

            } catch (Exception e) {
                con = null;
                if (waitTime != -1 && System.currentTimeMillis() > endTime) {
                  System.err.println(LogSecurityConstants.CTX_AUDIT + " " + MSG1 + e.getMessage()); //$NON-NLS-1$

                    throw new SQLException(MSG1 + e.getMessage());
                }


                try {
                        Thread.sleep(RETRY_TIME * 1000); // retry every 5 seconds.

                } catch( InterruptedException ie ) {
                // ignore it
                }


            }
        }
    }


	/**
	 * Shutdown - close database.
	 */
	public void shutdown() {
        try {
            stmt.close();
        } catch(SQLException e) {
            I18nLogManager.logError(LogSecurityConstants.CTX_AUDIT, ErrorMessageKeys.SEC_AUDIT_0020, e,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0020));
        }

        try {
	        con.close();
        } catch(SQLException e) {
            I18nLogManager.logError(LogSecurityConstants.CTX_AUDIT, ErrorMessageKeys.SEC_AUDIT_0021, e,
                    PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0021));
        }
	}

}
