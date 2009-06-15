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

package com.metamatrix.platform.security.audit.destination;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.service.AuditMessage;
import com.metamatrix.platform.PlatformPlugin;
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
    private StringBuffer insertStr;

    public DatabaseAuditDestination() {
        super();
    }

	/**
	 * Return description
	 * @return Description
	 */
	public String getDescription() {
        return "JDBC Shared Connection Pool"; //$NON-NLS-1$
	}

	/**
	 * Initialize this destination with the specified properties.
     * @param props the properties that this destination should use to initialize
     * itself.
     * @throws AuditDestinationInitFailedException if there was an error during initialization.
	 */
	public void initialize(Properties props) throws AuditDestinationInitFailedException {
        super.initialize(props);
 
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
       SQLException ex = null;
       for (int i = 0; i < 3; i++) {
	        try {
	            recordMsg(message);
	            return;
	
	        } catch(SQLException e) {
	        	ex = e;
	        }
       }
       LogManager.logError(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0019, ex));
    }



	/**
	 * Print to the file writer
	 * @param message Message to print
	 */
	public void recordMsg(AuditMessage message) throws SQLException {
		// Add values to Prepared statement
		Connection connection = JDBCConnectionPoolHelper.getInstance().getConnection(); 
		PreparedStatement stmt = null;
		try {
			stmt = connection.prepareStatement(insertStr.toString());

			// Timestamp column
			stmt.setString(1, DateUtil.getDateAsString(new Timestamp(System.currentTimeMillis())));

			// Message context column
			stmt.setString(2, StringUtil.truncString(message.getContext(),
					maxGeneralLength));

			// Message activity column
			stmt.setString(3, StringUtil.truncString(message.getActivity(),
					maxGeneralLength));

			// Resources column
			stmt.setString(4, StringUtil.truncString(message
					.getText(resourceDelim), maxResourceLength));

			// Message principal column
			stmt.setString(5, StringUtil.truncString(message.getPrincipal(),
					maxGeneralLength));

			// Message hostname column
			stmt.setString(6, StringUtil.truncString("n/a", //$NON-NLS-1$
					maxGeneralLength));

			// Message VM ID column
			stmt.setString(7, StringUtil.truncString("n/a", //$NON-NLS-1$
					maxGeneralLength));

			// Insert the row into the table
			stmt.executeUpdate();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				LogManager.logError(LogSecurityConstants.CTX_AUDIT,PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0020, e));
			}

			try {
				connection.close();
			} catch (SQLException e) {
				LogManager.logError(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0021,e));
			}

		}
	}

	/**
	 * Shutdown - close database.
	 */
	public void shutdown() {
	}

}
