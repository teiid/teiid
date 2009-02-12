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

package com.metamatrix.common.connection.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;

public class JDBCMgdResourceConnection extends ManagedConnection {


    private Connection jdbcConnection;
    private boolean originalAutocommit;

    /**
     * Create a new instance of a JDBC connection.
     * @param env the environment properties for the new connection.
     */
    public JDBCMgdResourceConnection(Properties env, String userName) {
        super(env);
        super.setUserName(userName);
    }

    /**
     * Create the JDBC connection that is being managed.
     * @return the JDBC connection object.
     */
    public Connection getConnection() {
        return this.jdbcConnection;
    }

    // ----------------------------------------------------------------------------------------
    //                 I N I T I A L I Z A T I O N    M E T H O D S
    // ----------------------------------------------------------------------------------------

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be established.
     * @throws ManagedConnectionException if there is an error establishing the connection.
     */
    protected void openConnection() throws ManagedConnectionException {

        try {
            this.jdbcConnection = JDBCConnectionPoolHelper.getInstance().getConnection();
            this.originalAutocommit = jdbcConnection.getAutoCommit();
        } catch (SQLException rpe) {
        	closeConnection();
            throw new ManagedConnectionException(rpe, ErrorMessageKeys.CONNECTION_ERR_0010, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0010, this.getUserName()));
        }
    }

    /**
     * This method is invoked by the pool to notify the specialized class that the
     * connection is to be terminated.
     * @throws ManagedConnectionException if there is an error terminating the connection.
     */
    protected void closeConnection() throws ManagedConnectionException {
        if ( this.jdbcConnection != null ) {
            LogManager.logTrace(LogCommonConstants.CTX_POOLING,"Closing connection to JDBC"); //$NON-NLS-1$
            Throwable firstException=null;
            try {
                  boolean isClosed = false;
                  try {
                        isClosed = this.jdbcConnection.isClosed();
                        if (!isClosed) {
                            if (!this.jdbcConnection.getAutoCommit()) {                            
                                this.jdbcConnection.commit();
                            }
                        }
                  } catch (Exception sqle) {
                        firstException=sqle;
                        LogManager.logWarning(LogCommonConstants.CTX_POOLING, sqle, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0011));
                        throw sqle;
                  } finally {

                	if (!isClosed) {
                	    try {
                    		this.jdbcConnection.setAutoCommit(this.originalAutocommit);
                		} catch (Throwable t) {                    		 
                    		LogManager.logWarning(LogCommonConstants.CTX_POOLING, t, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0011));                   		
                			if (firstException == null) {
                                firstException = t;
                				throw t;
                			}
                    	} finally {
                            try {
                                this.jdbcConnection.close();
                            } catch (Throwable t) {                          
                                LogManager.logWarning(LogCommonConstants.CTX_POOLING, t, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0011));                        
                                if (firstException == null) {
                                    throw t;
                                }
                            }
   
                        }
                	} // isclosed
                  } 

            } catch ( Throwable e ) {
            
                throw new ManagedConnectionException(e, ErrorMessageKeys.CONNECTION_ERR_0012, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0012,
                		this.getEnvironment().getProperty(ManagedConnection.DATABASE, "NoDatabaseProperty"))); //$NON-NLS-1$
            } finally {
                this.jdbcConnection = null;
            }
        }
    }

    // ----------------------------------------------------------------------------------------
    //             T R A N S A C T I O N   M A N A G E M E N T    M E T H O D S
    // ----------------------------------------------------------------------------------------

    /**
     * Prepare this connection for read-only transactions.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void prepareForRead() throws ManagedConnectionException {
        if ( this.jdbcConnection != null ) {
            try {
                LogManager.logTrace(LogCommonConstants.CTX_POOLING,"Attempting to set JDBC transaction to READ ONLY"); //$NON-NLS-1$
                this.jdbcConnection.setAutoCommit(true);
                //setReadOnly(true);
            } catch ( SQLException e ) {
                LogManager.logTrace(LogCommonConstants.CTX_POOLING,"UNABLE to set JDBC transaction to READ ONLY"); //$NON-NLS-1$
            }
        }
    }

    /**
     * Prepare this connection for write transactions.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void prepareForWrite() throws ManagedConnectionException {
        if ( this.jdbcConnection != null ) {
            try {
                LogManager.logTrace(LogCommonConstants.CTX_POOLING,"Attempting to set JDBC transaction to WRITE"); //$NON-NLS-1$
                this.jdbcConnection.setAutoCommit(false);
                //setReadOnly(false);
            } catch ( SQLException e ) {
                LogManager.logTrace(LogCommonConstants.CTX_POOLING,"UNABLE to set JDBC transaction to WRITE"); //$NON-NLS-1$
            }
        }
    }

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void performCommit() throws ManagedConnectionException {
        if ( this.jdbcConnection != null ) {
            try {
                LogManager.logTrace(LogCommonConstants.CTX_POOLING, "Attempting to commit JDBC transaction"); //$NON-NLS-1$
                if (!this.jdbcConnection.getAutoCommit()) {
                    this.jdbcConnection.commit();
                }
            } catch ( SQLException e ) {
                throw new ManagedConnectionException(e, ErrorMessageKeys.CONNECTION_ERR_0013, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0013, this.getEnvironment().getProperty(ManagedConnection.DATABASE, "NoDatabaseProperty"))); //$NON-NLS-1$
            }
        }
    }

    /**
     * Make all changes made since the previous commit/rollback permanent,
     * and release any data source locks currently held by the Connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with this connection.
     */
    protected void performRollback() throws ManagedConnectionException {
        if ( this.jdbcConnection != null ) {
            try {
                if (!this.jdbcConnection.getAutoCommit()) {
                    this.jdbcConnection.rollback();
                }
            } catch ( SQLException e ) {
                throw new ManagedConnectionException(e,ErrorMessageKeys.CONNECTION_ERR_0014, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0014, this.getEnvironment().getProperty(ManagedConnection.DATABASE, "NoDatabaseProperty"))); //$NON-NLS-1$
            }
        }
    }

    // ----------------------------------------------------------------------------------------
    //                 A L L O W A B L E S - R E L A T E D    M E T H O D S
    // ----------------------------------------------------------------------------------------


    // Used for tests:


}

