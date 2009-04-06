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

package com.metamatrix.common.id.dbid.spi.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.connection.BaseTransaction;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.jdbc.JDBCMgdResourceConnection;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.id.dbid.ReservedIDBlock;
import com.metamatrix.common.id.dbid.spi.DBIDSourceTransaction;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;

/**
 * TransactionSourceTransaction implementation.
 */
public class DBIDResourceTransaction extends BaseTransaction implements DBIDSourceTransaction {
    private Connection jdbcConnection;

    /**
     * Create a new instance of a transaction for a managed connection.
     * @param connectionPool the pool to which the transaction should return the connection when completed
     * @param connection the connection that should be used and that was created using this
     * factory's <code>createConnection</code> method (thus the transaction subclass may cast to the
     * type created by the <code>createConnection</code> method.
     * @param readonly true if the transaction is to be readonly, or false otherwise
     * @throws ManagedConnectionException if there is an error creating the transaction.
     */
    public DBIDResourceTransaction(ManagedConnection connection, boolean readonly)
        throws ManagedConnectionException {

        super(connection, readonly);

        try {
        	JDBCMgdResourceConnection jdbcManagedConnection = (JDBCMgdResourceConnection) connection;
        	jdbcConnection = jdbcManagedConnection.getConnection();

        } catch ( Exception e ) {
            throw new ManagedConnectionException(e,ErrorMessageKeys.ID_ERR_0016, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0016));
        }


    }
    
    @Override
    public void createIDBlock(long blockSize, String context,
    		ReservedIDBlock block) throws Exception {
        long startValue = -1;
        String sqlSelect = JDBCNames.SELECT_ID_BLOCK;
        String sqlUpdate = JDBCNames.UPDATE_ID_BLOCK;
        String sqlInsert = JDBCNames.INSERT_ID_BLOCK;
        PreparedStatement selectStatement = null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;

        try {
            selectStatement = jdbcConnection.prepareStatement(sqlSelect);
            selectStatement.setString(1,context);
            ResultSet results = selectStatement.executeQuery();

            if (results.next()) {
                startValue = results.getLong(1);
            } else {
                LogManager.logInfo(LogCommonConstants.CTX_DBIDGEN, CommonPlugin.Util.getString("MSG.003.013.0002", //$NON-NLS-1$
                		new Object[] {context, String.valueOf(JDBCNames.START_ID)}) );

                startValue = JDBCNames.START_ID;

                insertStatement = jdbcConnection.prepareStatement(sqlInsert);
                insertStatement.setString(1, context);
                insertStatement.setLong(2, startValue);
                int nr = insertStatement.executeUpdate();
                if (nr != 1) {
                    String msg = CommonPlugin.Util.getString("ERR.003.013.0017", //$NON-NLS-1$
                                                                new Object[] {context, JDBCNames.IDTable.TABLE_NAME});
                    LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, msg);
                    throw new DBIDGeneratorException(msg);
                }
            }

            if ( (startValue + blockSize) > getMaxValue()) {
            	startValue = JDBCNames.START_ID;
            } 
            long next = startValue + blockSize;

            updateStatement = jdbcConnection.prepareStatement(sqlUpdate);
            updateStatement.setLong(1, next);
            updateStatement.setString(2, context);
            int nrows = updateStatement.executeUpdate();

            // nrows != 0 , something is really wrong, bail
            if (nrows !=  1) {
                String msg = CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0019,
                                            new Object[] {context, JDBCNames.IDTable.TABLE_NAME});
               LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, msg);
               throw new DBIDGeneratorException(msg);
            }
            block.setBlockValues(startValue, next - 1);
        } catch (DBIDGeneratorException dbe) {
        	throw dbe;
        } catch ( SQLException se ) {
              LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0021, context));
             throw new DBIDGeneratorException(se, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0021, context));
        } catch ( Exception e ) {
              LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0022, context));
             throw new DBIDGeneratorException(e, ErrorMessageKeys.ID_ERR_0022,  CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0022, context));
        } finally {
            if ( selectStatement != null ) {
                try {
                    selectStatement.close();
                } catch ( SQLException e2 ) {
               		LogManager.logWarning(LogCommonConstants.CTX_DBIDGEN, e2, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0023, context));

                 }
            }
            if ( insertStatement != null ) {
                try {
                    insertStatement.close();
                } catch ( SQLException e2 ) {
              		LogManager.logWarning(LogCommonConstants.CTX_DBIDGEN, e2, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0023, context));
                }
            }
            if ( updateStatement != null ) {
                try {
                    updateStatement.close();
                } catch ( SQLException e2 ) {
                    LogManager.logWarning(LogCommonConstants.CTX_DBIDGEN, e2, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0023, context));

                 }
            }
        }
    }

    public long getMaxValue() {
    	return 999999999;
    }
}
