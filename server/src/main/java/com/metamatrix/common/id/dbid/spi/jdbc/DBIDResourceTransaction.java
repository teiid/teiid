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
import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.jdbc.JDBCPlatformFactory;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;

/**
 * TransactionSourceTransaction implementation.
 */
public class DBIDResourceTransaction extends BaseTransaction implements DBIDSourceTransaction {
    private Connection jdbcConnection;

    private JDBCPlatform platform=null;

//    private static long columnMax=-1;
    private static long columnMax=999999999;

    private static final String NINE = "9"; //$NON-NLS-1$
    private static final int INSERT_RETRIES = 5;


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

            platform = JDBCPlatformFactory.getPlatform(jdbcConnection);

        } catch ( Exception e ) {
            throw new ManagedConnectionException(e,ErrorMessageKeys.ID_ERR_0016, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0016));
        }


    }

    /**
     * <p>Create and return a new ReservedIDBlock.</p>
     * <p>Read in nextID from database, createIDBlock, then update nextID in database.
     *
     * @param blockSize size of id block
     * @return ReservedIDBlock instance
     * @throws TransactionServiceException when an error updating or reading the database occurs
     */
    public ReservedIDBlock createIDBlock(long blockSize, String context, boolean wrapNumber) throws Exception {

        long idValue = -1;
        long startValue = -1;
        long endValue= -1;
        long lastValue = -1;
        String sqlSelect = JDBCNames.SELECT_ID_BLOCK;
        String sqlUpdate = JDBCNames.UPDATE_ID_BLOCK;
        String sqlInsert = JDBCNames.INSERT_ID_BLOCK;
        PreparedStatement selectStatement = null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;

        int retries = 0;

        try {

            if (columnMax == -1) {
                determineColumnMax();
            }


            boolean updateFailed = true;
            while (updateFailed) {

                // if not first time thru, then close statements.
                try {
                    if (selectStatement != null) selectStatement.close();
                } catch (Exception e) {}

                try {
                    if (insertStatement != null) insertStatement.close();
                } catch (Exception e) {}

                try {
                    if (updateStatement != null) updateStatement.close();
                } catch (Exception e) {}

                selectStatement = jdbcConnection.prepareStatement(sqlSelect);
                selectStatement.setString(1,context);
                ResultSet results = selectStatement.executeQuery();

                if (results.next()) {
                    startValue = results.getLong(1);
                } else {

                    LogManager.logInfo(LogCommonConstants.CTX_DBIDGEN, CommonPlugin.Util.getString("MSG.003.013.0002", //$NON-NLS-1$
                    		new Object[] {context, String.valueOf(JDBCNames.START_ID)}) );

                    startValue = JDBCNames.START_ID;

                    try {
                        insertStatement = jdbcConnection.prepareStatement(sqlInsert);
                        insertStatement.setString(1, context);
                        insertStatement.setLong(2, startValue);
                        int nr = insertStatement.executeUpdate();
                        if (nr != 1) {
                            String msg = CommonPlugin.Util.getString("ERR.003.013.0017", //$NON-NLS-1$
                                                                        new Object[] {context, JDBCNames.IDTable.TABLE_NAME});
                            LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, msg);
//                           System.out.println(msg);
                            throw new DBIDGeneratorException(msg);
                        }
                    } catch (SQLException sqle) {
                        ++retries;

                        // allow for retries incase multiple servers are adding the same context
                        // a duplicate exception would be thrown
                        if (retries >= INSERT_RETRIES) {
                            String msg = CommonPlugin.Util.getString("ERR.003.013.0018", //$NON-NLS-1$
                                new Object[] {context, JDBCNames.IDTable.TABLE_NAME});
                            
                           LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, msg);
//                           System.out.println(msg);
                           throw new DBIDGeneratorException(sqle, msg);

//                            throw sqle;
                        }

                        continue;  // try read again
                    }
                }
                lastValue = startValue;

                if (wrapNumber) {
                   startValue = JDBCNames.START_ID;
                   idValue = JDBCNames.START_ID + blockSize;
//                   Integer.parseInt( new Long(blockSize).toString());
                   endValue = idValue - 1;
                } else if ( (startValue + blockSize) > columnMax) {
                  // if next ending value is greater than allowed, then
                  // reset to max allowed
                   idValue = columnMax;
                //Integer.parseInt( new Long(columnMax).toString());
                   // dont decrement the endValue when at maximum because it
                   // leaves 1 extra id available
                   endValue = idValue;
                } else {
                   idValue = startValue + blockSize;
                //Integer.parseInt( new Long(blockSize).toString());
                   endValue = idValue - 1;
                }

                updateStatement = jdbcConnection.prepareStatement(sqlUpdate);
                updateStatement.setLong(1, idValue);
                updateStatement.setString(2, context);
                updateStatement.setLong(3, lastValue);
                int nrows = updateStatement.executeUpdate();

                // if nrows == 1 then update succeeded.
                if (nrows == 1) {
                    updateFailed = false;
                    break;
                }

                // nrows != 0 , something is really wrong, bail
                if (nrows !=  0) {
                    String msg = CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0019,
                                                new Object[] {context, JDBCNames.IDTable.TABLE_NAME});
                       LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, msg);
                       throw new DBIDGeneratorException(msg);
                }
                LogManager.logWarning(LogCommonConstants.CTX_DBIDGEN, 
                                          CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0020, context));
            }
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
        ReservedIDBlock idBlock = new ReservedIDBlock(context, startValue, endValue, columnMax);
//    (idValue == columnMax ? idValue : idValue-1), columnMax);
//    idBlock.setMax(columnMax);

    return idBlock;
    }


    private void determineColumnMax() throws DBIDGeneratorException, SQLException {

          if (columnMax > -1) return;

          int s = platform.getDatabaseColumnSize(JDBCNames.IDTable.TABLE_NAME, 
                                JDBCNames.IDTable.ColumnName.NEXT_ID,
                                jdbcConnection);     
//        Map columns = platform.getDatabaseColumns(JDBCNames.IDTable.TABLE_NAME, jdbcConnection);

          if (s <= 0) {
              LogManager.logCritical(LogCommonConstants.CTX_DBIDGEN, CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0024,
              		new Object[] {JDBCNames.IDTable.TABLE_NAME, JDBCNames.IDTable.ColumnName.NEXT_ID}));
             throw new DBIDGeneratorException(ErrorMessageKeys.ID_ERR_0024,  CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0021,
			 		new Object[] {JDBCNames.IDTable.TABLE_NAME, JDBCNames.IDTable.ColumnName.NEXT_ID}));

          }

//          DatabaseColumn column = (DatabaseColumn) columns.get(JDBCNames.IDTable.ColumnName.NEXT_ID);

          // build the maximum value
          // e.g.  size = 5 then max value = 99999
          if (s > 0) {
              StringBuffer sb = new StringBuffer();
              for (int i = 0; i < s; i++) {
                  sb.append(NINE);
              }

              columnMax = Long.parseLong(sb.toString());
              return;
          }

         columnMax = -1;

    }
}
