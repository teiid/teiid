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

package com.metamatrix.server.dqp.service.tracker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.server.ServerPlugin;

/**
 *
 */
public class TransactionLogWriter {

    public static final String TRANSACTION_LOG_STATEMENT = "INSERT INTO TX_TXNLOG (TXNUID,TXNPOINT,TXN_STATUS,SESSIONUID,PRINCIPAL_NA,VDBNAME,VDBVERSION,CREATED_TS,ENDED_TS) VALUES (?,?,?,?,?,?,?,?,?)"; //$NON-NLS-1$
    public static final String MMX_COMMAND_LOG_STATEMENT = "INSERT INTO TX_MMXCMDLOG (REQUESTID,TXNUID,CMDPOINT,CMD_STATUS,SESSIONUID,APP_NAME,PRINCIPAL_NA,VDBNAME,VDBVERSION,CREATED_TS,ENDED_TS,FINL_ROWCNT,SQL_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"; //$NON-NLS-1$
    public static final String SQL_LOG_STATEMENT = "INSERT INTO TX_SQL (SQL_ID,SQL_VL) VALUES (?,?)"; //$NON-NLS-1$
    public static final String SRC_COMMAND_LOG_STATEMENT = "INSERT INTO TX_SRCCMDLOG (REQUESTID,NODEID,SUBTXNUID,CMD_STATUS,MDL_NM,CNCTRNAME,CMDPOINT,SESSIONUID,PRINCIPAL_NA,CREATED_TS,ENDED_TS,SQL_ID,FINL_ROWCNT) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"; //$NON-NLS-1$

    public static final int DEFAULT_MAX_VDB_NAME_LENGTH = 255;
    public static final int DEFAULT_MAX_VDB_VERSION_LENGTH = 50;
    public static final int DEFAULT_MAX_CNCTR_NAME_LENGTH = 255;
    public static final int DEFAULT_MAX_MDL_NAME_LENGTH = 255;
    public static final int DEFAULT_MAX_SQL_LENGTH = 512;

    private static final int WRITE_RETRIES = 3; // # of retries before stop writing
    private static final int RESUME_LOGGING_AFTER_TIME =  180 * 1000; // 3 mins 
    
    private Properties connProps;
    
    private static volatile boolean isLogSuspended=false;
    private static volatile long resumeTime=-1;
    private boolean shutdown = false;
    
    /**
     * Initialize this destination with the specified properties.
     * 
     * @param props the properties that this destination should use to
     *            initialize itself.
     * @throws LogDestinationInitFailedException if there was an error during
     *             initialization.
     */
    public TransactionLogWriter(Properties props) {
        this.connProps = props;
    }

    public void print(TransactionLogMessage message) {
        int retrycnt = 0;
        if (isLogSuspended && System.currentTimeMillis() > resumeTime) {
            resumeLogging();
        }

        //retry until suspended
        while (!isLogSuspended && !shutdown) {
            try {
                printMsg(message);
                return;            
            } catch (SQLException t) {
                if (retrycnt >= WRITE_RETRIES) {
                    isLogSuspended=true;
                    resumeTime = System.currentTimeMillis() + RESUME_LOGGING_AFTER_TIME;
                    Date rd = new Date(resumeTime);
                    String stringDate = DateUtil.getDateAsString(rd);
                    LogManager.logError(LogCommonConstants.CTX_TXN_LOG, t, ServerPlugin.Util.getString("TransactionLogWriter.Transaction_Logging_has_been_suspended", stringDate)); //$NON-NLS-1$
                } else {
                    LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, t, t.getMessage()); 
                }
                ++retrycnt;
            }
        }
        
        LogManager.logError(LogCommonConstants.CTX_TXN_LOG, ServerPlugin.Util.getString("TransactionLogWriter.Transaction_Logging_message_while_suspended", message.toString())); //$NON-NLS-1$
    }
    
    private void resumeLogging() {
        // if the resume time has passed, then set the suspended flag to false
        // so that logging will resume
        isLogSuspended=false;
        resumeTime=-1;
        
        Date rd = new Date(System.currentTimeMillis());
        String stringDate = DateUtil.getDateAsString(rd);
        
        LogManager.logInfo(LogCommonConstants.CTX_TXN_LOG, ServerPlugin.Util.getString("TransactionLogWriter.Transaction_Logging_has_been_resumed", stringDate)); //$NON-NLS-1$
    }
    
    private void printMsg(TransactionLogMessage message) throws SQLException {
        Connection con = null;
        PreparedStatement ps = null;
        
        try {
            con = JDBCConnectionPoolHelper.getInstance().getConnection();
            con.setAutoCommit(true);
            
            short dest = message.getDestinationTable();
    
            switch (dest) {
                case TransactionLogMessage.DEST_TXNLOG:
                    ps = con.prepareStatement(TRANSACTION_LOG_STATEMENT);
                    ps.setString(1, message.getTxnUid());
                    ps.setShort(2, message.getPoint());
                    ps.setShort(3, message.getStatus());
                    ps.setString(4, message.getSessionUid());
                    ps.setString(5, message.getPrincipal());
                    ps.setString(6, truncString(message.getVdbName(), DEFAULT_MAX_VDB_NAME_LENGTH));
                    ps.setString(7, truncString(message.getVdbVersion(), DEFAULT_MAX_VDB_VERSION_LENGTH));
                    ps.setString(8, message.getBeginTimeStamp());
                    ps.setString(9, message.getEndTimeStamp());
                    ps.executeUpdate();
                    break;
                case TransactionLogMessage.DEST_MMXCMDLOG:
                    ps = con.prepareStatement(MMX_COMMAND_LOG_STATEMENT);
                    ps.setString(1, message.getRequestId());
                    ps.setString(2, message.getTxnUid());
                    ps.setShort(3, message.getPoint());
                    ps.setShort(4, message.getStatus());
                    ps.setString(5, message.getSessionUid());
                    ps.setString(6, message.getApplicationName());
                    ps.setString(7, message.getPrincipal());
                    ps.setString(8, truncString(message.getVdbName(), DEFAULT_MAX_VDB_NAME_LENGTH));
                    ps.setString(9, truncString(message.getVdbVersion(), DEFAULT_MAX_VDB_VERSION_LENGTH));
                    ps.setString(10, message.getBeginTimeStamp());
                    ps.setString(11, message.getEndTimeStamp());
                    if (message.getRowCount() < 0) {
                        ps.setNull(12, Types.INTEGER);
                    } else {
                        ps.setInt(12, message.getRowCount());
                    }
                    if (message.getSql() == null) {
                        ps.setLong(13, -1);
                        ps.executeUpdate();
                    } else {
                        long sqlId = getNextSqlID();
                        ps.setLong(13, sqlId);
                        ps.executeUpdate();
                        insertSqlValue(sqlId, message.getSql(), con);
                    }
                    break;
                case TransactionLogMessage.DEST_SRCCMDLOG:
                    ps = con.prepareStatement(SRC_COMMAND_LOG_STATEMENT);
                    ps.setString(1, message.getRequestId());
                    ps.setLong(2, message.getType());
                    ps.setString(3, message.getSubTxnUid());
                    ps.setShort(4, message.getStatus());
                    ps.setString(5, truncString(message.getModelName(), DEFAULT_MAX_MDL_NAME_LENGTH));
                    ps.setString(6, truncString(message.getConnectorBindingName(), DEFAULT_MAX_CNCTR_NAME_LENGTH));
                    ps.setShort(7, message.getPoint());
                    ps.setString(8, message.getSessionUid());
                    ps.setString(9, message.getPrincipal());
                    ps.setString(10, message.getBeginTimeStamp());
                    ps.setString(11, message.getEndTimeStamp());
                    if (message.getSql() == null) {
                        ps.setLong(12, -1);
                        ps.setInt(13, message.getRowCount());
                        ps.executeUpdate();
                    } else {
                        long sqlId = getNextSqlID();
                        ps.setLong(12, sqlId);
                        ps.setInt(13, message.getRowCount());
                        ps.executeUpdate();
                        insertSqlValue(sqlId, message.getSql(), con);
                    }
            }
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException ex) {
                LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, ServerPlugin.Util.getString("ERR.003.031.0037", ex.getMessage())); //$NON-NLS-1$
            }
    
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException ex) {
                LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, ServerPlugin.Util.getString("ERR.003.031.0038", ex.getMessage())); //$NON-NLS-1$
            }
        }

    }
    
    public void shutdown() {
        shutdown = true;
    }
    
    /**
     * Simple static method to tuncate Strings to given length.
     * 
     * @param in the string that may need tuncating.
     * @param len the lenght that the string should be truncated to.
     * @return a new String containing chars with length <= len or
     *         <code>null</code> if input String is <code>null</code>.
     */
    public static String truncString(String in, int len) {
        if (in != null && in.length() > len) {
            in = in.substring(0, len);
        }
        return in;
    }

    private long getNextSqlID() throws SQLException {
        try {
            return DBIDGenerator.getInstance().getID("TX_SQL"); //$NON-NLS-1$
        } catch (DBIDGeneratorException e) {
            throw new SQLException(ServerPlugin.Util.getString("ERR.003.031.0039")); //$NON-NLS-1$
        }
    }

    private void insertSqlValue(long sqlID, String sqlValue, Connection con) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(SQL_LOG_STATEMENT);
            ps.setLong(1, sqlID);
            ps.setString(2, sqlValue);
            if (ps.executeUpdate() != 1) {
                throw new SQLException(ServerPlugin.Util.getString("ERR.003.031.0040")); //$NON-NLS-1$
            }
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException ex) {
                LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, ServerPlugin.Util.getString("ERR.003.031.0037", ex.getMessage())); //$NON-NLS-1$
            }
        }
    }

}