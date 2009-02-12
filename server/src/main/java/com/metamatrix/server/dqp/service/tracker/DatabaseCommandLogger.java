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

/*
 */
package com.metamatrix.server.dqp.service.tracker;

import java.util.Properties;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.dqp.spi.CommandLoggerSPI;
import com.metamatrix.dqp.spi.TrackerLogConstants;
import com.metamatrix.server.ServerPlugin;

/**
 */
public class DatabaseCommandLogger implements CommandLoggerSPI {
	
    private boolean isStopped = false;
    private TransactionLogWriter writer;

    /**
     * Utility method to return whether the log manager for this VM is currently stopped.
     * @return true if the log manager is currently stopped.
     */
    public boolean isStopped() {
        return isStopped;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) {
        this.writer = new TransactionLogWriter(props);
    }

	@Override
	public void close() {
        LogManager.logInfo(LogCommonConstants.CTX_TXN_LOG, ServerPlugin.Util.getString("MSG.003.031.0006")); //$NON-NLS-1$
        isStopped = true;
        this.writer.shutdown();
	}

	@Override
	public void dataSourceCommandEnd(long timestamp, String requestID,
			long sourceCommandID, String subTransactionID, String modelName,
			String connectorBindingName, String sessionID, String principal,
			int finalRowCount, boolean isCancelled, boolean errorOccurred,
			ExecutionContext context) {
		short status = TrackerLogConstants.CMD_POINT.END;
		if (isCancelled) {
			status = TrackerLogConstants.CMD_STATUS.CANCEL;
		} else if (errorOccurred) {
			status = TrackerLogConstants.CMD_STATUS.ERROR;
		}
		writer.print(new TransactionLogMessage(requestID, sourceCommandID,
				subTransactionID, status,
				modelName, connectorBindingName,
				TrackerLogConstants.CMD_POINT.END, sessionID, principal, null,
				finalRowCount, timestamp));
	}

	@Override
	public void dataSourceCommandStart(long timestamp, String requestID,
			long sourceCommandID, String subTransactionID, String modelName,
			String connectorBindingName, String sessionID, String principal,
			String sql, ExecutionContext context) {
		writer.print(new TransactionLogMessage(requestID, sourceCommandID,
				subTransactionID, TrackerLogConstants.CMD_STATUS.NEW,
				modelName, connectorBindingName,
				TrackerLogConstants.CMD_POINT.BEGIN, sessionID, principal, sql,
				-1, timestamp));
	}

	@Override
	public void userCommandEnd(long timestamp, String requestID,
			String transactionID, String sessionID, String principal,
			String vdbName, String vdbVersion, int finalRowCount,
			boolean isCancelled, boolean errorOccurred) {
		short status = TrackerLogConstants.CMD_POINT.END;
		if (isCancelled) {
			status = TrackerLogConstants.CMD_STATUS.CANCEL;
		} else if (errorOccurred) {
			status = TrackerLogConstants.CMD_STATUS.ERROR;
		}
		writer.print(new TransactionLogMessage(requestID, transactionID,
						TrackerLogConstants.CMD_POINT.END, status, sessionID,
						null, principal, vdbName, vdbVersion, null,
						finalRowCount, timestamp));
	}

	@Override
	public void userCommandStart(long timestamp, String requestID,
			String transactionID, String sessionID, String applicationName,
			String principal, String vdbName, String vdbVersion, String sql) {
		writer.print(new TransactionLogMessage(requestID, transactionID,
						TrackerLogConstants.CMD_POINT.BEGIN,
						TrackerLogConstants.CMD_STATUS.NEW, sessionID,
						applicationName, principal, vdbName, vdbVersion, sql,
						-1, timestamp));
	}

}
