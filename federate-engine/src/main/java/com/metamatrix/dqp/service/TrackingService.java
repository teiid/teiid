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

/*
 */
package com.metamatrix.dqp.service;

import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.query.sql.lang.Command;

/**
 * This service is used to log transactions and commands,
 * as well as store profiling data.
 */
public interface TrackingService extends ApplicationService {
    /**
     * Log the command into database if the vaule of the property
     * "metamatrix.transaction.log.storeMMCMD" is "true".
     * @param requestId Unique command ID.
     * @param txnUid Unique transaction ID.
     * @param cmdPoint Point in command being logged - 
     * TransactionLogConstants.POINT.BEGIN, or TransactionLogConstants.POINT.END.
     * @param sessionUid Session ID.
     * @param applicationName name of the user application
     * @param principal User name.
     * @param vdbName VDB name.
     * @param vdbVersion VDB version.
     * @param sql SQL for the command.
     * @param rowCount Final row count.
     */
    public void log(String requestId, String txnUid, short cmdPoint, short status,
            String sessionUid, String applicationName, String principal, String vdbName, String vdbVersion, Command sql, int rowCount);

    /**
     * Log the command into database if the vaule of the property
     * "metamatrix.transaction.log.storeSRCCMD" is "true".
     * @param requestId Unique command ID.
     * @param nodeID Subcommand ID
     * @param subTxnUid Unique subtransaction ID.
     * @param status Type of request -
     * TransactionLogConstants.SRCCMD_STATUS.NEW, or TransactionLogConstants.SRCCMD_STATUS.CANCEL, 
     * TransactionLogConstants.SRCCMD_STATUS.END, or TransactionLogConstants.SRCCMD_STATUS.ERROR.
     * @param modelName Name of model.
     * @param cbName Connector binding name.
     * @param cmdPoint Point in command being logged - 
     * TransactionLogConstants.POINT.BEGIN, or TransactionLogConstants.POINT.END.
     * @param sessionUid Session ID.
     * @param principal User name.
     * @param sql SQL for the command.
     * @param rowCount Final row count.
     */
    public void log(String requestId, long nodeID, String subTxnUid, 
            short status, String modelName, String cbName, short cmdPoint, 
            String sessionUid, String principal, Command sql, int rowCount, 
            ExecutionContext context);

    /**
     * Returns whether the tracker will record MM commands. 
     * @param txnPoint
     * @return
     * @since 4.2
     */
    public boolean willRecordMMCmd();

    /**
     * Returns whether the tracker will record source commands. 
     * @param txnPoint
     * @return
     * @since 4.2
     */
    public boolean willRecordSrcCmd();


}
