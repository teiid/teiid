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
package com.metamatrix.server.dqp.service;

import java.util.Properties;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.dqp.service.TrackingService;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.server.dqp.service.tracker.TransactionLogManager;

/**
 */
public class DatabaseTrackingService implements TrackingService {

    /* (non-Javadoc)
     * @see com.metamatrix.dqp.service.TrackingService#log(java.lang.String, short, short, long, java.lang.String, java.lang.String, java.lang.String)
     */
    public void log(
        String txnUid,
        short txnPoint,
        short status,
        String sessionUid,
        String principal,
        String vdbName,
        String vdbVersion) {
        TransactionLogManager.log(txnUid, txnPoint, status, sessionUid, principal, vdbName, vdbVersion);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.dqp.service.TrackingService#log(long, java.lang.String, short, long, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public void log(
        String requestId,
        String txnUid,
        short cmdPoint,
		short status,
        String sessionUid,
        String applicationName,
        String principal,
        String vdbName,
        String vdbVersion,
        Command sql,
        int rowCount) {
        TransactionLogManager.log(requestId, txnUid, cmdPoint, status, sessionUid, applicationName, principal, vdbName, vdbVersion, sql, rowCount);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.dqp.service.TrackingService#log(long, long, java.lang.String, short, java.lang.String, java.lang.String, short, long, java.lang.String, java.lang.String, int)
     */
    public void log(
        String requestId,
        long nodeID,
        String subTxnUid,
        short status,
        String modelName,
        String cbName,
        short cmdPoint,
        String sessionUid,
        String principal,
        Command sql,
        int rowCount,
        ExecutionContext context) {
        TransactionLogManager.log(requestId, nodeID, subTxnUid, status, modelName, cbName, cmdPoint, sessionUid, principal, sql, rowCount);
    }

    public boolean willRecordTxn() {
        return TransactionLogManager.isTxnLogged();
    }
    
    public boolean willRecordMMCmd() {
        return TransactionLogManager.isMMCmdLogged();
    }
    public boolean willRecordSrcCmd() {
        return TransactionLogManager.isSrcCmdLogged();
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#bind()
     */
    public void bind() throws ApplicationLifecycleException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     */
    public void unbind() throws ApplicationLifecycleException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        // TODO Auto-generated method stub

    }

}
