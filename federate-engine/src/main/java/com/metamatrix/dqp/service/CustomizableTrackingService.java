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

package com.metamatrix.dqp.service;

import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.spi.CommandLoggerSPI;
import com.metamatrix.dqp.spi.TrackerLogConstants;
import com.metamatrix.query.sql.lang.Command;


/** 
 * Tracking service implementation that defers to a {@link CommandLoggerSPI}
 * service provider.
 * 
 * A value for the {@link DQPServiceProperties.TrackingService#COMMAND_LOGGER_CLASSNAME}
 * must be supplied in the DQP properties in order to use this Tracking Service
 * implementation.
 */
public class CustomizableTrackingService implements TrackingService {
    
    /**
     * Whether to log transaction. Defaults to false.
     */
    public static final String SYSTEM_TXN_STORE_TXN   = "metamatrix.transaction.log.storeTXN"; //$NON-NLS-1$

    /**
     * Whether to log MetaMatrix command. Defaults to false.
     */
    public static final String SYSTEM_TXN_STORE_MMCMD   = "metamatrix.transaction.log.storeMMCMD"; //$NON-NLS-1$

    /**
     * Whether to log source command. Defaults to false.
     */
    public static final String SYSTEM_TXN_STORE_SRCCMD  = "metamatrix.transaction.log.storeSRCCMD"; //$NON-NLS-1$

    /**
     * The name of the System property that contains the time to live (in milliseconds) for threads
     * in the LogManager.  The time to live is simply the period of thread inactivity
     * that determines when a thread may be expired.  This is an optional property
     * that defaults to '600000' milliseconds (or 10 minutes).
     */
    public static final String SYSTEM_LOG_THREAD_TTL            = "metamatrix.transaction.log.threadTTL"; //$NON-NLS-1$
    
    protected static final String DEFAULT_LOG_THREAD_TTL           = "600000"; // 10 minute default //$NON-NLS-1$

    private CommandLoggerSPI commandLogger;
    private boolean recordUserCommands;
    private boolean recordSourceCommands;
    private long workerTTL;
    
    private WorkerPool logQueue;

    /** 
     * @see com.metamatrix.dqp.service.TrackingService#log(java.lang.String, java.lang.String, short, short, java.lang.String, java.lang.String, java.lang.String, java.lang.String, com.metamatrix.query.sql.lang.Command)
     */
    public void log(String requestId,
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

        if (this.willRecordMMCmd()) {
            CustomizableTrackingMessage message = null;
            if (cmdPoint == TrackerLogConstants.CMD_POINT.BEGIN) {
                message = new CustomizableTrackingMessage(System.currentTimeMillis(), requestId, txnUid, sessionUid, applicationName, principal, vdbName, vdbVersion, (sql!=null)?sql.toString():null);
            } else {
                boolean isCancelled = false;
                boolean errorOccurred = false;

                if (status == TrackerLogConstants.CMD_STATUS.CANCEL) {
                    isCancelled = true;
                } else if (status == TrackerLogConstants.CMD_STATUS.ERROR) {
                    errorOccurred = true;
                }
                message = new CustomizableTrackingMessage(System.currentTimeMillis(), requestId, txnUid, sessionUid, principal, vdbName, vdbVersion, rowCount, isCancelled, errorOccurred);
            }
            addWork(message);
        }
    }

    /** 
     * @see com.metamatrix.dqp.service.TrackingService#log(java.lang.String, long, java.lang.String, short, java.lang.String, java.lang.String, short, java.lang.String, java.lang.String, com.metamatrix.query.sql.lang.Command, int)
     */
    public void log(String requestId,
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
        
        if (this.willRecordSrcCmd()) {
            CustomizableTrackingMessage message = null;
            if (cmdPoint == TrackerLogConstants.CMD_POINT.BEGIN) {
                message = new CustomizableTrackingMessage(System.currentTimeMillis(), requestId, nodeID, subTxnUid, modelName, cbName, sessionUid, principal, (sql!=null)?sql.toString():null, context);

            } else {
                boolean isCancelled = false;
                boolean errorOccurred = false;

                if (status == TrackerLogConstants.CMD_STATUS.CANCEL) {
                    isCancelled = true;
                } else if (status == TrackerLogConstants.CMD_STATUS.ERROR) {
                    errorOccurred = true;
                }
                message = new CustomizableTrackingMessage(System.currentTimeMillis(), requestId, nodeID, subTxnUid, modelName, cbName, sessionUid, principal, rowCount, isCancelled, errorOccurred, context);
            }            
            addWork(message);
        }
    }

    /** 
     * @see com.metamatrix.dqp.service.TrackingService#willRecordMMCmd()
     */
    public boolean willRecordMMCmd() {
        return this.recordUserCommands;
    }

    /** 
     * @see com.metamatrix.dqp.service.TrackingService#willRecordSrcCmd()
     */
    public boolean willRecordSrcCmd() {
        return this.recordSourceCommands;
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
        String propvalue = props.getProperty(SYSTEM_TXN_STORE_MMCMD);
        if(propvalue != null){
            recordUserCommands = Boolean.valueOf(propvalue).booleanValue();
        }
        propvalue = props.getProperty(SYSTEM_TXN_STORE_SRCCMD);
        if(propvalue != null){
            recordSourceCommands = Boolean.valueOf(propvalue).booleanValue();
        }
        
        try {
            workerTTL = Long.parseLong(System.getProperty(SYSTEM_LOG_THREAD_TTL, DEFAULT_LOG_THREAD_TTL));
        } catch (NumberFormatException e) {
            throw new ApplicationInitializationException(e);
        }
        
        String commandLoggerClassname = props.getProperty(DQPServiceProperties.TrackingService.COMMAND_LOGGER_CLASSNAME);

        ClassLoader loader = this.getClass().getClassLoader();
        try {
            CommandLoggerSPI logger = (CommandLoggerSPI)loader.loadClass(commandLoggerClassname).newInstance();
            logger.initialize(props);
            this.commandLogger = logger;
        } catch (Exception e) {
            throw new ApplicationInitializationException(e);
        }
    }
    
    // TODO This method is used strictly for testing. Probably remove
    void initialize(CommandLoggerSPI commandLogger, boolean willRecordTransactions, 
                    boolean willRecordUserCommands, boolean willRecordSourceCommands) {
        
        this.commandLogger = commandLogger;
        this.recordUserCommands = willRecordUserCommands;
        this.recordSourceCommands = willRecordSourceCommands;
        this.workerTTL = Long.parseLong(DEFAULT_LOG_THREAD_TTL);
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        logQueue = WorkerPoolFactory.newWorkerPool("CustomTracker", //$NON-NLS-1$
                                  1,   // Use only a single thread
                                  workerTTL);
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#bind()
     */
    public void bind() throws ApplicationLifecycleException {
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     */
    public void unbind() throws ApplicationLifecycleException {
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        if (logQueue != null) {
            logQueue.shutdown();
            logQueue = null;
        }
        this.commandLogger.close();
    }
    
    private void addWork(CustomizableTrackingMessage work) {
        try {
            if (logQueue != null) {
            	work.commandLogger = this.commandLogger;
                logQueue.execute(work);
            } else {
                LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, DQPPlugin.Util.getString("CustomizableTrackingService.not_started")); //$NON-NLS-1$
            }
        } catch (RejectedExecutionException e) {
            LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, e.getMessage());
        }
    }
}
