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

package com.metamatrix.server.dqp.service.tracker;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.server.ServerPlugin;


/**
 * This class represents the interface to a single transaction logging framework
 * that is easily accessible by any component.  Using the TransactionLogManager, a component
 * can quickly submit a log message, and can rely upon the TransactionLogManager to determine
 * whether that message is to be recorded or discarded. Thus, the component's code that submits
 * messages does not have to be modified to alter the logging behavior of the
 * application.
 * <p>
 * The TransactionLogManager has a number of features that makes it an efficient and configurable
 * framework. First, the methods in the TransactionLogManager that submit messages are
 * asynchronous to minimize the amount of time a client component waits for
 * the TransactionLogManager.
 * <p>
 * Secondly, the TransactionLogManager's behavior can be controlled at VM start time
 * through current Server configuration properties.
 */
public final class TransactionLogManager {

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
     * The name of the System property that contains the maximum number of threads
     * for the LogManager.  This is an optional property that defaults to '1'.
     * Note that the maximum value must be greater than or equal to the minimum value.
     */
    public static final String SYSTEM_LOG_MAX_THREADS           = "metamatrix.transaction.log.maxThreads"; //$NON-NLS-1$

    /**
     * The name of the System property that contains the time to live (in milliseconds) for threads
     * in the LogManager.  The time to live is simply the period of thread inactivity
     * that determines when a thread may be expired.  This is an optional property
     * that defaults to '600000' milliseconds (or 10 minutes).
     */
    public static final String SYSTEM_LOG_THREAD_TTL            = "metamatrix.transaction.log.threadTTL"; //$NON-NLS-1$

    protected static final String DEFAULT_LOG_MAX_THREADS          = "1"; //$NON-NLS-1$
    protected static final String DEFAULT_LOG_THREAD_TTL           = "600000"; //$NON-NLS-1$

	private static TransactionLogManager INSTANCE = new TransactionLogManager();

	private boolean storeTXN;
	private boolean storeMMCMD;
	private boolean storeSRCCMD;

	private WorkerPool workerPool;
	private Properties currentConfigProperties;
    private boolean isInitialized = false;
    private boolean isStopped = false;

    protected void finalize() {
        if ( this.isManagerStopped() ) {
            stop();
        }
    }

    private TransactionLogManager() {
        // Initialize the worker factory and message queue ...
        // Doing this before anything else allows messages to be enqueue before
        // and before the AuditManager is alive and before the destinations are created ...
    }

    private void init() {
        //get system properties
        currentConfigProperties = new Properties();
        try {
            Properties globalProperties = CurrentConfiguration.getProperties();

            currentConfigProperties.putAll(globalProperties);
            currentConfigProperties.putAll(CurrentConfiguration.getResourceProperties(ResourceNames.TRANSACTION_LOGGING));
        } catch ( ConfigurationException e ) {
            LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, e, ServerPlugin.Util.getString("ERR.003.001.0027")); //$NON-NLS-1$
            currentConfigProperties.putAll(System.getProperties());
        }

        String propvalue = currentConfigProperties.getProperty(SYSTEM_TXN_STORE_TXN);
        if(propvalue != null){
            storeTXN = Boolean.valueOf(propvalue).booleanValue();
        }
        propvalue = currentConfigProperties.getProperty(SYSTEM_TXN_STORE_MMCMD);
        if(propvalue != null){
            storeMMCMD = Boolean.valueOf(propvalue).booleanValue();
        }
        propvalue = currentConfigProperties.getProperty(SYSTEM_TXN_STORE_SRCCMD);
        if(propvalue != null){
            storeSRCCMD = Boolean.valueOf(propvalue).booleanValue();
        }

        //if not logging anything, no need to really initialize anything
        if(!storeTXN && !storeMMCMD && !storeSRCCMD){
            this.isInitialized = true;
            return;
        }

        // Initialize the queue workers ...
        this.initializeQueueWorkers();

        this.isInitialized = true;
    }

    private void initializeQueueWorkers() {
        // Create the worker pool
        String maxThreadsString = System.getProperty(SYSTEM_LOG_MAX_THREADS, DEFAULT_LOG_MAX_THREADS);
        String threadTTLString = System.getProperty(SYSTEM_LOG_THREAD_TTL, DEFAULT_LOG_THREAD_TTL);
        this.workerPool = WorkerPoolFactory.newWorkerPool(
            "TransactionLog",  //$NON-NLS-1$
            Integer.parseInt(maxThreadsString),
            Integer.parseInt(threadTTLString));
    }

    private boolean isInitialized() {
        return this.isInitialized;
    }

    protected static synchronized TransactionLogManager getInstance() {
        if ( ! INSTANCE.isInitialized() ) {
            INSTANCE.init();
        }
        
        return INSTANCE;
    }

	/**
     * Utility method to stop (permanently or temporarily) the log manager for
     * this VM.  This method should be called when messages to the LogManager are
     * to be prevented, but to wait until all messages already in the LogManager
     * are processed.  Note that this method does block until all messages
     * are processed and until all destinations are closed.
     * <p>
     * This method is designed to be called by an application that wishes to
     * exit gracefully yet have all messages sent to the database logger.
     */
    public static void stop() {
        TransactionLogManager manager = TransactionLogManager.getInstance();
        LogManager.logInfo(LogCommonConstants.CTX_TXN_LOG, ServerPlugin.Util.getString("MSG.003.031.0006")); //$NON-NLS-1$

        manager.workerPool.shutdown();

        // Sleep for another 1 second to allow the worker threads
        // to finish processing the last messages ...
        try {
        	manager.workerPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException err) {
        }
        manager.isStopped = manager.workerPool.isTerminated();
    }

    /**
     * Utility method to return whether the log manager for this VM is currently stopped.
     * @return true if the log manager is currently stopped.
     */
    public static boolean isStopped() {
        return getInstance().isManagerStopped();
    }

    protected boolean isManagerStopped() {
        return this.isStopped;
    }

    /**
     * Helper method to add a message to the queue
     * @param msg Message for the queue
     */
    private void addMessageToQueue(TransactionLogMessage msg) {
        this.workerPool.execute(new TransactionLogWriter(this.currentConfigProperties, msg));
    }

    //=== The following methods should be used to log transactions into database ===//

    /**
     * Log the transaction into database if the vaule of the property
     * "metamatrix.transaction.log.storeTXN" is "true".
     * @param txnUid Unique transaction ID.
     * @param txnPoint Point in transaction being logged -
     * TransactionLogConstants.POINT.BEGIN, or TransactionLogConstants.POINT.END.
     * @param status Status of the transaction -
     * TransactionLogConstants.TXN_STATUS.BEGIN, TransactionLogConstants.STATUS.COMMIT, TransactionLogConstants.TXN_STATUS.ROLLBACK.
     * @param sessionUid Session ID.
     * @param principal User name.
     * @param vdbName VDB name.
     * @param vdbVersion VDB version.
     */
    public static void log(String txnUid, short txnPoint, short status,
            String sessionUid, String principal, String vdbName, String vdbVersion){

    	if(!getInstance().storeTXN){
    		return;
    	}

    	TransactionLogMessage logMsg = new TransactionLogMessage(txnUid, txnPoint, status, sessionUid, principal, vdbName, vdbVersion);
    	try {
            getInstance().addMessageToQueue(logMsg);
        } catch ( Exception e ) {
			LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, e, e.getMessage());
        }
    }

    /**
     * Log the command into database if the vaule of the property
     * "metamatrix.transaction.log.storeMMCMD" is "true".
     * @param requestId Unique command ID.
     * @param txnUid Unique transaction ID.
     * @param cmdPoint Point in command being logged -
     * TransactionLogConstants.POINT.BEGIN, or TransactionLogConstants.POINT.END.
     * @param sessionUid Session ID.
     * @param principal User name.
     * @param vdbName VDB name.
     * @param vdbVersion VDB version.
     * @param sql SQL for the command.
     */
    public static void log(String requestId, String txnUid, short cmdPoint, short status,
            String sessionUid, String applicationName, String principal, String vdbName,
            String vdbVersion, Object command, int rowCount){

		if(!getInstance().storeMMCMD){
    		return;
    	}

        String sqlString = null;
        if (command != null) {
            sqlString = command.toString();
        }
    	TransactionLogMessage logMsg = new TransactionLogMessage(requestId, txnUid, cmdPoint, status, sessionUid, applicationName, principal, vdbName, vdbVersion, sqlString, rowCount);
    	try {
            getInstance().addMessageToQueue(logMsg);
        } catch ( Exception e ) {
			LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, e, e.getMessage());
        }
	}

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
    public static void log(String requestId, long nodeID, String subTxnUid,
			short status, String modelName, String cbName, short cmdPoint,
        String sessionUid, String principal, Object sql, int rowCount){

		if(!getInstance().storeSRCCMD){
    		return;
    	}
        String sqlString = null;
        if (sql != null) {
            sqlString = sql.toString();
        }
    	TransactionLogMessage logMsg = new TransactionLogMessage(requestId, nodeID, subTxnUid, status, modelName, cbName, cmdPoint, sessionUid, principal, sqlString, rowCount);
    	try {
            getInstance().addMessageToQueue(logMsg);
        } catch ( Exception e ) {
            LogManager.logWarning(LogCommonConstants.CTX_TXN_LOG, e, e.getMessage());
        }
	}

    public static boolean isTxnLogged(){
        return getInstance().storeTXN;
    }

    public static boolean isMMCmdLogged(){
        return getInstance().storeMMCMD;
    }

    public static boolean isSrcCmdLogged(){
        return getInstance().storeSRCCMD;
    }

}
