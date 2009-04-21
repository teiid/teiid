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

package com.metamatrix.dqp.service;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.spi.CommandLoggerSPI;
import com.metamatrix.dqp.spi.TrackerLogConstants;


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
    
    private CommandLoggerSPI commandLogger;
    private boolean recordUserCommands;
    private boolean recordSourceCommands;
    
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
                    String sql,
                    int rowCount) {

        if (this.willRecordMMCmd()) {
            CustomizableTrackingMessage message = null;
            if (cmdPoint == TrackerLogConstants.CMD_POINT.BEGIN) {
                message = new CustomizableTrackingMessage(System.currentTimeMillis(), requestId, txnUid, sessionUid, applicationName, principal, vdbName, vdbVersion, sql);
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
                    String sql,
                    int rowCount,
                    ExecutionContext context) {
        
        if (this.willRecordSrcCmd()) {
            CustomizableTrackingMessage message = null;
            if (cmdPoint == TrackerLogConstants.CMD_POINT.BEGIN) {
                message = new CustomizableTrackingMessage(System.currentTimeMillis(), requestId, nodeID, subTxnUid, modelName, cbName, sessionUid, principal, sql, context);

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
        String commandLoggerClassnameProperty = props.getProperty(DQPConfigSource.COMMAND_LOGGER_CLASSNAME);

		// Search for additional, implementation-specific properties stuff into
		// this string.
		// They should be delimited by semi-colon - TODO clean this up - sbale 5
		// /3/05
		//
		// Possible examples of expected value of commandLoggerClassnameProperty
		// String variable:
		//
		// com.metamatrix.dqp.spi.basic.FileCommandLogger;dqp.commandLogger.
		// fileName=commandLogFile.txt
		// com.myCode.MyCommandLoggerClass;myFirstCustomProperty=someValue;
		// mySecondCustomProperty=otherValue

		List tokens = StringUtil.getTokens(commandLoggerClassnameProperty, ";"); //$NON-NLS-1$

		// 1st token is the classname property
		String commandLoggerClassname = (String) tokens.remove(0);

		// Additional tokens are name/value pairs, properties specific to
		// service provider impl
		props = new Properties(props);
		Iterator i = tokens.iterator();
		while (i.hasNext()) {
			String nameValueString = (String) i.next();
			List nameValuePair = StringUtil.getTokens(nameValueString, "="); //$NON-NLS-1$
			String name = (String) nameValuePair.get(0);
			String value = (String) nameValuePair.get(1);
			props.setProperty(name, value);
		}
        
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            CommandLoggerSPI logger = (CommandLoggerSPI)loader.loadClass(commandLoggerClassname).newInstance();
            logger.initialize(props);
            this.commandLogger = logger;
        } catch (Exception e) {
            throw new ApplicationInitializationException(e);
        }
        
        String propvalue = props.getProperty(SYSTEM_TXN_STORE_MMCMD);
        if(propvalue != null){
            recordUserCommands = Boolean.valueOf(propvalue).booleanValue();
        }
        propvalue = props.getProperty(SYSTEM_TXN_STORE_SRCCMD);
        if(propvalue != null){
            recordSourceCommands = Boolean.valueOf(propvalue).booleanValue();
        }
    }
    
    /** 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
        logQueue = WorkerPoolFactory.newWorkerPool("CustomTracker", //$NON-NLS-1$
                                  1);
    }

    /** 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
        if (logQueue != null) {
            logQueue.shutdown();
            try {
				logQueue.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
            logQueue = null;
        }
        if (commandLogger != null) {
        	this.commandLogger.close();
        }
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

	public CommandLoggerSPI getCommandLogger() {
		return commandLogger;
	}
}
