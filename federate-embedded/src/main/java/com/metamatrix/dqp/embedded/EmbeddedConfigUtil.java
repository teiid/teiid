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

package com.metamatrix.dqp.embedded;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.log.config.BasicLogConfiguration;
import com.metamatrix.core.log.FileLimitSizeLogWriter;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.NullLogWriter;
import com.metamatrix.core.log.SystemLogWriter;
import com.metamatrix.internal.core.log.PlatformLog;

/**
 * DQPConfigUtil
 */
public class EmbeddedConfigUtil {
    static final String STDOUT = "STDOUT"; //$NON-NLS-1$
    private static Object lock = new Object();
    private static int count = 0;
    
    static {
        System.setProperty("shutdownHookInstalled", String.valueOf(Boolean.TRUE)); //$NON-NLS-1$ 
    }

    private EmbeddedConfigUtil() {
    }
    
    /**
     * Set the log level to tehe specified level 
     * @param logLevel the new log level
     * @throws MetaMatrixComponentException if the logLevel String cannot be parsed as an integer
     * @since 4.3
     */
    public static void setLogLevel(String logLevel) throws MetaMatrixComponentException {
        // Set up log level (default to none)
        int level = 0;
        if(logLevel != null && logLevel.trim().length() > 0) {
            try {
                level = Integer.parseInt(logLevel);                        
            } catch(NumberFormatException e) {
                throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("DQPComponent.Unable_to_parse_level") + logLevel);      //$NON-NLS-1$
            }            
        }
        LogConfiguration config = LogManager.getLogConfiguration(true);
        config.setMessageLevel(level);
        LogManager.setLogConfiguration(config);
        LogManager.logInfo("DQP", "LogManager configured with level = " + level); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Replaces the old set of discarded contexts with those provided.
     * @param contexts the new set of contexts to be discarded. 
     * @since 4.3
     */
    public static void setDiscardedContexts(Collection contexts) {
        LogManager.setLogConfiguration(new BasicLogConfiguration(contexts, LogManager.getLogConfiguration(false).getMessageLevel()));
        LogManager.logInfo("DQP", "LogManager discarded contexts " + contexts); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Installs a new log listener for the DQP, if a log listener of the same type does not already exist. 
     * @param newListener The LogListener to be installed
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public static void installLogListener(LogListener newListener) throws MetaMatrixComponentException {
        if (newListener == null) {
            // Make no changes to the current listeners
            return;
        }
           
        PlatformLog log = PlatformLog.getInstance();
        List listeners = new ArrayList(log.getLogListeners());
        if (listeners.size() == 1 && listeners.get(0).getClass().equals(newListener.getClass())) {
            // if the platform log writer is already established, dont do anything else
            return;
        }        
        
        // remove all existing listeners from the platform logger - we will add one back later if needed
        removeListeners(listeners);
        
        // Set up log listener
        log.addListener(newListener);
    }
    
    private static void removeListeners(List listeners) {
        // Walk through the listeners and remove them
        for(int i=0; i<listeners.size(); i++) {
            PlatformLog.getInstance().removeListener((LogListener)listeners.get(i));
        }
    }
    
    /**
     * Configures the PlatformLog to work correctly with the DQP.  The 
     * PlatformLog is configured to NOT log to System.out (as this is frowned upon
     * in a user's environment).  If a logFile and logLevel are specified, 
     * a file log is configured and LogManager is set up to pay attention to the 
     * log level.
     * 
     * @param logFile Log file name or null if no logging should occur
     * @param logLevel Log level or null for default level
     */
    public static void configureLogger(String logFile, String logLevel, boolean captureSystemStreams, boolean useSingleLogger) 
        throws MetaMatrixComponentException {
        
        PlatformLog log = PlatformLog.getInstance();
        List previousListeners = new ArrayList(log.getLogListeners());
        if (previousListeners.size() == 0 || !useSingleLogger) { 
            if(logFile != null && logFile.trim().length() > 0) {
                if (logFile.equalsIgnoreCase(STDOUT)) { 
                    SystemLogWriter logWriter = new SystemLogWriter();
                    installLogListener(logWriter);
                }
                else {
                    File file = new File(logFile);
                    FileLimitSizeLogWriter logWriter = new FileLimitSizeLogWriter(file, captureSystemStreams);
                    installLogListener(logWriter);
                }                
            } else {
                // Pass a safe copy of listeners to be removed.
                removeListeners(previousListeners);
                NullLogWriter logNull = new NullLogWriter();
                installLogListener(logNull);                
            }
            
            // now set the log level
            if(logLevel != null) {
                setLogLevel(logLevel);
            }else {
                setLogLevel("3"); //$NON-NLS-1$
            }            
        }
        // Increment counter to keep count of number of DQPs which are using this 
        // logger object. In the case of unifiedClassLoader we would like to have
        // single logger.
        synchronized(lock) {
            count++;
        }
    }

    /** 
     * 
     * @since 4.3
     */
    public static void shutdownLogger() {
        // decrement the counter and when it reaches zero then only shutdown the logger
        // becuase in the case of the unified class laoder we have only one logger and we
        // would like to close at the end of last dqp shutdown.
        synchronized(lock) {
            count--;
        }        

        if (count == 0) {
            PlatformLog.getInstance().shutdown(false);
            PlatformLog.getInstance().start();
            //LogManager.stop();
        }
    }    
}
