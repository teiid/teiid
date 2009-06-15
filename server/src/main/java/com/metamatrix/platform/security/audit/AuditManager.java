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

package com.metamatrix.platform.security.audit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.dqp.service.AuditMessage;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.audit.config.AuditConfigurationException;
import com.metamatrix.platform.security.audit.config.AuditConfigurationFactory;
import com.metamatrix.platform.security.audit.config.BasicAuditConfiguration;
import com.metamatrix.platform.security.audit.config.CurrentConfigAuditConfigurationFactory;
import com.metamatrix.platform.security.audit.config.UnmodifiableAuditConfiguration;
import com.metamatrix.platform.security.audit.destination.AuditDestination;
import com.metamatrix.platform.security.audit.destination.AuditDestinationInitFailedException;
import com.metamatrix.platform.security.audit.destination.ConsoleAuditDestination;
import com.metamatrix.platform.security.audit.destination.DatabaseAuditDestination;
import com.metamatrix.platform.security.audit.destination.SingleFileAuditDestination;
import com.metamatrix.platform.security.util.LogSecurityConstants;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;

/**
 * This class represents the interface to a single auditing framework
 * that is easily accessible by any component.  Using the AuditManager, a component
 * can quickly submit a log message, and can rely upon the AuditManager to determine
 * (a) whether that message is to be recorded or discarded; and (b) where
 * to send any recorded messages.  Thus, the component's code that submits
 * messages does not have to be modified to alter the logging behavior of the
 * application.
 * <p>
 * The AuditManager has a number of features that makes it an efficient and configurable
 * framework.  First, the methods in the AuditManager that submit messages are
 * asynchronous to minimize the amount of time a client component waits for
 * the AuditManager.  Within these asynchronous methods, the AuditManager simply
 * checks the current auditing level and, if the message level is being recorded,
 * places the submitted message in a queue and returns; one or more workers
 * in separate threads pull sumbitted messages out of the queue
 * and process them.  During processing, the contexts of each message is examined;
 * any message that is not to be recorded is simply discarded, while those that
 * are to be recorded are sent to each of the destinations.
 * <p>
 * Secondly, the AuditManager's behavior can be controlled both at VM start time
 * (through current Server configuration properties) and during execution (through method invocation).
 * The destinations of the AuditManager must be configured at the start time of the VM,
 * (i.e., through the current configuration properties) but the control parameters
 * (i.e., the auditing level and the contexts, see below)
 * are initially defined using the current configuration properties at start time
 * and optionally during normal execution via method invocations.
 * <p>
 * By default, all context(s) are logged by the AuditManager.   The messages that
 * the AuditManager actually records and sends to the destinations
 * can be controlled using two different and orthogonal parameters.
 * The first is a message <i>level</i> that filters messages based upon detail,
 * and the second is a message <i>context</i> that filters messages based upon
 * origin.  The AuditManager tracks only those context(s) that should NOT be
 * logged.  Only if a message (which also is defined with these two parameters)
 * passes both filters will it be sent to the destinations.
 * <p>
 * Each message is submitted with one of the following levels (determined
 * by the particular method used to submit the message), sorted from the
 * least detailed to the greatest:
 * <li><b>None</b>:  No audit messages are recorded.
 * <li><b>Full</b>:  All audit messages are recorded.
 * <p>
 * The context for a message is any application-specified String.  Again, only
 * those message contexts that match those in the AuditManager's configuration will
 * be sent to the destinations.
 *
 */
public final class AuditManager {


    /**
     * The name of the configuration property that contains the message level for the AuditManager.
     * This is an optional property that defaults to '0'.
     */
    public static final String SYSTEM_AUDIT_LEVEL_PROPERTY_NAME   = "metamatrix.audit.enabled"; //$NON-NLS-1$

    /**
     * The name of the configuration property that contains 'true' if the log messages
     * are to be sent to System.out, or 'false' otherwise.  This is an optional
     * property that defaults to 'true'.  Note, however, that if the message
     * level for the logger is specified to be something other than NONE but
     * no file destination is specified, the value for this propery is
     * always assumed to be 'true'.
     */
    public static final String SYSTEM_AUDIT_CONSOLE_PROPERTY_NAME = "metamatrix.audit.console"; //$NON-NLS-1$



    protected static final String DEFAULT_AUDIT_MAX_THREADS          = "1"; //$NON-NLS-1$
    protected static final String DEFAULT_AUDIT_THREAD_TTL           = "600000"; //$NON-NLS-1$

    private static AuditConfiguration configuration = null;
    private List auditDestinations = new ArrayList();
	private WorkerPool workerPool;

	private static AuditManager INSTANCE;
	
	public static synchronized AuditManager getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new AuditManager();
		}
		return INSTANCE;
	}
	
    AuditManager() {
    	
    	// Log the beginning of the initialization
        LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0001));

        Properties currentConfigProperties = new Properties();
        Properties globalProperties = CurrentConfiguration.getInstance().getProperties();

        currentConfigProperties.putAll(globalProperties);

        Properties auditProperties = PropertiesUtils.clone(currentConfigProperties,System.getProperties(),true,false);
        auditProperties.setProperty(SingleFileAuditDestination.APPEND_PROPERTY_NAME,Boolean.TRUE.toString());
        
        AuditConfigurationFactory configFactory = new CurrentConfigAuditConfigurationFactory();
        try {
            configuration = configFactory.getConfiguration(auditProperties );
        } catch ( AuditConfigurationException e ) {
            LogManager.logWarning(LogSecurityConstants.CTX_AUDIT, e, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0004));
            configuration = new BasicAuditConfiguration();
        }
        
        configuration = new UnmodifiableAuditConfiguration(configuration);        // Initialize the message destinations ...
        if ( configuration.getAuditLevel() != AuditLevel.NONE ) {
            this.initializeDestinations(auditProperties);
        }

        // Log the destinations for the log messages ...
        LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0008, configuration.toString()));
        Iterator iter = this.auditDestinations.iterator();
        StringBuffer dests = new StringBuffer();
        while ( iter.hasNext() ) {
            AuditDestination dest = (AuditDestination) iter.next();
            dests.append(dest.getDescription());
            dests.append(", "); //$NON-NLS-1$
        }
        // Chop last ", "
        if ( dests.length() >= 2 ) {
            dests.setLength(dests.length() - 2);
        }
        LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0002, dests.toString()));

        // Initialize the queue workers ...
        this.initializeQueueWorkers();
        LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0003));
    }


    private void initializeDestinations(Properties auditProperties) {
        this.auditDestinations.clear();

        // Create and init the file destinations ...
        String specifiedLogFileName = auditProperties.getProperty(SingleFileAuditDestination.FILE_NAME_PROPERTY_NAME);
        if ( specifiedLogFileName != null && specifiedLogFileName.trim().length() != 0 ) {
            SingleFileAuditDestination destination = new SingleFileAuditDestination();
            try {
                destination.initialize(auditProperties);
                this.auditDestinations.add(destination);
                LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0004, destination.getDescription()));
            } catch( AuditDestinationInitFailedException e ) {
            	LogManager.logError(LogSecurityConstants.CTX_AUDIT, e, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0006, destination.getDescription()));
            }
        }

        // Create and init the database destination, if enabled ...
        boolean dbEnabled = Boolean.valueOf(auditProperties.getProperty(DatabaseAuditDestination.DATABASE_PROPERTY_NAME)).booleanValue();
        if ( dbEnabled ) {
            DatabaseAuditDestination destination = new DatabaseAuditDestination();
            try {
                destination.initialize(auditProperties);
                this.auditDestinations.add(destination);
                LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0004, destination.getDescription()));
            } catch( AuditDestinationInitFailedException e ) {
                LogManager.logError(LogSecurityConstants.CTX_AUDIT, e, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0006, destination.getDescription()));
            }
        } else {
        	LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0005));
        }

        // Create the console destinations ...
        boolean includeConsole = Boolean.valueOf(auditProperties.getProperty(SYSTEM_AUDIT_CONSOLE_PROPERTY_NAME) ).booleanValue();
        if ( includeConsole || this.auditDestinations.size() == 0 ) {
            ConsoleAuditDestination destination = new ConsoleAuditDestination();
            try {
                destination.initialize(auditProperties);
                this.auditDestinations.add(destination);
                LogManager.logInfo(LogSecurityConstants.CTX_AUDIT,
                        PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0004, destination.getDescription()));
            } catch( AuditDestinationInitFailedException e ) {
            	LogManager.logError(LogSecurityConstants.CTX_AUDIT, e, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0006, destination.getDescription()));
            }
        }

    }

    private void initializeQueueWorkers() {
        try {
            this.workerPool = WorkerPoolFactory.newWorkerPool("AuditQueue",1);//$NON-NLS-1$
        } catch ( Exception e ) {
            LogManager.logError(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0007, e));
        }
    }

    /**
     * Send a critical message to the log.  This level of message is generally
     * used to record an event or error that must be recorded (if any logging
     * is used).  If it is used to record an error, it generally means that the
     * system encountered a critical error which affects the integrity, accuracy,
     * reliability and/or capability of the system.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param principal the principal attempting access to the given resources.
     * @param permissions A collection of <code>AuthorizationPermission</code>s
     * that contain resources the given principal wishes to access.
     */
    public void record(String context, String activity, String principal, Collection permissions) {
        if (permissions != null && ! permissions.isEmpty()) {
            if ( this.isLevelDiscarded( AuditLevel.FULL ) ) {
                return;
            }
            List resources = new ArrayList(permissions.size());
            Iterator permItr = permissions.iterator();
            while ( permItr.hasNext() ) {
                resources.add(((AuthorizationPermission)permItr.next()).getResourceName());
            }

            AuditMessage msg = new AuditMessage( context, activity, principal, resources.toArray());
            try {
                addMessageToQueue(msg);
            } catch ( Exception e2 ) {
            	LogManager.logError(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0010, e2));
            }
        }
    }

    /**
     * Send a critical message to the log.  This level of message is generally
     * used to record an event or error that must be recorded (if any logging
     * is used).  If it is used to record an error, it generally means that the
     * system encountered a critical error which affects the integrity, accuracy,
     * reliability and/or capability of the system.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param activity the activity the given principal is attempting to perform on
     * the given resources.
     * @param principal the principal attempting access to the given resources.
     * @param resources the resources that the given proncipal is attempting to access.
     */
    public void record(String context, String activity, String principal, Object[] resources) {
        if (resources != null) {
            if ( this.isLevelDiscarded( AuditLevel.FULL ) ) {
                return;
            }

            AuditMessage msg = new AuditMessage( context, activity, principal, resources);
            try {
                addMessageToQueue(msg);
            } catch ( Exception e2 ) {
            	LogManager.logError(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0010, e2));
            }
        }
    }

    /**
     * Send a critical message to the log.  This level of message is generally
     * used to record an event or error that must be recorded (if any logging
     * is used).  If it is used to record an error, it generally means that the
     * system encountered a critical error which affects the integrity, accuracy,
     * reliability and/or capability of the system.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param activity the activity the given principal is attempting to perform on
     * the given resources.
     * @param principal the principal attempting access to the given resources.
     * @param resource the resource that the given proncipal is attempting to access.
     */
    public void record(String context, String activity, String principal, String resource) {
        if (resource != null) {
            if ( this.isLevelDiscarded( AuditLevel.FULL ) ) {
                return;
            }

            AuditMessage msg = new AuditMessage( context, activity, principal, new Object[]{resource});
            try {
                addMessageToQueue(msg);
            } catch ( Exception e2 ) {
            	LogManager.logError(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0010, e2));
            }       
        }
    }

    private boolean isLevelDiscarded( int msgLevel ) {
        return configuration.isLevelDiscarded(msgLevel);
    }

    private boolean isContextDiscarded( String context ) {
        return configuration.isContextDiscarded(context);
    }

    /**
     * Utility method to stop (permanently or temporarily) the audit manager for
     * this VM.  This method should be called when messages to the AuditManager are
     * to be prevented, but to wait until all messages already in the AuditManager
     * are processed.  Note that this method does block until all messages
     * are processed and until all destinations are closed.
     * <p>
     * This method is designed to be called by an application that wishes to
     * exit gracefully yet have all messages sent to the audit destinations.
     */
    public synchronized void stop() {
        LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0006));
        try {
            if (this.workerPool != null ) {
                this.workerPool.shutdown();
            }

            // Sleep for another 1 second to allow the worker threads
            // to finish processing the last messages ...
            this.workerPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
            Iterator iter = this.auditDestinations.iterator();
            while(iter.hasNext()) {
                AuditDestination dest = (AuditDestination) iter.next();
                dest.shutdown();
            }
            this.auditDestinations.clear();
        } catch (Exception e) {
            LogManager.logError(LogSecurityConstants.CTX_AUDIT, e,PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_AUDIT_0008));
        }
    }

    /**
     * Utility method to obtain the current log configuration for the AuditManager.
     * @return the current log configuration
     */
    public AuditConfiguration getAuditConfiguration() {
        return configuration;
    }

    public void setAuditConfiguration( AuditConfiguration config ) {
        if ( config != null ) {
            LogManager.logInfo(LogSecurityConstants.CTX_AUDIT, PlatformPlugin.Util.getString(LogMessageKeys.SEC_AUDIT_0008, config));
            if ( config instanceof UnmodifiableAuditConfiguration ) {
                UnmodifiableAuditConfiguration unmodConfig = (UnmodifiableAuditConfiguration) config;
                configuration = (AuditConfiguration) unmodConfig.deepClone();
            } else {
                configuration = new UnmodifiableAuditConfiguration( (AuditConfiguration) config.clone() );
            }
        }
    }

    /**
     * Utility method to identify whether a audit message with the specified
     * context will be recorded in the AuditManager's destinations.
     * @param context the context of the message
     * @return true if the message would be recorded if sent to the AuditManager,
     * or false if it would be discarded by the AuditManager.
     */
    public boolean isMessageToBeRecorded(String context) {
        if ( context == null ) {
            return false;
        }

        // If the messsage's level is greater than the logging level,
        // then the message should NOT be recorded ...
        if ( isLevelDiscarded(AuditLevel.FULL) ) {
            return false;
        }

        // If the set contains the message's context and the msgLevel is
        // not withing the requested recording levell, then do not log
        if ( isContextDiscarded( context ) ) {
            return false;
        }
        return true;
    }

    /**
     * Utility method to identify whether a log message will be recorded
     * in the AuditManager's destinations.
     * @param message the message
     * @return true if the message would be recorded if sent to the AuditManager,
     * or false if it would be discarded by the AuditManager.
     */
    public boolean isMessageToBeRecorded(AuditMessage message) {
        if ( message == null ) {
            return false;
        }

        if ( isLevelDiscarded(AuditLevel.FULL) ) {
            return false;
        }

        if ( isContextDiscarded( message.getContext() ) ) {
            return false;
        }
        return true;
    }

    /**
     * Helper method to add a message to the queue
     * @param msg Message for the queue
     */
    private void addMessageToQueue(final AuditMessage msg) {
        // Everything is started, add the normal way
        this.workerPool.execute(new Runnable() {
        	public void run() {
        		AuditManager.this.distributeMessage(msg);
        	}
        });
    }

    /**
     * Send message to all registered AuditDestinations (according to properties).
     * @param message Formatted message
     */
    void distributeMessage(AuditMessage message) {
        if ( ! this.isContextDiscarded(message.getContext()) ) {
            Iterator iter = auditDestinations.iterator();
            while(iter.hasNext()) {
                AuditDestination dest = (AuditDestination) iter.next();
                dest.record(message);
            }
        }
    }

}

