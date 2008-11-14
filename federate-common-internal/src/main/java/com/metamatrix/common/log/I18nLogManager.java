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

package com.metamatrix.common.log;

import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.MessageLevel;



/**
 *
 * @deprecated Use <b><PluginName>Plugin.Util.log(Status, <PluginName>Plugin.Util.getString("An_externalized_string"))</b> instead.
 *
 * <p>This class represents the interface to a single logging framework
 * that is easily accessible by any component.  Using the I18NLogManager, a component
 * can quickly submit a log message, and can rely upon the I18NLogManager to determine
 * (a) whether that message is to be recorded or discarded; and (b) where
 * to send any recorded messages.  Thus, the component's code that submits
 * messages does not have to be modified to alter the logging behavior of the
 * application.</p>
 * <p>
 * The I18NLogManager has a number of features that makes it an efficient and configurable
 * framework.  First, the methods in the I18NLogManager that submit messages are
 * asynchronous to minimize the amount of time a client component waits for
 * the I18NLogManager.  Within these asynchronous methods, the I18NLogManager simply
 * checks the current logging level and, if the message level is being recorded,
 * places the submitted message in a queue and returns; one or more workers
 * in separate threads pull sumbitted messages out of the queue
 * and process them.  During processing, the contexts of each message is examined;
 * any message that is not to be recorded is simply discarded, while those that
 * are to be recorded are sent to each of the destinations.
 * <p>
 * Secondly, the I18NLogManager's behavior can be controlled both at VM start time
 * (through System properties) and during execution (through method invocation).
 * The destinations of the I18NLogManager must be configured at start time,
 * but the control parameters (i.e., the logging level and the contexts, see below)
 * are initially defined using the System properties at start time and optionally
 * during normal execution.
 * <p>
 * By default, all context(s) are logged by the I18NLogManager.   The messages that
 * the I18NLogManager actually records and sends to the destinations
 * can be controlled using two different and orthogonal parameters.
 * The first is a message <i>level</i> that filters messages based upon detail,
 * and the second is a message <i>context</i> that filters messages based upon
 * origin.  The I18NLogManager tracks only those context(s) that should NOT be
 * logged.  Only if a message (which also is defined with these two parameters)
 * passes both filters will it be sent to the destinations.
 * <p>
 * Each message is submitted with one of the following levels (determined
 * by the particular method used to submit the message), sorted from the
 * least detailed to the greatest:
 * <li><b>Critical</b>:  This level of message is generally
 *      used to record an event or error that must be recorded (if any logging
 *      is used).  If it is used to record an error, it generally means that the
 *      system encountered a critical error which affects the integrity, accuracy,
 *      reliability and/or capability of the system.</li>
 * <li><b>Error</b>:  Error messages are generally used
 *      to record unexpected problems, or errors that are not critical in nature
 *      and from which the system can automatically recover.</li>
 * <li><b>Warning</b>:  Warning messages generally described
 *      expected errors from which the system should recover.  However, this level
 *      is used to record the fact that such an error or event did occur.</li>
 * <li><b>Information</b>:  This level of logging is the usually
 *      the normal level.  All interesting periodic events should be logged at this
 *      level so someone looking through the log can see the amount and kind of
 *      processing happening in the system.</li>
 * <li><b>Detail</b>:  Such messages are moderately detailed,
 *      and help to debug typical problems in the system.  Generally, these
 *      messages are not so detailed that the big picture gets lost.</li>
 * <li><b>Trace</b>:  A trace message is the most detailed
 *      logging level, used to trace system execution for really nasty problems.
 *      At this level, logging will be so verbose that the system performance
 *      may be affected.</li>
 * <p>
 * The context for a message is any application-specified String.  Again, only
 * those message contexts that match those in the I18NLogManager's configuration will
 * be sent to the destinations.
 *
 */
public final class I18nLogManager {
    private static I18nLogManager INSTANCE = new I18nLogManager();

    private I18nLogManager() {
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
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgParts the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logCritical(String context, String msgID, Object[] msgParts) {
        if (msgParts != null) {
            INSTANCE.logMessage(MessageLevel.CRITICAL, msgID, context, msgParts);
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
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgPart the individual object part of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logCritical(String context, String msgID, Object msgPart) {
        if (msgPart != null) {
            INSTANCE.logMessage(MessageLevel.CRITICAL, msgID, context, new Object[] {msgPart});
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
     * @param msgID is the unique id that identifies the message in the resource bundles
     */
    public static void logCritical(String context, String msgID) {
            INSTANCE.logMessage(MessageLevel.CRITICAL, msgID, context);
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
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     */
    public static void logCritical(String context, String msgID, Throwable e) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.CRITICAL, msgID, context,e);
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
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgParts the individual parts of the log message (may be null)
     */
    public static void logCritical(String context, String msgID, Throwable e, Object[] msgParts) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.CRITICAL, msgID, context,e,msgParts);
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
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgPart the individual part of the log message (may be null)
     */
    public static void logCritical(String context, String msgID, Throwable e, Object msgPart) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.CRITICAL, msgID, context,e, new Object[] {msgPart});
        }
    }


    /**
     * Send an error message to the log.  Error messages are generally used
     * to record unexpected problems, or errors that are not critical in nature
     * and from which the system can automatically recover.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgParts the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logError(String context, String msgID, Object[] msgParts) {
        if (msgParts != null) {
            INSTANCE.logMessage(MessageLevel.ERROR, msgID, context,msgParts);
        }
    }

    /**
     * Send an error message to the log.  Error messages are generally used
     * to record unexpected problems, or errors that are not critical in nature
     * and from which the system can automatically recover.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgPart the individual object part of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logError(String context, String msgID, Object msgPart) {
        if (msgPart != null) {
            INSTANCE.logMessage(MessageLevel.ERROR, msgID, context, new Object[] {msgPart});
        }
    }


    /**
     * Send an error message to the log.  Error messages are generally used
     * to record unexpected problems, or errors that are not critical in nature
     * and from which the system can automatically recover.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     */
    public static void logError(String context, String msgID) {
            INSTANCE.logMessage(MessageLevel.ERROR, msgID, context);
    }

    /**
     * Send an error message to the log.  Error messages are generally used
     * to record unexpected problems, or errors that are not critical in nature
     * and from which the system can automatically recover.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     */
    public static void logError(String context, String msgID, Throwable e) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.ERROR, msgID, context,e);
        }
    }

    /**
     * Send an error message to the log.  Error messages are generally used
     * to record unexpected problems, or errors that are not critical in nature
     * and from which the system can automatically recover.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     */
    public static void logError(String context, Throwable e) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.ERROR, null, context,e);
        }
    }

    /**
     * Send an error message to the log.  Error messages are generally used
     * to record unexpected problems, or errors that are not critical in nature
     * and from which the system can automatically recover.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgParts the individual parts of the log message (may be null)
     */
    public static void logError(String context, String msgID, Throwable e, Object[] msgParts) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.ERROR, msgID, context,e,msgParts);
        }
    }

    /**
     * Send an error message to the log.  Error messages are generally used
     * to record unexpected problems, or errors that are not critical in nature
     * and from which the system can automatically recover.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgPart the individual part of the log message (may be null)
     */
    public static void logError(String context, String msgID, Throwable e, Object msgPart) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.ERROR, msgID, context,e, new Object[] {msgPart});
        }
    }


    /**
     * Send a warning message to the log.  Warning messages generally described
     * expected errors from which the system should recover.  However, this level
     * is used to record the fact that such an error or event did occur.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgParts the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logWarning(String context, String msgID, Object[] msgParts) {
        if (msgParts != null) {
            INSTANCE.logMessage(MessageLevel.WARNING, msgID, context,msgParts);
        }
    }

    /**
     * Send a warning message to the log.  Warning messages generally described
     * expected errors from which the system should recover.  However, this level
     * is used to record the fact that such an error or event did occur.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgPart the individual object part of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logWarning(String context, String msgID, Object msgPart) {
        if (msgPart != null) {
            INSTANCE.logMessage(MessageLevel.WARNING, msgID, context, new Object[] {msgPart});
        }
    }


    /**
     * Send a warning message to the log.  Warning messages generally described
     * expected errors from which the system should recover.  However, this level
     * is used to record the fact that such an error or event did occur.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     *
     */
    public static void logWarning(String context, String msgID) {
            INSTANCE.logMessage(MessageLevel.WARNING, msgID, context);
    }

    /**
     * Send a warning message to the log.  Warning messages generally described
     * expected errors from which the system should recover.  However, this level
     * is used to record the fact that such an error or event did occur.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     */
    public static void logWarning(String context, String msgID, Throwable e) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.WARNING, msgID, context,e);
        }
    }


    /**
     * Send a warning message to the log.  Warning messages generally described
     * expected errors from which the system should recover.  However, this level
     * is used to record the fact that such an error or event did occur.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgParts the individual parts of the log message (may be null)
     */
    public static void logWarning(String context, String msgID, Throwable e, Object[] msgParts) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.WARNING, msgID, context,e,msgParts);
        }
    }

    /**
     * Send a warning message to the log.  Warning messages generally described
     * expected errors from which the system should recover.  However, this level
     * is used to record the fact that such an error or event did occur.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgPart the individual part of the log message (may be null)
     */
    public static void logWarning(String context, String msgID, Throwable e, Object msgPart) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.WARNING, msgID, context,e, new Object[] {msgPart});
        }
    }

    /**
     * Send a information message to the log.  This level of logging is the usually
     * the normal level.  All interesting periodic events should be logged at this
     * level so someone looking through the log can see the amount and kind of
     * processing happening in the system.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgParts the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logInfo(String context, String msgID, Object[] msgParts) {
        if (msgParts != null) {
            INSTANCE.logMessage(MessageLevel.INFO, msgID, context,msgParts);
        }
    }

    /**
     * Send a information message to the log.  This level of logging is the usually
     * the normal level.  All interesting periodic events should be logged at this
     * level so someone looking through the log can see the amount and kind of
     * processing happening in the system.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgPart the individual object part of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logInfo(String context, String msgID, Object msgPart) {
        if (msgPart != null) {
            INSTANCE.logMessage(MessageLevel.INFO, msgID, context, new Object[] {msgPart});
        }
    }


    /**
     * Send a information message to the log.  This level of logging is the usually
     * the normal level.  All interesting periodic events should be logged at this
     * level so someone looking through the log can see the amount and kind of
     * processing happening in the system.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     */
    public static void logInfo(String context, String msgID) {
            INSTANCE.logMessage(MessageLevel.INFO, msgID, context);
    }

    /**
     * Send a information message to the log.  This level of logging is the usually
     * the normal level.  All interesting periodic events should be logged at this
     * level so someone looking through the log can see the amount and kind of
     * processing happening in the system.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgParts the individual parts of the log message (may be null)
     */
    public static void logInfo(String context, String msgID, Throwable e, Object[] msgParts) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.INFO,msgID,context,e,msgParts);
        }
    }


    /**
     * Send a detail message to the log.  Such messages are moderately detailed,
     * and help to debug typical problems in the system.  Generally, these
     * messages are not so detailed that the big picture gets lost.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgParts the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logDetail(String context, String msgID, Object[] msgParts) {
        if (msgParts != null) {
            INSTANCE.logMessage(MessageLevel.DETAIL, msgID, context, msgParts);
        }
    }

    /**
     * Send a detail message to the log.  Such messages are moderately detailed,
     * and help to debug typical problems in the system.  Generally, these
     * messages are not so detailed that the big picture gets lost.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     */
    public static void logDetail(String context, String msgID, Throwable e) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.DETAIL, msgID, context,e);
        }
    }


    /**
     * Send a detail message to the log.  Such messages are moderately detailed,
     * and help to debug typical problems in the system.  Generally, these
     * messages are not so detailed that the big picture gets lost.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgParts the individual parts of the log message (may be null)
     */
    public static void logDetail(String context, String msgID, Throwable e, Object[] msgParts) {
        if (e != null) {
            INSTANCE.logMessage(MessageLevel.DETAIL,msgID,context,e,msgParts);
        }
    }


    /**
     * Send a trace message to the log.  A trace message is the most detailed
     * logging level, used to trace system execution for really nasty problems.
     * At this level, logging will be so verbose that the system performance
     * may be affected.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param msgID is the unique id that identifies the message in the resource bundles
     * @param msgParts the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logTrace(String context, String msgID, Object[] msgParts) {
        if (msgParts != null) {
            INSTANCE.logMessage(MessageLevel.TRACE, msgID, context, msgParts);
        }
    }


    public static void setLogConfiguration( LogConfiguration config ) {
		LogManager.setLogConfiguration(config);
    }

    /**
     * Utility method to identify whether a log message with the specified
     * context and level will be recorded in the I18NLogManager's destinations.
     * @param message the message
     * @return true if the message would be recorded if sent to the I18NLogManager,
     * or false if it would be discarded by the I18NLogManager.
     */
    public static boolean isMessageToBeRecorded(String context, int msgLevel) {
    	return LogManager.isMessageToBeRecorded(context, msgLevel);
    }


    protected void logMessage(int level, String msgID, String context, Object[] msgParts) {
        // Check quickly the level of the message:
        // If the messsage's level is greater than the logging level,
        // then the message should NOT be recorded ...
        if (!LogManager.isMessageToBeRecorded(context, level)) {
        	return;
        }


        LogMessage msg = new LogMessage(msgID, context, level, msgParts);
        	LogManager.getInstance().forwardMessage(msg);
//            System.out.println("Enqueuing message: " + msg.getText() );
    }

    protected void logMessage(int level, String msgID, String context) {
        // Check quickly the level of the message:
        // If the messsage's level is greater than the logging level,
        // then the message should NOT be recorded ...
        if (!LogManager.isMessageToBeRecorded(context, level)) {
        	return;
        }
        LogMessage msg = new LogMessage(msgID, context, level);
//            System.out.println("Enqueuing message: " + msg.getText() );
        	LogManager.getInstance().forwardMessage(msg);
    }

    protected void logMessage(int level, String msgID, String context, Throwable e) {
        // Check quickly the level of the message:
        // If the messsage's level is greater than the logging level,
        // then the message should NOT be recorded ...
        if (!LogManager.isMessageToBeRecorded(context, level)) {
        	return;
        }


        LogMessage msg = new LogMessage(msgID, context, level, e);
//            System.out.println("Enqueuing message: " + msg.getText() );
        	LogManager.getInstance().forwardMessage(msg);
    }

    protected void logMessage(int level, String msgID, String context, Throwable e, Object[] msgParts) {
        // Check quickly the level of the message:
        // If the messsage's level is greater than the logging level,
        // then the message should NOT be recorded ...
        if (!LogManager.isMessageToBeRecorded(context, level)) {
        	return;
        }


        LogMessage msg = new LogMessage(msgID, context, level, e, msgParts);
//            System.out.println("Enqueuing message: " + msg.getText() );
           LogManager.getInstance().forwardMessage(msg);
    }




}

