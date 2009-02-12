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

package com.metamatrix.common.log;

import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.MessageLevel;

/**
 *
 * @deprecated use {@link LogManager}
 */
public final class I18nLogManager {
    private static I18nLogManager INSTANCE = new I18nLogManager();

    private I18nLogManager() {
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

    private void logMessage(int level, String msgID, String context, Object[] msgParts) {
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

    private void logMessage(int level, String msgID, String context, Throwable e) {
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

    private void logMessage(int level, String msgID, String context, Throwable e, Object[] msgParts) {
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

