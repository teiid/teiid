/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.logging;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;



/**
 * This class represents the interface to a single logging framework
 * that is easily accessible by any component.  Using the LogManager, a component
 * can quickly submit a log message, and can rely upon the LogManager to determine
 * (a) whether that message is to be recorded or discarded; and (b) where
 * to send any recorded messages.  Thus, the component's code that submits
 * messages does not have to be modified to alter the logging behavior of the
 * application.
 * <p>
 * By default, all context(s) are logged by the LogManager.   The messages that
 * the LogManager actually records and sends to the destinations
 * can be controlled using two different and orthogonal parameters.
 * The first is a message <i>level</i> that filters messages based upon detail,
 * and the second is a message <i>context</i> that filters messages based upon
 * origin.  The LogManager tracks only those context(s) that should NOT be
 * logged.  Only if a message (which also is defined with these two parameters)
 * passes both filters will it be sent to the destinations.
 * <p>
 * Each message is submitted with one of the following levels (determined
 * by the particular method used to submit the message), sorted from the
 * least detailed to the greatest:
 * <ul>
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
 * </ul>
 * <p>
 * The context for a message is any application-specified String.  Again, only
 * those message contexts that match those in the LogManager's configuration will
 * be sent to the destinations.
 *
 */
public final class LogManager {

    public static class LoggingProxy implements InvocationHandler {
        private final Object instance;
        private final String loggingContext;
        private final int level;

        public LoggingProxy(Object instance, String loggingContext, int level) {
            this.instance = instance;
            this.loggingContext = loggingContext;
            this.level = level;
        }

        public Object invoke(Object proxy,
                             Method method,
                             Object[] args) throws Throwable {
            boolean log = LogManager.isMessageToBeRecorded(loggingContext, level);
            if (log) {
                StringBuffer message = new StringBuffer();
                message.append("before "); //$NON-NLS-1$
                message.append(method.getName());
                message.append(":"); //$NON-NLS-1$
                message.append(instance);
                message.append("("); //$NON-NLS-1$
                if (args != null) {
                    for (int i = 0; i < args.length; i++) {
                        if (args[i] != null) {
                            message.append(args[i]);
                        } else {
                            message.append("null"); //$NON-NLS-1$
                        }
                        if (i != args.length - 1) {
                            message.append(","); //$NON-NLS-1$
                        }
                    }
                }
                message.append(")"); //$NON-NLS-1$
                LogManager.log(level, loggingContext, message.toString());
            }
            try {
                Object result = method.invoke(instance, args);
                if (log) {
                    LogManager.log(level, loggingContext,
                        "after " + method.getName()+ " : "+result); //$NON-NLS-1$ //$NON-NLS-2$
                }
                return result;
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }

    static volatile Logger logListener = new JavaLogger(); // either injected or manually set using the set methods

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
     * @param message the log message; the message is
     * not logged if this parameter is null
     */
    public static void logCritical(String context, Object message) {
        logMessage(MessageLevel.CRITICAL, context, message);
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
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param message the log message (may be null)
     */
    public static void logCritical(String context, Throwable e, Object message) {
        log(MessageLevel.CRITICAL,context,e,message);
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
     * @param message the log message; the message is
     * not logged if this parameter is null
     */
    public static void logError(String context, Object message) {
        logMessage(MessageLevel.ERROR, context,message);
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
     * @param message the log message (may be null)
     */
    public static void logError(String context, Throwable e, Object message) {
        log(MessageLevel.ERROR,context,e,message);
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
     * @param message the log message; the message is
     * not logged if this parameter is null
     */
    public static void logWarning(String context, Object message) {
        logMessage(MessageLevel.WARNING, context,message);
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
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param message the log message (may be null)
     */
    public static void logWarning(String context, Throwable e, Object message) {
        log(MessageLevel.WARNING,context,e,message);
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
     * @param message the log message; the message is
     * not logged if this parameter is null
     */
    public static void logInfo(String context, Object message) {
        logMessage(MessageLevel.INFO, context,message);
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
     * @param msgPart the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logDetail(String context, Object msgPart) {
        logMessage(MessageLevel.DETAIL, context, msgPart);
    }

    public static void logDetail(String context, Object msgPart, Object msgPart1) {
        logMessage(MessageLevel.DETAIL, context, msgPart, msgPart1);
    }

    public static void logDetail(String context, Object msgPart, Object msgPart1, Object msgPart2) {
        logMessage(MessageLevel.DETAIL, context, msgPart, msgPart1, msgPart2);
    }

    public static void logDetail(String context, Object ... msgParts) {
        logMessage(MessageLevel.DETAIL, context, msgParts);
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
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param message the log message (may be null)
     */
    public static void logDetail(String context, Throwable e, Object ... message) {
        log(MessageLevel.DETAIL,context,e,message);
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
     * @param msgParts the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void logTrace(String context, Object ... msgParts) {
        logMessage(MessageLevel.TRACE, context, msgParts);
    }

    public static void logTrace(String context, Object msgPart) {
        logMessage(MessageLevel.TRACE, context, msgPart);
    }

    public static void logTrace(String context, Object msgPart, Object msgPart1) {
        logMessage(MessageLevel.TRACE, context, msgPart, msgPart1);
    }

    public static void logTrace(String context, Object msgPart, Object msgPart1, Object msgPart2) {
        logMessage(MessageLevel.TRACE, context, msgPart, msgPart1, msgPart2);
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
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param msgParts the individual parts of the log message (may be null)
     */
    public static void logTrace(String context, Throwable e, Object ... msgParts) {
        logMessage(MessageLevel.TRACE,context,e,msgParts);
    }

    /**
     * Send a message of the specified level to the log.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param msgLevel
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param message the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void log(int msgLevel, String context, Object message) {
        logMessage(msgLevel, context, message);
    }

    /**
     * Send a message of the specified level to the log.
     * <p>
     * Only if the log manager is configured to send such messages to the
     * destination will the message be recorded.
     * @param context the context for this log message (for example, the component
     * that is generating this message).
     * @param e the exception that is to be logged; the message is
     * not logged if this parameter is null
     * @param message the individual parts of the log message; the message is
     * not logged if this parameter is null
     */
    public static void log(int msgLevel, String context, Throwable e, Object... message) {
        if (!isMessageToBeRecorded(context, msgLevel)) {
            return;
        }
        logListener.log(msgLevel, context, e, message);
    }

    public static Logger setLogListener(Logger listener) {
        Logger old = logListener;
        logListener.shutdown();
        if (listener != null) {
            logListener = listener;
        }
        else {
            logListener = new JavaLogger();
        }
        return old;
    }

    /**
     * Utility method to identify whether a log message with the specified
     * context and level will be recorded in the LogManager's destinations.
     * @param context
     * @param msgLevel
     * @return true if the message would be recorded if sent to the LogManager,
     * or false if it would be discarded by the LogManager.
     */
    public static boolean isMessageToBeRecorded(String context, int msgLevel) {
        if (logListener != null) {
            return logListener.isEnabled(context, msgLevel);
        }
        return true;
    }

    private static void logMessage(int level, String context, Object ... msgParts) {
        if (msgParts == null || msgParts.length == 0 || !isMessageToBeRecorded(context, level)) {
            return;
        }
        logListener.log(level, context, msgParts);
    }

    private static void logMessage(int level, String context, Object msgPart) {
        if (msgPart == null || !isMessageToBeRecorded(context, level)) {
            return;
        }
        logListener.log(level, context, msgPart);
    }

    private static void logMessage(int level, String context, Object msgPart, Object msgPart1) {
        if (msgPart == null || !isMessageToBeRecorded(context, level)) {
            return;
        }
        logListener.log(level, context, msgPart, msgPart1);
    }

    private static void logMessage(int level, String context, Object msgPart, Object msgPart1, Object msgPart2) {
        if (msgPart == null || !isMessageToBeRecorded(context, level)) {
            return;
        }
        logListener.log(level, context, msgPart, msgPart1, msgPart2);
    }

    /**
     * Create a logging proxy, that logs at entry and exit points of the method calls on the provided interfaces.
     */
    public static Object createLoggingProxy(final String loggingContext,
                                             final Object instance,
                                             final Class<?>[] interfaces,
                                             final int level) {
        return Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, new LoggingProxy(instance, loggingContext, level));
    }

    public static Object createLoggingProxy(final String loggingContext,
            final Object instance,
            final Class<?>[] interfaces,
            final int level,
            ClassLoader classLoader) {
            return Proxy.newProxyInstance(classLoader, interfaces, new LoggingProxy(instance, loggingContext, level));
    }

    public static void putMdc(String key, String val) {
        logListener.putMdc(key, val);
    }

    public static void removeMdc(String key) {
        logListener.removeMdc(key);
    }
}
