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

package com.metamatrix.admin.api.objects;

import java.util.Set;


/** 
 * The LogConfiguration describes the current configuration of the
 * system logger.
 * 
 * <p>The log configuration is used to filter on log contexts (components) 
 * and log levels (severity).</p>
 * 
 * @since 4.3
 */
public interface LogConfiguration extends AdminObject {
    
    /**
     * Message level value that specifies that no messages are to be recorded.
     */
    public static final int NONE            = 0;

    /**
     * Message level value that specifies that critical messages are to be recorded.
     */
    public static final int CRITICAL        = 1;

    /**
     * Message level value that specifies that error messages and critical
     * messages are to be recorded.
     */
    public static final int ERROR           = 2;

    /**
     * Message level value that specifies that warning, error and critical
     * messages are to be recorded.
     */
    public static final int WARNING         = 3;

    /**
     * Message level value that specifies that information, warning, error and critical
     * messages are to be recorded.
     */
    public static final int INFO            = 4;

    /**
     * Message level value that specifies that detailed, information, warning, error and critical
     * messages are to be recorded.
     */
    public static final int DETAIL          = 5;

    /**
     * Message level value that specifies that all messages are to be recorded.
     */
    public static final int TRACE           = 6;
    
    /**
     * A flag that specifies ALL logging contexts.  Can be used in specifying included or
     * discarded logging contexts.  If this value exists among other logging contexts
     * in the included or discarded sets, all other values are ignored.  If this value
     * is contained in <i>both</i> sets, no change in the logging configuration will take
     * place.
     */
    public static final String CTX_ALL      = "CTX_ALL"; //$NON-NLS-1$
    
    /**
     * The name of the System property that contains the message level for the LogManager.
     * This is an optional property that defaults to '3'.
     */
    public static final String LOG_LEVEL_PROPERTY_NAME   = "metamatrix.log"; //$NON-NLS-1$

    /**
     * <p>The name of the System property that contains the set of comma-separated
     * context names for messages <i>not</i> to be recorded.  A message context is simply
     * some string that identifies something about the component that generates
     * the message.  The value for the contexts is application specific.
     * </p><p>
     * This is an optional property that defaults to no contexts (i.e., messages
     * with any context are recorded).</p>
     */
    public static final String LOG_CONTEXT_PROPERTY_NAME = "metamatrix.log.contexts"; //$NON-NLS-1$

    /**
     * This String should separate each of the contexts in the String value
     * for the property {@link #LOG_CONTEXT_PROPERTY_NAME}.  For example,
     * if this delimiter were a comma, the value for the property might
     * be something like "CONFIG,QUERY,CONFIGURATION_ADAPTER".
     */
    public static final String CONTEXT_DELIMETER = ","; //$NON-NLS-1$
    
    
    /**
     * Get the current configured Log Level 
     * TODO: An int log level is of little use to clients.  Need to get log level descriptions too.
     * @return int value 
     * @see com.metamatrix.core.log.MessageLevel
     * @since 4.3
     */
    int getLogLevel();
    
    /**
     * Obtain the set of message contexts that are currently used.
     * @return the unmodifiable set of context Strings; never null
     */
    Set getIncludedContexts(); 
    
    /**
     * Obtain the set of contexts for messages that are to be discarded.
     * If this method returns an empty set, then messages in all contexts
     * are being recorded; if not empty, then messages with a context in the
     * returned set are discarded and messages for all other contexts recorded.
     * @return the set of contexts for messages that are to be discarded
     */
    Set getDiscardedContexts();
    
    /** 
     * Set the Log Level
     * 
     *  Note:  Must call setLogConfiguration(LogConfiguration) for log level to take
     *  affect on the server. 
     * 
     * @param logLevel The logLevel to set.
     * @since 4.3
     */
    public void setLogLevel(int logLevel);
    

    /**
     * Direct the log configuration to discard the given contexts and
     * not record them.
     * 
     *  Note:  Must call setLogConfiguration(LogConfiguration) for log level to take
     *  affect on the server. 
     * 
     * @param contexts the Set of contexts that should be discarded.
     */
    void setDiscardedContexts(Set contexts);

    /**
     * Direct the log configuration to record only these contexts.
     * 
     *  Note:  Must call setLogConfiguration(LogConfiguration) for log level to take
     *  affect on the server. 
     * 
     * @param contexts the Set of contexts that should be included.
     */
    void setIncludedContexts(Set contexts);
    
}
