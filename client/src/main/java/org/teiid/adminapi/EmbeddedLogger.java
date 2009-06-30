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

package org.teiid.adminapi;


/** 
 * Custom logging interface that provides a hook for custom implementations to log messages
 * produced by MM Query.
 * @since 4.3
 */
public interface EmbeddedLogger {
    
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
     * Logs the given message if the current logging level is &gt;= the logLevel parameter. 
     * @param logLevel logging level for this message
     * @param timestamp timestamp at which this log message was generated
     * @param componentName name of the component that generated this message
     * @param threadName name of the thread that generated this message
     * @param message message body. May be null.
     * @param throwable exception thrown. May be null.
     * @since 4.3
     */
    void log(int logLevel, long timestamp, String componentName, String threadName, String message, Throwable throwable);
}
