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

package com.metamatrix.common.comm.platform.socket;

/**
 * Logger used by the socket communication framework.
 */
public interface SocketLog {
    
    
    //Message levels copied from com.metamatrix.common.log.MessageLevel

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
    
    
    
    void logCritical(String context, String message);
    void logCritical(String context, Throwable throwable, String message);

    void logError(String context, String message);
    void logError(String context, Throwable  throwable, String message);
    
    void logWarning(String context, String message);
    void logWarning(String context, Throwable throwable, String message);
    
    void logInfo(String context, String message);
    void logInfo(String context, Throwable throwable, String message);
    
    void logDetail(String context, String message);
    void logDetail(String context, Throwable throwable, String message);
    
    void logTrace(String context, String message);
    void logTrace(String context, Throwable throwable, String message);
    
    boolean isLogged(String context, int logLevel);
}
