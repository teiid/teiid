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

package com.metamatrix.common.comm.platform.socket;

import java.io.PrintStream;

/**
 * Writes log messages to a PrintStream.
 */
public class PrintStreamSocketLog implements SocketLog{
    private PrintStream stream;
    private String staticContext;
    
    private int logLevel = SocketLog.NONE;
    
    public PrintStreamSocketLog(PrintStream stream, String staticContext, int logLevel) {
        this.stream = stream;
        this.staticContext = staticContext;
        this.logLevel = logLevel;
    }




    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logCritical(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logCritical(String context,
                            String message) {
        if (isLogged(context, SocketLog.CRITICAL)) {
            log(SocketLog.CRITICAL, context, message);
        }
    }




    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logCritical(java.lang.String, java.lang.Throwable, java.lang.String)
     * @since 4.3
     */
    public void logCritical(String context,
                            Throwable throwable,
                            String message) {
        if (isLogged(context, SocketLog.CRITICAL)) {
            log(SocketLog.CRITICAL, context, throwable, message);
        }
    }
    
    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logError(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logError(String context,
                         String message) {
        if (isLogged(context, SocketLog.ERROR)) {
            log(SocketLog.ERROR, context, message);
        }
    }

    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logError(java.lang.String, java.lang.Throwable, java.lang.String)
     * @since 4.3
     */
    public void logError(String context,
                         Throwable throwable,
                         String message) {
        if (isLogged(context, SocketLog.ERROR)) {
            log(SocketLog.ERROR, context, throwable, message);
        }
    }

    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logWarning(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logWarning(String context,
                           String message) {
        if (isLogged(context, SocketLog.WARNING)) {
            log(SocketLog.WARNING, context, message);
        }
    }

    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logWarning(java.lang.String, java.lang.Throwable, java.lang.String)
     * @since 4.3
     */
    public void logWarning(String context,
                           Throwable throwable,
                           String message) {
        if (isLogged(context, SocketLog.WARNING)) {
            log(SocketLog.WARNING, context, throwable, message);
        }
    }

    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logInfo(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logInfo(String context,
                        String message) {
        if (isLogged(context, SocketLog.INFO)) {
            log(SocketLog.INFO, context, message);
        }
    }
    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logInfo(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logInfo(String context,
                        Throwable throwable,
                        String message) {
        if (isLogged(context, SocketLog.INFO)) {
            log(SocketLog.INFO, context, throwable, message);
        }
    }
    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logDetail(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logDetail(String context,
                          String message) {
        if (isLogged(context, SocketLog.DETAIL)) {
            log(SocketLog.DETAIL, context, message);
        }
    }
    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logDetail(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logDetail(String context,
                          Throwable throwable,
                          String message) {
        if (isLogged(context, SocketLog.DETAIL)) {
            log(SocketLog.DETAIL, context, throwable, message);
        }
    }

    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logTrace(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logTrace(String context,
                         String message) {
        if (isLogged(context, SocketLog.TRACE)) {
            log(SocketLog.TRACE, context, message);
        }
    }
    /** 
     * @see com.metamatrix.common.comm.platform.socket.SocketLog#logTrace(java.lang.String, java.lang.String)
     * @since 4.3
     */
    public void logTrace(String context,
                         Throwable throwable,
                         String message) {
        if (isLogged(context, SocketLog.TRACE)) {
            log(SocketLog.TRACE, context, throwable, message);
        }
    }


    private void log(int level, String context, String message) {
        stream.print(getLogLevelString(level));
        stream.print("\t["); //$NON-NLS-1$
        stream.print(staticContext);
        stream.print("]\t"); //$NON-NLS-1$
        stream.print(context);
        stream.print("\t"); //$NON-NLS-1$
        stream.println(message);
    }
    
    private void log(int level, String context, Throwable t, String message) {
        log(level, context, message);
        t.printStackTrace(stream);
    }
    
    
    
    public boolean isLogged(String context, int level) {
        return this.logLevel >= level;
    }
    
    
    private static String getLogLevelString(int level) {
        switch(level) {
            case SocketLog.CRITICAL:
                return "CRITICAL"; //$NON-NLS-1$
            case SocketLog.ERROR:
                return "ERROR"; //$NON-NLS-1$
            case SocketLog.WARNING:
                return "WARNING"; //$NON-NLS-1$ 
            case SocketLog.INFO:
                return "INFO"; //$NON-NLS-1$
            case SocketLog.DETAIL:
                return "DETAIL"; //$NON-NLS-1$
            case SocketLog.TRACE:
                return "TRACE"; //$NON-NLS-1$
                
            default:
                return "NONE"; //$NON-NLS-1$
        }
    }
    
    public static int getLogLevelInt(String level) {
        if (level.equalsIgnoreCase("CRITICAL")) {  //$NON-NLS-1$
            return SocketLog.CRITICAL;
        } else if (level.equalsIgnoreCase("ERROR")) {  //$NON-NLS-1$
            return SocketLog.ERROR;
        } else if (level.equalsIgnoreCase("WARNING")) {  //$NON-NLS-1$
            return SocketLog.WARNING;
        } else if (level.equalsIgnoreCase("INFO")) {  //$NON-NLS-1$
            return SocketLog.INFO;
        } else if (level.equalsIgnoreCase("DETAIL")) {  //$NON-NLS-1$
            return SocketLog.DETAIL;
        } else if (level.equalsIgnoreCase("TRACE")) {  //$NON-NLS-1$
            return SocketLog.TRACE;
        } else {
            return SocketLog.NONE;
        }
    }
}
