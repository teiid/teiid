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



/** 
 * Constants that govern the behavior of the socket communications framework.
 * @since 4.2
 */
public class SocketConstants {
    
    /**Java system property.  The loglevel that SocketLog will use.  Should be a String value {NONE|CRITICAL|ERROR|WARNING|INFO|DETAIL|TRACE}*/
    public static final String SOCKET_LOG_LEVEL   = "metamatrix.sockets.log.level";  //$NON-NLS-1$
    /**Java system property.  Maximum number of threads used to read sockets*/
    public static final String SOCKET_MAX_THREADS   = "metamatrix.sockets.max.threads"; //$NON-NLS-1$
    /**Java system property.  Maximum time to live for a socket reader thread asynchronous calls.  If it times out, it will be removed, and recreated later when needed.*/
    public static final String SOCKET_TTL           = "metamatrix.sockets.ttl"; //$NON-NLS-1$
    /**Java system property.  Maximum time to live for a socket reader thread synchronous calls.  If it times out, it will be removed, and recreated later when needed.*/    
    public static final String SYNCH_SOCKET_TTL           = "metamatrix.synchronous.sockets.ttl"; //$NON-NLS-1$

    /**Java system property.  Input buffer size of the physical sockets.*/
	public static final String SOCKET_INPUT_BUFFER_SIZE           = "metamatrix.sockets.inputBufferSize"; //$NON-NLS-1$
    /**Java system property.  Output buffer size of the physical sockets.*/
    public static final String SOCKET_OUTPUT_BUFFER_SIZE           = "metamatrix.sockets.outputBufferSize"; //$NON-NLS-1$
    /**Java system property.  Value of the conserve-bandwidth flag of the physical sockets.*/
    public static final String SOCKET_CONSERVE_BANDWIDTH           = "metamatrix.sockets.conserveBandwidth"; //$NON-NLS-1$
    
    public static final String DEFAULT_SOCKET_LOG_LEVEL = "ERROR"; //$NON-NLS-1$
    public static final int DEFAULT_MAX_THREADS = 15; 
    public static final long DEFAULT_TTL = 120000L; 
    public static final long DEFAULT_SYNCH_TTL = 120000L; 
    public static final int DEFAULT_SOCKET_INPUT_BUFFER_SIZE = 102400; 
    public static final int DEFAULT_SOCKET_OUTPUT_BUFFER_SIZE = 102400; 

    /** 
     * 
     * @since 4.2
     */
    public SocketConstants() {
        super();
    }
    
    /*
     * Retrieve the asynchronous call Time-To-Live
     *  
     * @return number of ms 
     * @since 4.2
     */
    public static long getTTL() {
        return Long.getLong(SOCKET_TTL, DEFAULT_TTL).longValue();
    }
    
    /*
     * Retrieve the synchronous call Time-To-Live
     *  
     * @return number of ms 
     * @since 4.2
     */
    public static long getSynchronousTTL() {
        return Long.getLong(SYNCH_SOCKET_TTL, DEFAULT_SYNCH_TTL).longValue();
    }      

    /* 
     * Get the max number of threads
     * 
     * @return max number
     * @since 4.2
     */
    public static int getMaxThreads() {
        return Integer.getInteger(SOCKET_MAX_THREADS, DEFAULT_MAX_THREADS).intValue();
    }

    /*
     * Get the SocketLog.  If the value of the java System property DEBUG_SOCKETS_NAME is not true,
     * returns a NullLogger.
     */
    public static SocketLog getLog(String contextName) {
        SocketLog result = new PrintStreamSocketLog(System.out, contextName, getLogLevel());
        return result;
    }
    
    
    /**
     * Get the logLevel that SocketLog will use. 
     * @return
     * @since 4.3
     */
    public static int getLogLevel() {
        String logLevelString = System.getProperty(SOCKET_LOG_LEVEL, DEFAULT_SOCKET_LOG_LEVEL);
        return PrintStreamSocketLog.getLogLevelInt(logLevelString);
    }

    public static int getInputBufferSize() {
        return Integer.getInteger(SOCKET_INPUT_BUFFER_SIZE, DEFAULT_SOCKET_INPUT_BUFFER_SIZE).intValue();
    }

    public static int getOutputBufferSize() {
        return Integer.getInteger(SOCKET_OUTPUT_BUFFER_SIZE, DEFAULT_SOCKET_OUTPUT_BUFFER_SIZE).intValue();
    }
    
    public static boolean getConserveBandwidth() {
        return Boolean.getBoolean(SOCKET_CONSERVE_BANDWIDTH); // Defaults to false
    }

}
