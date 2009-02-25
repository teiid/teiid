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

package org.teiid.connector.api;

/**
 * Provide access to write messages to the MetaMatrix logs.
 */
public interface ConnectorLogger {

    /**
     * Log an error message.
     * @param message The message
     */
    void logError( String message );

    /**
     * Log an error message with an error, which may allow the stack
     * trace for the error to be logged, depending on the log configuration.
     * @param message The message 
     * @param error The error
     */
    void logError( String message, Throwable error );

    /**
     * Log a warning message.
     * @param message The message
     */
    void logWarning( String message );

    /**
     * Log a warning message.
     * @param message The message
     */
    void logWarning( String message, Throwable error );

    /**
     * Log an informational message.
     * @param message The message
     */
    void logInfo( String message );

    /**
     * Log an informational message.
     * @param message The message
     */
    void logInfo( String message, Throwable error);
    
    /**
     * Log a detail debugging message.
     * @param message The message
     */
    void logDetail( String message );

    /**
     * Log a detail debugging message.
     * @param message The message
     */
    void logDetail( String message, Throwable error );

    /**
     * Log a trace debugging message.
     * @param message The message
     */
    void logTrace( String message );
    
    /**
     * Log a trace debugging message.
     * @param message The message
     */
    void logTrace( String message, Throwable error );

    /**
     * @return true if error logging is enabled
     */
    boolean isErrorEnabled();

    /**
     * @return true if warning logging is enabled
     */
    boolean isWarningEnabled();

    /**
     * @return true if info logging is enabled
     */
    boolean isInfoEnabled();

    /**
     * @return true if detail logging is enabled
     */
    boolean isDetailEnabled();

    /**
     * @return true if trace logging is enabled
     */
    boolean isTraceEnabled();
    
}
