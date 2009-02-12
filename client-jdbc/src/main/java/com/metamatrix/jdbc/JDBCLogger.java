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

package com.metamatrix.jdbc;

import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.LogMessage;
import com.metamatrix.core.log.Logger;
import com.metamatrix.core.log.MessageLevel;


/** 
 * This is Logger used for the logging the Client side JDBC Specific log
 * messages. 
 */
class JDBCLogger implements Logger {
    static final String PLUGIN_ID = "JDBC"; //$NON-NLS-1$
    
    LogListener listener = null;
    int allowedSeverity = BaseDataSource.LOG_NONE;
    String connectonId;
    
    /**
     * ctor 
     * @param severity
     * @param listener
     */
    public JDBCLogger(int severity, LogListener listener, String connId){
        this.allowedSeverity = severity;
        this.listener = listener;
        this.connectonId = connId;
    }
    
        
    /**
     * The problem is JDBC Logging levels & core MessageLevel levels and
     * IStatus logging levels all are different. Tried to minimize the code to
     * specify MessageLevel, however user configures to the JDBC level on URL, and 
     * need to write in IStatus level to make use of Listeners. (what a mess)    
     * @param level
     * @return true if allowed
     */
    private boolean allow(int level) {
        return (convertToJDBCLogLevel(level) <= this.allowedSeverity);
    }
    
    /**
     * Convert the message level to jdbc log level 
     * @param messageLevel
     * @return jdbc level
     */
    static int convertToJDBCLogLevel(int messageLevel) {
        switch(messageLevel) {
            case MessageLevel.CRITICAL:
            case MessageLevel.ERROR:
                return BaseDataSource.LOG_ERROR;
            case MessageLevel.WARNING:
            case MessageLevel.INFO:                
                return BaseDataSource.LOG_INFO;
            case MessageLevel.TRACE:                
                return BaseDataSource.LOG_TRACE;                                
            default:
                return BaseDataSource.LOG_NONE;            
        }
    }    
    
    /** 
     * @see com.metamatrix.core.log.Logger#log(int, java.lang.String)
     */
    public void log(int severity, String message) {
        log(severity, null, message);            
    }

    /** 
     * @see com.metamatrix.core.log.Logger#log(int, java.lang.Throwable, java.lang.String)
     */
    public void log(int severity, Throwable t, String message) {
        if (message != null && allow(severity)) {
            LogMessage msg = new LogMessage(PLUGIN_ID, severity, t, new Object[] {connectonId, message});
            listener.logMessage(msg);                                
        }        
    }
}
