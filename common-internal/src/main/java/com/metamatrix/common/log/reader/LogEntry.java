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

package com.metamatrix.common.log.reader;

import java.io.Serializable;
import java.util.Date;


/** 
 * @since 4.3
 */
public class LogEntry implements Serializable {

    
    
    private String exception;
    private Date date;
    private String message;
    private String context;
    private int level;
    private String hostName;
    private String processName;
    private String threadName;

   
    
    /**
     * The name of the System property that contains the maximum number of rows
     * that will be returned for viewing..  This is an optional property that defaults to '2500'.
     */
    public static final String MAX_LOG_ROWS_RETURNED = "metamatrix.log.maxRows"; //$NON-NLS-1$


    /** 
     * @return Returns the context.
     * @since 4.3
     */
    public String getContext() {
        return this.context;
    }
    
    /** 
     * @param context The context to set.
     * @since 4.3
     */
    public void setContext(String context) {
        this.context = context;
    }

    /** 
     * @return Returns the date.
     * @since 4.3
     */
    public Date getDate() {
        return this.date;
    }
    /** 
     * @param date The date to set.
     * @since 4.3
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /** 
     * @return Returns the exception.
     * @since 4.3
     */
    public String getException() {
        return this.exception;
    }
    
    /** 
     * @param exception The exception to set.
     * @since 4.3
     */
    public void setException(String exception) {
        this.exception = exception;
    }
    /** 
     * @return Returns the hostName.
     * @since 4.3
     */
    public String getHostName() {
        return this.hostName;
    }
    /** 
     * @param hostName The hostName to set.
     * @since 4.3
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /** 
     * @return Returns the level.
     * @since 4.3
     */
    public int getLevel() {
        return this.level;
    }

    /** 
     * @param level The level to set.
     * @since 4.3
     */
    public void setLevel(int level) {
        this.level = level;
    }

    /** 
     * @return Returns the message.
     * @since 4.3
     */
    public String getMessage() {
        return this.message;
    }
    /** 
     * @param message The message to set.
     * @since 4.3
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /** 
     * @return Returns the processName.
     * @since 4.3
     */
    public String getProcessName() {
        return this.processName;
    }

    /** 
     * @param processName The processName to set.
     * @since 4.3
     */
    public void setProcessName(String processName) {
        this.processName = processName;
    }

    /** 
     * @return Returns the threadName.
     * @since 4.3
     */
    public String getThreadName() {
        return this.threadName;
    }

    /** 
     * @param threadName The threadName to set.
     * @since 4.3
     */
    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }
    
    
}
