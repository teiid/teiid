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

package com.metamatrix.dqp.spi.basic;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.dqp.spi.CommandLoggerSPI;

/**
 * <p>Command logger service provider which logs to a file.  The filename must be
 * supplied as a DQP property, see {@link #LOG_FILE_NAME_PROPERTY}.<p>
 * 
 * <p>This implementation is intended purely as an example.  It is not intended for
 * a heavy, enterprise load.  It is built using an internal java.io.BufferedWriter.
 * Each log entry will be on a new line, and the BufferedWriter is flushed after
 * each line is written.  Timestamps are included for each log entry.</p>
 */
public class FileCommandLogger implements CommandLoggerSPI {
    
    /**
     * The property name of the log filename property; name and value must be supplied
     * in DQP properties.  The value should be a String file name suitable for instantiating
     * a java.io.BufferedWriter instance.
     */
    public static final String LOG_FILE_NAME_PROPERTY = "dqp.commandLogger.fileName";  //$NON-NLS-1$

    private static final String TIMESTAMP_FORMAT = "yyyy.MM.dd HH:mm:ss.SSS"; //$NON-NLS-1$
    // Cache date formatter which is expensive to create
    private static DateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat(TIMESTAMP_FORMAT);

    
    private String filename;
    private BufferedWriter logWriter;

    /** 
     * Initialize this command logger.  This requires the {@link #LOG_FILE_NAME_PROPERTY}
     * property.  After this method is called, a java.io.BufferedWriter will be opened to
     * the file.  Any exception in opening the BufferedWriter will be printed to
     * System.out.
     * @see #LOG_FILE_NAME_PROPERTY
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#initialize(java.util.Properties)
     */
    public void initialize(Properties props) {
        this.filename = props.getProperty(LOG_FILE_NAME_PROPERTY);
        if (this.filename == null) {
            System.out.println("FileCommandLogger could not find log filename property"); //$NON-NLS-1$
        } else {
            boolean append = true;
            try {
                this.logWriter = new BufferedWriter( new FileWriter(this.filename, append));
            } catch (IOException e) {
                System.out.println("FileCommandLogger could not write to log file " + this.filename + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
    }
    
    /** 
     * This method causes the internal java.io.BufferedWriter to be closed.
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#close()
     */
    public void close() {
        try {
            this.logWriter.close();
        } catch (IOException err) {
            // ignore
        }
    }

    private void logMessage(String outputString) {

        try {
            this.logWriter.write(outputString, 0, outputString.length());
            this.logWriter.newLine();
            this.logWriter.flush();
        } catch (IOException e) {
            System.out.println("FileCommandLogger could not write to log file " + this.filename + ": " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        } 
    }
    
    private String getTimestampString(Date date) {
        return TIMESTAMP_FORMATTER.format(date);
    }
    
    /** 
     * Logs an entry for user command starting.
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#userCommandStart(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public void userCommandStart(long timestamp,
                                 String requestID,
                                 String transactionID,
                                 String sessionID,
                                 String applicationName,
                                 String principal,
                                 String vdbName,
                                 String vdbVersion,
                                 String sql) {

        String outputString = getTimestampString(new Date()) + "\tSTART USER COMMAND:\tstartTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tapplicationName=" + applicationName + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion + "\tsql=" + sql;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ 
        this.logMessage(outputString);
    }

    /** 
     * Logs an entry for user command ending.
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#userCommandEnd(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     */
    public void userCommandEnd(long timestamp,
                               String requestID,
                               String transactionID,
                               String sessionID,
                               String principal,
                               String vdbName,
                               String vdbVersion,
                               int finalRowCount,
                               boolean isCancelled,
                               boolean errorOccurred) {
        String outputString = getTimestampString(new Date()) + "\tEND USER COMMAND:\tendTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion + "\tfinalRowCount=" + finalRowCount + "\tisCancelled=" + isCancelled + "\terrorOccurred=" + errorOccurred;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$
        this.logMessage(outputString);
    }

    /** 
     * Logs an entry for data source command starting.
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#dataSourceCommandStart(java.lang.String, long, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public void dataSourceCommandStart(long timestamp,
                                       String requestID,
                                       long sourceCommandID,
                                       String subTransactionID,
                                       String modelName,
                                       String connectorBindingName,
                                       String sessionID,
                                       String principal,
                                       String sql,
                                       ExecutionContext context) {
        String outputString = getTimestampString(new Date()) + "\tSTART DATA SRC COMMAND:\tstartTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\tsourceCommandID="+ sourceCommandID + "\tsubTxID=" + subTransactionID + "\tmodelName="+ modelName + "\tconnectorBindingName=" + connectorBindingName + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tsql=" + sql;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
        this.logMessage(outputString);
    }

    /** 
     * Logs an entry for data source command ending.
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#dataSourceCommandEnd(java.lang.String, long, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, int, boolean, boolean)
     */
    public void dataSourceCommandEnd(long timestamp,
                                     String requestID,
                                     long sourceCommandID,
                                     String subTransactionID,
                                     String modelName,
                                     String connectorBindingName,
                                     String sessionID,
                                     String principal,
                                     int finalRowCount,
                                     boolean isCancelled,
                                     boolean errorOccurred,
                                     ExecutionContext context) {
        String outputString = getTimestampString(new Date()) + "\tEND DATA SRC COMMAND:\tendTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\tsourceCommandID="+ sourceCommandID + "\tsubTxID=" + subTransactionID + "\tmodelName="+ modelName + "\tconnectorBindingName=" + connectorBindingName + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tfinalRowCount=" + finalRowCount + "\tisCancelled=" + isCancelled + "\terrorOccurred=" + errorOccurred;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$ //$NON-NLS-11$
        this.logMessage(outputString);
    }

    /** 
     * Logs an entry for transaction starting.
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#transactionStart(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public void transactionStart(long timestamp,
                                 String transactionID,
                                 String sessionID,
                                 String principal,
                                 String vdbName,
                                 String vdbVersion) {
        
        String outputString = getTimestampString(new Date()) + "\tSTART TRANSACTION:\tstartTime=" + getTimestampString(new Date(timestamp)) + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$
        this.logMessage(outputString);
    }

    /** 
     * Logs an entry for transaction ending.
     * @see com.metamatrix.dqp.spi.CommandLoggerSPI#transactionEnd(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, boolean)
     */
    public void transactionEnd(long timestamp,
                               String transactionID,
                               String sessionID,
                               String principal,
                               String vdbName,
                               String vdbVersion,
                               boolean isCommit) {
        
        String outputString = getTimestampString(new Date()) + "\tEND TRANSACTION:\tendTime=" + getTimestampString(new Date(timestamp)) + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion + "\tisCommit=" + isCommit;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        this.logMessage(outputString);
    }

}
