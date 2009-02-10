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

package com.metamatrix.dqp.spi;

import java.util.Properties;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.dqp.service.TrackingService;

/** 
 * Defines a service-provider interface for accepting command and transaction 
 * information, suitable for logging, from MetaMatrix DQP.  
 * (See DQP {@link TrackingService} interface.)
 */
public interface CommandLoggerSPI {

    /**
     * Initialize this service provider with the specified properties.  Properties
     * must be specified in the DQP properties (along with the property defining
     * the concrete classname of the implementation of this interface).
     * @param props Initialialization properties
     */
    public void initialize(Properties props);
    
    /**
     * Close this service provider, release any external resources 
     */
    public void close();
    
    /**
     * A user command has started.  This method will be called when the command
     * starts.
     * @param timestamp time in milliseconds marking the start of the command
     * @param requestID Unique command ID.
     * @param transactionID Unique transaction ID (optional, may be null)
     * @param sessionID Session ID.
     * @param applicationName name of the user application
     * @param principal User name.
     * @param vdbName VDB name.
     * @param vdbVersion VDB version.
     * @param sql SQL for the command.
     */
    public void userCommandStart(long timestamp, String requestID, String transactionID,
            String sessionID, String applicationName, String principal, String vdbName,
            String vdbVersion, String sql);

    /**
     * A user command has ended.  This method will be called as the command
     * ends, either successfully, with an error, or by being cancelled.
     * @param timestamp time in milliseconds marking the end of the command.
     * @param requestID Unique command ID.
     * @param transactionID Unique transaction ID (optional, may be null)
     * @param sessionID Session ID.
     * @param principal User name.
     * @param vdbName VDB name.
     * @param vdbVersion VDB version.
     * @param finalRowCount total rows returned to the user.
     * @param isCancelled true if command was requested to be cancelled, false otherwise.
     * @param errorOccurred true if error occurred, false if status is okay.
     */
    public void userCommandEnd(long timestamp, String requestID, String transactionID,
           String sessionID, String principal, String vdbName, String vdbVersion, 
           int finalRowCount, boolean isCancelled, boolean errorOccurred);
    
    
    /**
     * A data source-specific command has started.  This method will be called as the
     * command starts.
     * @param timestamp time in milliseconds marking the start of the source command.
     * @param requestID Unique command ID.
     * @param sourceCommandID unique ID of source command, which is also the
     * ID of the plan node representing that source command.
     * @param subTransactionID Unique subtransaction ID (optional, may be null).
     * @param modelName Name of model.
     * @param connectorBindingName Connector binding name.
     * @param sessionID Session ID.
     * @param principal User name.
     * @param sql SQL for the command.
     */
    public void dataSourceCommandStart(long timestamp, String requestID, long sourceCommandID, String subTransactionID, String modelName, 
            String connectorBindingName, String sessionID, String principal, String sql, ExecutionContext context);

    /**
     * Data source-specific command has ended.  This method will be called as the
     * command ends, either normally, by being cancelled, or with an error.
     * @param timestamp time in milliseconds marking the end of the source command.
     * @param requestID Unique command ID.
     * @param sourceCommandID unique ID of source command, which is also the
     * ID of the plan node representing that source command.
     * @param subTransactionID Unique subtransaction ID (optional, may be null).
     * @param modelName Name of model.
     * @param connectorBindingName Connector binding name.
     * @param sessionID Session ID.
     * @param principal User name.
     * @param finalRowCount Final row count.
     * @param isCancelled true if command was requested to be cancelled, false otherwise.
     * @param errorOccurred true if error occurred, false if status is okay.
     */
    public void dataSourceCommandEnd(long timestamp, String requestID, long sourceCommandID, String subTransactionID, String modelName, 
            String connectorBindingName, String sessionID, String principal, int finalRowCount, 
            boolean isCancelled, boolean errorOccurred, ExecutionContext context);
        
}
