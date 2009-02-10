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

package com.metamatrix.dqp.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.dqp.spi.CommandLoggerSPI;
import com.metamatrix.dqp.spi.TrackerLogConstants;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.TestQuery;

/** 
 * Tests the DQP Tracking service implementation which uses a CommandLogger service
 * provider
 */
public class TestCustomizableTrackingService extends TestCase {

    /**
     * Constructor for TestConnectorCapabilitiesFinder.
     * @param name
     */
    public TestCustomizableTrackingService(String name) {
        super(name);
    }
    
    // ========================================================================================================
    // tests
    // ========================================================================================================
    
    public void testLogAll() throws Exception {
    	CustomizableTrackingService trackingService = getTrackingService(true, true, true);
        List expectedResults = new ArrayList();
        expectedResults.add(logExampleSourceCommandStart(trackingService));
        expectedResults.add(logExampleUserCommandStart(trackingService));
        expectedResults.add(logExampleUserCommandEnd(trackingService));
        expectedResults.add(logExampleSourceCommandCancelled(trackingService));
        trackingService.stop();
        assertEquals(expectedResults, ((FakeCommandLogger)trackingService.getCommandLogger()).logEntries);
    }

    public void testLogJustTransactions() throws Exception {
    	CustomizableTrackingService trackingService = getTrackingService(true, false, false);
        List expectedResults = new ArrayList();
        logExampleSourceCommandStart(trackingService);
        logExampleSourceCommandCancelled(trackingService);
        logExampleUserCommandStart(trackingService);
        logExampleUserCommandEnd(trackingService);
        trackingService.stop();
        assertEquals(expectedResults, ((FakeCommandLogger)trackingService.getCommandLogger()).logEntries);
    }    

    public void testLogJustCommands() throws Exception {
    	CustomizableTrackingService trackingService = getTrackingService(false, true, true);
        List expectedResults = new ArrayList();
        expectedResults.add(logExampleSourceCommandStart(trackingService));
        expectedResults.add(logExampleUserCommandStart(trackingService));
        expectedResults.add(logExampleUserCommandEnd(trackingService));
        expectedResults.add(logExampleSourceCommandCancelled(trackingService));
        trackingService.stop();
        assertEquals(expectedResults, ((FakeCommandLogger)trackingService.getCommandLogger()).logEntries);
    }     

    public void testLogJustUserCommands() throws Exception {
    	CustomizableTrackingService trackingService = getTrackingService(false, true, false);
        List expectedResults = new ArrayList();
        logExampleSourceCommandStart(trackingService);
        expectedResults.add(logExampleUserCommandStart(trackingService));
        expectedResults.add(logExampleUserCommandEnd(trackingService));
        logExampleSourceCommandCancelled(trackingService);
        trackingService.stop();
        assertEquals(expectedResults, ((FakeCommandLogger)trackingService.getCommandLogger()).logEntries);
    }      
    
    // ========================================================================================================
    // test utilities
    // ========================================================================================================

    private CustomizableTrackingService getTrackingService(boolean willRecordTransactions, 
                                               boolean willRecordUserCommands, boolean willRecordSourceCommands) throws ApplicationInitializationException, ApplicationLifecycleException {
        
        CustomizableTrackingService service = new CustomizableTrackingService();
        Properties p = new Properties();
    	p.setProperty(CustomizableTrackingService.SYSTEM_TXN_STORE_SRCCMD, String.valueOf(willRecordSourceCommands));
    	p.setProperty(CustomizableTrackingService.SYSTEM_TXN_STORE_MMCMD, String.valueOf(willRecordUserCommands));
    	p.setProperty(CustomizableTrackingService.SYSTEM_TXN_STORE_TXN, String.valueOf(willRecordTransactions));
    	p.setProperty(DQPConfigSource.COMMAND_LOGGER_CLASSNAME, FakeCommandLogger.class.getName());
    	service.initialize(p);
        service.start(null);
        return service;
    }
    
    private List logExampleUserCommandStart(TrackingService trackingService) {
        
        String requestID = "req1"; //$NON-NLS-1$
        String transactionID = "35"; //$NON-NLS-1$
        short cmdPoint = TrackerLogConstants.CMD_POINT.BEGIN;
        short status = TrackerLogConstants.CMD_STATUS.NEW;
        String sessionID = "ses1"; //$NON-NLS-1$
        String applicationName = "myApp"; //$NON-NLS-1$
        String principal = "stuart"; //$NON-NLS-1$
        String vdbName = "myVDB"; //$NON-NLS-1$
        String vdbVersion = "2"; //$NON-NLS-1$
        Command sql = TestQuery.sample1();
        int rows = 10000;
        
        trackingService.log(requestID, transactionID, cmdPoint, status, sessionID, applicationName, principal, vdbName, vdbVersion, sql, rows);
        
        List expectedLogEntry = new ArrayList();
        expectedLogEntry.add(requestID);
        expectedLogEntry.add(transactionID);
        expectedLogEntry.add(sessionID);
        expectedLogEntry.add(applicationName);
        expectedLogEntry.add(principal);
        expectedLogEntry.add(vdbName);
        expectedLogEntry.add(vdbVersion);
        expectedLogEntry.add(sql.toString());
        
        return expectedLogEntry;
    }    
    
    private List logExampleUserCommandEnd(TrackingService trackingService) {
        
        String requestID = "req1"; //$NON-NLS-1$
        String transactionID = "35"; //$NON-NLS-1$
        short cmdPoint = TrackerLogConstants.CMD_POINT.END;
        short status = TrackerLogConstants.CMD_STATUS.END;
        String sessionID = "ses1"; //$NON-NLS-1$
        String applicationName = "myApp"; //$NON-NLS-1$
        String principal = "stuart"; //$NON-NLS-1$
        String vdbName = "myVDB"; //$NON-NLS-1$
        String vdbVersion = "2"; //$NON-NLS-1$
        Command sql = null;
        int rows = 10000;
        boolean isCancelled = false;
        boolean errorOccurred = false;
        
        trackingService.log(requestID, transactionID, cmdPoint, status, sessionID, applicationName, principal, vdbName, vdbVersion, sql, rows);
        
        List expectedLogEntry = new ArrayList();
        expectedLogEntry.add(requestID);
        expectedLogEntry.add(transactionID);
        expectedLogEntry.add(sessionID);
        expectedLogEntry.add(principal);
        expectedLogEntry.add(vdbName);
        expectedLogEntry.add(vdbVersion);
        expectedLogEntry.add(new Integer(rows));
        expectedLogEntry.add(Boolean.valueOf(isCancelled));
        expectedLogEntry.add(Boolean.valueOf(errorOccurred));
        
        return expectedLogEntry;
    }    
    
    private List logExampleSourceCommandStart(TrackingService trackingService) {
        String requestID = "req1"; //$NON-NLS-1$
        long sourceCommandID = 2112l;
        String subTransactionID = "42"; //$NON-NLS-1$
        String modelName = "myModel"; //$NON-NLS-1$
        String connectorBindingName = "myBinding"; //$NON-NLS-1$
        String sessionID = "ses1"; //$NON-NLS-1$
        String principal = "stuart"; //$NON-NLS-1$
        Command sql = TestQuery.sample1();
        short cmdPoint = TrackerLogConstants.CMD_POINT.BEGIN;
        short status = TrackerLogConstants.CMD_STATUS.NEW;
        int finalRowCount = 3;
        
        trackingService.log(requestID, sourceCommandID, subTransactionID, status, modelName, connectorBindingName, 
                            cmdPoint, sessionID, principal, sql, finalRowCount, null);
        
        List expectedLogEntry = new ArrayList();
        expectedLogEntry.add(requestID);
        expectedLogEntry.add(new Long(sourceCommandID));
        expectedLogEntry.add(subTransactionID);
        expectedLogEntry.add(modelName);
        expectedLogEntry.add(connectorBindingName);
        expectedLogEntry.add(sessionID);
        expectedLogEntry.add(principal);
        expectedLogEntry.add(sql.toString());
        
        return expectedLogEntry;
    }
    
    private List logExampleSourceCommandCancelled(TrackingService trackingService) {
        String requestID = "req1"; //$NON-NLS-1$
        long sourceCommandID = 3113l;
        String subTransactionID = "42"; //$NON-NLS-1$
        String modelName = "myModel"; //$NON-NLS-1$
        String connectorBindingName = "myBinding2"; //$NON-NLS-1$
        String sessionID = "ses1"; //$NON-NLS-1$
        String principal = "stuart"; //$NON-NLS-1$
        Command sql = null;
        short cmdPoint = TrackerLogConstants.CMD_POINT.END;
        short status = TrackerLogConstants.CMD_STATUS.CANCEL;
        int finalRowCount = 3;
        boolean isCancelled = true;
        boolean errorOccurred = false;

        trackingService.log(requestID, sourceCommandID, subTransactionID, status, modelName, connectorBindingName, 
                            cmdPoint, sessionID, principal, sql, finalRowCount, null);
        
        List expectedLogEntry = new ArrayList();
        expectedLogEntry.add(requestID);
        expectedLogEntry.add(new Long(sourceCommandID));
        expectedLogEntry.add(subTransactionID);
        expectedLogEntry.add(modelName);
        expectedLogEntry.add(connectorBindingName);
        expectedLogEntry.add(sessionID);
        expectedLogEntry.add(principal);
        expectedLogEntry.add(new Integer(finalRowCount));
        expectedLogEntry.add(Boolean.valueOf(isCancelled));
        expectedLogEntry.add(Boolean.valueOf(errorOccurred));
        
        return expectedLogEntry;
    }    
    


    /**
     * Fake implementation of CommandLoggerSPI to use for testing.  
     * Logged entries are cached in memory in Lists for later comparison
     * with expected results. 
     */
    public static class FakeCommandLogger implements CommandLoggerSPI {

        
        private List logEntries = new ArrayList();
        
        /** 
         * @see com.metamatrix.dqp.spi.CommandLoggerSPI#initialize(java.util.Properties)
         */
        public void initialize(Properties props) {
        }
        /** 
         * @see com.metamatrix.dqp.spi.CommandLoggerSPI#close()
         */
        public void close() {
        }

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
            
            List logEntry = new ArrayList(12);
            logEntry.add(requestID);
            logEntry.add(new Long(sourceCommandID));
            logEntry.add(subTransactionID);
            logEntry.add(modelName);
            logEntry.add(connectorBindingName);
            logEntry.add(sessionID);
            logEntry.add(principal);
            logEntry.add(sql);
            logEntries.add(logEntry);
        }        
        
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
            
            List logEntry = new ArrayList(12);
            logEntry.add(requestID);
            logEntry.add(new Long(sourceCommandID));
            logEntry.add(subTransactionID);
            logEntry.add(modelName);
            logEntry.add(connectorBindingName);
            logEntry.add(sessionID);
            logEntry.add(principal);
            logEntry.add(new Integer(finalRowCount));
            logEntry.add(Boolean.valueOf(isCancelled));
            logEntry.add(Boolean.valueOf(errorOccurred));
            logEntries.add(logEntry);
        }

        public void transactionStart(long timestamp,
                                     String transactionID,
                                     String sessionID,
                                     String principal,
                                     String vdbName,
                                     String vdbVersion) {
            
            List logEntry = new ArrayList(8);
            logEntry.add(transactionID);
            logEntry.add(sessionID);
            logEntry.add(principal);
            logEntry.add(vdbName);
            logEntry.add(vdbVersion);
            logEntries.add(logEntry);
        }

        public void transactionEnd(long timestamp,
                                   String transactionID,
                                   String sessionID,
                                   String principal,
                                   String vdbName,
                                   String vdbVersion,
                                   boolean commit) {
            
            List logEntry = new ArrayList(8);
            logEntry.add(transactionID);
            logEntry.add(sessionID);
            logEntry.add(principal);
            logEntry.add(vdbName);
            logEntry.add(vdbVersion);
            logEntry.add(Boolean.valueOf(commit));
            logEntries.add(logEntry);
        }
        
        public void userCommandStart(long timestamp,
                                     String requestID,
                                     String transactionID,
                                     String sessionID,
                                     String applicationName,
                                     String principal,
                                     String vdbName,
                                     String vdbVersion,
                                     String sql) {
            
            List logEntry = new ArrayList(10);
            logEntry.add(requestID);
            logEntry.add(transactionID);
            logEntry.add(sessionID);
            logEntry.add(applicationName);
            logEntry.add(principal);
            logEntry.add(vdbName);
            logEntry.add(vdbVersion);
            logEntry.add(sql);
            logEntries.add(logEntry);            
        }
        
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
            
            List logEntry = new ArrayList(10);
            logEntry.add(requestID);
            logEntry.add(transactionID);
            logEntry.add(sessionID);
            logEntry.add(principal);
            logEntry.add(vdbName);
            logEntry.add(vdbVersion);
            logEntry.add(new Integer(finalRowCount));
            logEntry.add(Boolean.valueOf(isCancelled));
            logEntry.add(Boolean.valueOf(errorOccurred));
            logEntries.add(logEntry);            
        }
    }
}
