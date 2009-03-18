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

package com.metamatrix.dqp.service;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.teiid.connector.api.ExecutionContext;
import org.teiid.dqp.internal.datamgr.impl.FakeTransactionService;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;
import com.metamatrix.dqp.spi.CommandLoggerSPI;
import com.metamatrix.dqp.spi.TrackerLogConstants;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.TestQuery;
import com.metamatrix.query.unittest.FakeMetadataFactory;

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
    
    /**
     * Test the tracking service as it is invoked by DQP during query processing
     * using a <code>String</code> as the query object.
     * <p>
     * This test creates a sample query statement of type <code>String</code> along 
     * with an instance of <code>TrackingService</code> returned by a call to 
     * <code>getTrackingService()</code> asking for a <code>TrackingService</code> that 
     * will log the user-command.  The query and <code>TrackingService</code> instance 
     * are passed to the helper method <code>logUserCommandViaDQP()</code> and the 
     * expected results returned from the helper method are compared to the actual 
     * results logged by <code>FakeCommandLogger</code>.
     * <p>
     * This test can only succeed if the log entry sent to <code>FakeCommandLogger</code>
     * from <code>DQPCore</code> matches the expected results returned by the 
     * helper method.
     * 
     * @see #getTrackingService(boolean, boolean, boolean)
     * @see #helpLogUserCommandViaDQP(TrackingService, Serializable)
     * @since 6
     * @throws Exception
     */
    public void testUserCommandFromDQP_String() throws Exception {
        String sql = "SELECT SQRT(100)"; //$NON-NLS-1$
    	CustomizableTrackingService trackingService = getTrackingService(false, true, false);
        helpLogUserCommandViaDQP(trackingService, sql);
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
    
    /**
     * Helper method that creates an instance of <code>DQPCore</code> and sends 
     * it a <code>RequestMessage</code> along with the value passed in <code>ts</code> 
     * so that <code>DQPCore</code> can use it as the tracking service to log the 
     * user-command <code>command</code> if <code>ts</code> is set to capture 
     * user-commands.  
     * <p>
     * If <code>ts</code> is set to record the user-command, this method will 
     * return two log entries.  The first log entry represents the START state 
     * of the user-command and the second represents the END state of the 
     * user command.  
     * <p>
     * <code>command</code> must be valid and DQP must be able to parse the query.  
     * Because this helper method does not actually build or establish any metadata 
     * for DQP or define any sources, <code>command</code> should only contain scalar 
     * functions or constant values for its symbols.  Because DQP can receive either 
     * a <code>String</code> or a <code>Command</code> object representing the 
     * user-command, <code>command</code> can be of either type.  <code>command</code> 
     * will be passed to the <code>RequestMessage</code> that is sent to <code>DQPCore</code> 
     * 
     * @param ts A configured and running instance of a <code>TrackingService</code>.
     * @param command The query representing the user-command.
     * @return If <code>ts</code> has been set to record the user-command, two log entries 
     *         should be returned.  Each entry is made up of one or more <code>Object</code> 
     *         types and is contained within a <code>List</code> object.  The final two log
     *         entries are also contained in a <code>List</code> object.
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     * @throws MetaMatrixProcessingException
     * @throws ApplicationLifecycleException
     */
    private void helpLogUserCommandViaDQP(CustomizableTrackingService ts, String command) throws InterruptedException, ExecutionException, TimeoutException, MetaMatrixProcessingException, ApplicationLifecycleException {
        String principal = "stuart"; //$NON-NLS-1$
        String vdbName = "bqt"; //$NON-NLS-1$
		String vdbVersion = "1"; //$NON-NLS-1$
		MetaMatrixSessionID sessionID = new MetaMatrixSessionID(1); 
        int requestID = 100;
        int finalRowCount = 1;
        List<Object> expectedStartLogEntry = new ArrayList<Object>();
        List<Object> expectedEndLogEntry = new ArrayList<Object>();
        List<List<Object>> expectedLogEntries = new ArrayList<List<Object>>();

        if ( ts.willRecordMMCmd() ) {
	        expectedStartLogEntry.add(sessionID + "." + requestID);	// Request ID //$NON-NLS-1$
	        expectedStartLogEntry.add(null);							// Transaction ID 
	        expectedStartLogEntry.add(sessionID.toString());			// Session ID
	        expectedStartLogEntry.add(null);							// Application Name
	        expectedStartLogEntry.add(principal);						// Principal Name
	        expectedStartLogEntry.add(vdbName);						// VDB Name
	        expectedStartLogEntry.add(vdbVersion);					// VDB Version
	        expectedStartLogEntry.add(command.toString());				// SQL
	        expectedLogEntries.add(expectedStartLogEntry);
	
	        expectedEndLogEntry.add(sessionID + "." + requestID);	// Request ID //$NON-NLS-1$
	        expectedEndLogEntry.add(null);							// Transaction ID 
	        expectedEndLogEntry.add(sessionID.toString());			// Session ID
	        expectedEndLogEntry.add(principal);						// Principal Name
	        expectedEndLogEntry.add(vdbName);							// VDB Name
	        expectedEndLogEntry.add(vdbVersion);						// VDB Version
	        expectedEndLogEntry.add(new Integer(finalRowCount));		// Expected Number of Rows
	        expectedEndLogEntry.add(Boolean.FALSE);					// isCanceled?
	        expectedEndLogEntry.add(Boolean.FALSE);					// wasError?
	        expectedLogEntries.add(expectedEndLogEntry);
        }
        
    	ApplicationEnvironment env = new ApplicationEnvironment();
        env.bindService(DQPServiceNames.BUFFER_SERVICE, new FakeBufferService());
        FakeMetadataService mdSvc = new FakeMetadataService();
		mdSvc.addVdb(vdbName, vdbVersion, FakeMetadataFactory.exampleBQTCached()); 
        env.bindService(DQPServiceNames.METADATA_SERVICE, mdSvc);
        env.bindService(DQPServiceNames.DATA_SERVICE, new AutoGenDataService());
        env.bindService(DQPServiceNames.TRANSACTION_SERVICE, new FakeTransactionService());
        env.bindService(DQPServiceNames.TRACKING_SERVICE, ts);
        FakeVDBService vdbService = new FakeVDBService();
        vdbService.addBinding(vdbName, vdbVersion, "BQT1", "mmuuid:blah", "BQT"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        vdbService.addBinding(vdbName, vdbVersion, "BQT2", "mmuuid:blah", "BQT"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        vdbService.addBinding(vdbName, vdbVersion, "BQT3", "mmuuid:blah", "BQT"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        vdbService.addModel(vdbName, vdbVersion, "BQT3", ModelInfo.PRIVATE, false); //$NON-NLS-1$
        env.bindService(DQPServiceNames.VDB_SERVICE, vdbService);

        DQPCore core = new DQPCore(env);
        core.start(new Properties());

        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName(vdbName);
        workContext.setVdbVersion(vdbVersion);
        workContext.setSessionId(sessionID);
        workContext.setUserName(principal);
        DQPWorkContext.setWorkContext(workContext);
        
        RequestMessage reqMsg = new RequestMessage(command);
        reqMsg.setCallableStatement(false);
        reqMsg.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        reqMsg.setFetchSize(10);
        reqMsg.setPartialResults(false);
        reqMsg.setExecutionId(requestID);

        ResultsMessage results = null;
        Future<ResultsMessage> message = null;
        
        // Execute reqMsg
        message = core.executeRequest(reqMsg.getExecutionId(), reqMsg);
        results = message.get(50000, TimeUnit.MILLISECONDS);
        assertNull("executeRequest resulted in " + results.getException(), results.getException()); //$NON-NLS-1$
        core.closeRequest(requestID);
        FakeCommandLogger fcl = (FakeCommandLogger)ts.getCommandLogger();
        //close is fully asynch, calling stop immediately may take effect before the work item is requeued
        synchronized (fcl) {
        	for (int i = 0; i < 10 && fcl.logEntries.size() != expectedLogEntries.size(); i++) {
        		fcl.wait(50);
        	}
		}
        core.stop();
        ts.stop();
        assertEquals(expectedLogEntries, fcl.logEntries);
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

        String sqlStr = sql != null ? sql.toString() : null;
        trackingService.log(requestID, transactionID, cmdPoint, status, sessionID, applicationName, principal, vdbName, vdbVersion, sqlStr, rows);
        
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
        
        String sqlStr = sql != null ? sql.toString() : null;
        trackingService.log(requestID, transactionID, cmdPoint, status, sessionID, applicationName, principal, vdbName, vdbVersion, sqlStr, rows);
        
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
        
        String sqlStr = sql != null ? sql.toString() : null;
        trackingService.log(requestID, sourceCommandID, subTransactionID, status, modelName, connectorBindingName, 
                            cmdPoint, sessionID, principal, sqlStr, finalRowCount, null);
        
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

        String sqlStr = sql != null ? sql.toString() : null;
        trackingService.log(requestID, sourceCommandID, subTransactionID, status, modelName, connectorBindingName, 
                            cmdPoint, sessionID, principal, sqlStr, finalRowCount, null);
        
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

        public synchronized void dataSourceCommandStart(long timestamp,
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
            notifyAll();
        }        
        
        public synchronized void dataSourceCommandEnd(long timestamp,
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
            notifyAll();
        }

        public synchronized void userCommandStart(long timestamp,
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
            notifyAll();
        }
        
        public synchronized void userCommandEnd(long timestamp,
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
            notifyAll();
        }
    }
}
