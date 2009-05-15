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

package org.teiid.dqp.internal.pooling.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MappedUserIdentity;
import org.teiid.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import org.teiid.dqp.internal.datamgr.impl.ConnectorWrapper;
import org.teiid.dqp.internal.datamgr.impl.ExecutionContextImpl;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;


public class TestConnectionPool {
    private ConnectionPool userIDPool;
    private ConnectionPool singleIDPool;
    private static ArrayList<Exception> EXCEPTIONS = new ArrayList<Exception>();
    private static WorkerPool pool = WorkerPoolFactory.newWorkerPool(TestConnectionPool.class.getSimpleName(), 1);
    
    @AfterClass public static void tearDownOnce() {
    	pool.shutdownNow();
    }
    
    @Before public void setUp() throws Exception{
        FakeSourceConnectionFactory.alive = true;        
        
        singleIDPool = new ConnectionPool(new ConnectorWrapper(new FakeSourceConnectionFactory()));
        userIDPool = new ConnectionPool(new ConnectorWrapper(new FakeUserIdentityConnectionFactory()));
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "10"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "5"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        ConnectorEnvironment env = createConnectorEnvironment(poolProperties);
        singleIDPool.initialize(env);
        userIDPool.initialize(env);
    }

	private ConnectorEnvironment createConnectorEnvironment(
			Properties poolProperties) {
		ConnectorEnvironment env = new ConnectorEnvironmentImpl(poolProperties, Mockito.mock(ConnectorLogger.class), new ApplicationEnvironment(), pool);
		return env;
	}
    
    public void tearDown() throws Exception{
        singleIDPool.shutDown();
        userIDPool.shutDown();
    }
        
    public static ExecutionContext createContext(final String user, boolean userIdentity) {
    	ExecutionContextImpl context = new ExecutionContextImpl(null, null, user, null, null, null, null, null, null, null);
    	if (userIdentity) {
    		context.setConnectorIdentity(new MappedUserIdentity(context.getUser(), null, null));
    	}
    	return context;
    }
    
    //=== tests ===//
    @Test public void testPoolUsingSingleIdentity() throws Exception {
        ExecutionContext context = createContext("x", false);//$NON-NLS-1$

        ConnectionWrapper conn1 = singleIDPool.obtain(context);
        ConnectionWrapper conn2 = singleIDPool.obtain(context);
        ConnectionWrapper conn3 = singleIDPool.obtain(context);
        ConnectionWrapper conn4 = singleIDPool.obtain(context);   
        
        assertEquals(4, singleIDPool.getTotalCreatedConnectionCount());
        assertEquals(4, singleIDPool.getTotalConnectionCount());
        assertEquals(4, singleIDPool.getNumberOfConnectionsInUse());
        assertEquals(0, singleIDPool.getNumberOfConnectinsWaiting());
        assertEquals(0, singleIDPool.getTotalDestroyedConnectionCount());

     

        
        singleIDPool.release(conn2, false);
        singleIDPool.release(conn4, true);
        
        // only 1 has been closed/destroyed  #4
        assertEquals(3, singleIDPool.getTotalConnectionCount());
        assertEquals(1, singleIDPool.getTotalDestroyedConnectionCount());
        assertEquals(1, singleIDPool.getNumberOfConnectinsWaiting());
        assertEquals(4, singleIDPool.getTotalCreatedConnectionCount());
        assertEquals(2, singleIDPool.getNumberOfConnectionsInUse());

        
        List unusedConns = singleIDPool.getUnusedConnections(conn1);
        assertEquals(1, unusedConns.size());
        assertTrue(unusedConns.contains(conn2));
        List usedConns = singleIDPool.getUsedConnections(conn2);
        assertEquals(2, usedConns.size());
        assertTrue(usedConns.contains(conn1));
        assertTrue(usedConns.contains(conn3));    
        assertEquals(3, singleIDPool.getTotalConnectionCount());  
    }

    @Test public void testMaxConnectionTest() throws Exception {
        ConnectionPool pool = new ConnectionPool(new ConnectorWrapper(new FakeSourceConnectionFactory()));
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "0"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "5"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        
        try {
            pool.initialize(createConnectorEnvironment(poolProperties));
            fail("should have failed to use 0 as max connections"); //$NON-NLS-1$
        }catch (ConnectionPoolException e) {
            // pass
        }
    }
    
    @Test public void testMaxConnectionTest1() throws Exception {
        ConnectionPool pool = new ConnectionPool(new ConnectorWrapper(new FakeSourceConnectionFactory()));
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "-1"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "5"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        
        try {
            pool.initialize(createConnectorEnvironment(poolProperties));
            fail("should have failed to use 0 as max connections"); //$NON-NLS-1$
        }catch (ConnectionPoolException e) {
            // pass
        }
    }
    
    @Test public void testMaxConnectionTest2() throws Exception {
        ConnectionPool pool = new ConnectionPool(new ConnectorWrapper(new FakeSourceConnectionFactory()));
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "10"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "0"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        
        try {
            pool.initialize(createConnectorEnvironment(poolProperties));
            fail("should have failed to use 0 as max connections"); //$NON-NLS-1$
        }catch (ConnectionPoolException e) {
            // pass
        }
    }

    @Test public void testMessageWhenPoolMaxedOutPerIdentity() throws Exception {
        ExecutionContext context = createContext("x", false);//$NON-NLS-1$

        // Max out the pool - 5 connections for same ID
        for(int i=0; i<5; i++) {
            singleIDPool.obtain(context);
        }
        
        // Ask for one more - this should time out
        try {
            singleIDPool.obtain(context);
            fail("No exception received when maxing out the pool"); //$NON-NLS-1$
        } catch(ConnectionPoolException e) {
            assertEquals("The connection pool for identity \"SingleIdentity\" is at the maximum connection count \"5\" and no connection became available in the timeout period.  Consider increasing the number of connections allowed per identity or the wait time.", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    @Test public void testMessageWhenPoolTimedOut() throws Exception {
        FakeSourceConnectionFactory.alive = true;        
        
        singleIDPool = new ConnectionPool(new ConnectorWrapper(new FakeSourceConnectionFactory()));
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, Boolean.FALSE.toString());
        poolProperties.put(ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        singleIDPool.initialize(createConnectorEnvironment(poolProperties));
        
        ExecutionContext context = createContext("x", false);//$NON-NLS-1$

        singleIDPool.obtain(context);
        
        try {            
            // Ask for a connection... should time out.
            singleIDPool.obtain(context);
            
            fail("No exception received on pool timeout"); //$NON-NLS-1$           
        } catch (ConnectionPoolException e) {
            assertEquals("The connection pool for identity \"SingleIdentity\" exceeded wait time for connection, \"1\" ms, and no connection became available in the timeout period.  Consider increasing the number of connections allowed per identity or the wait time.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testPoolUsingUserIdentity() throws Exception {
        ExecutionContext context1 = createContext("Jack", true); //$NON-NLS-1$
        ExecutionContext context2 = createContext("Tom", true); //$NON-NLS-1$
        userIDPool.obtain(context1);
        ConnectionWrapper conn2 = userIDPool.obtain(context1);
        ConnectionWrapper conn3 = userIDPool.obtain(context2);
        ConnectionWrapper conn4 = userIDPool.obtain(context2);      
        
        assertEquals(4, userIDPool.getTotalCreatedConnectionCount());
        assertEquals(4, userIDPool.getTotalConnectionCount());
        assertEquals(4, userIDPool.getNumberOfConnectionsInUse());
        assertEquals(0, userIDPool.getNumberOfConnectinsWaiting());
        assertEquals(0, userIDPool.getTotalDestroyedConnectionCount());
        
        
        
        userIDPool.release(conn2, false);
        userIDPool.release(conn4, true);
        
        // only 1 has been closed/destroyed  #4
        assertEquals(3, userIDPool.getTotalConnectionCount());
        assertEquals(1, userIDPool.getTotalDestroyedConnectionCount());
        assertEquals(1, userIDPool.getNumberOfConnectinsWaiting());
        assertEquals(4, userIDPool.getTotalCreatedConnectionCount());
        assertEquals(2, userIDPool.getNumberOfConnectionsInUse());
        
        
        List unusedConns = userIDPool.getUnusedConnections(conn2);
        assertEquals(1, unusedConns.size());
        assertTrue(unusedConns.contains(conn2));
        List usedConns = userIDPool.getUsedConnections(conn3);
        assertEquals(1, usedConns.size());
        assertTrue(usedConns.contains(conn3));    
        assertEquals(3, userIDPool.getTotalConnectionCount());        
    }
    
    @Test public void testPoolCleanUp() throws Exception {
        ExecutionContext context = createContext("x", false);       //$NON-NLS-1$ 

        ConnectionWrapper conn1 = singleIDPool.obtain(context);
        ConnectionWrapper conn2 = singleIDPool.obtain(context);
        ConnectionWrapper conn3 = singleIDPool.obtain(context);
        ConnectionWrapper conn4 = singleIDPool.obtain(context);         
        singleIDPool.release(conn2, false);
        singleIDPool.release(conn4, true);
        
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e1) {
        }
        
        List unusedConns = singleIDPool.getUnusedConnections(conn1);
        assertEquals(0, unusedConns.size());
        List usedConns = singleIDPool.getUsedConnections(conn1);
        assertEquals(2, usedConns.size());
        assertTrue(usedConns.contains(conn1));
        assertTrue(usedConns.contains(conn3));   
        assertEquals(2, singleIDPool.getTotalConnectionCount());           
    }
    
    @Test public void testMultiThreading() throws Exception {
    	EXCEPTIONS.clear();
        int runnerNumber = 20;
        int connPerRunner = 20;

        LoadRunner runner[] = new LoadRunner[runnerNumber];
        for(int i=0; i< runnerNumber; i++){
            runner[i] = new LoadRunner(userIDPool, connPerRunner);
        }
        
        for(int i=0; i< runnerNumber; i++){
            runner[i].start();
        }
        
        for(int i=0; i< runnerNumber; i++){
            try {
                runner[i].join();
            } catch (InterruptedException e) {
            }
        }
        
        assertEquals(EXCEPTIONS.toString(), 0, EXCEPTIONS.size());
        
        ExecutionContext context1 = createContext("Jack", true); //$NON-NLS-1$
        ExecutionContext context2 = createContext("Tom", true); //$NON-NLS-1$

        //List unusedConns1 = pool.getUnusedConneections(id1);
        List usedConns1 = userIDPool.getUsedConnections(userIDPool.obtain(context1));

        assertEquals(1, usedConns1.size());
        //List unusedConns2 = pool.getUnusedConneections(id2);
        List usedConns2 = userIDPool.getUsedConnections(userIDPool.obtain(context2));
        assertEquals(1, usedConns2.size()); 
    }
    
    static class LoadRunner extends Thread {        
    	ConnectionPool pool;
        private int n;
        
        LoadRunner(ConnectionPool pool, int connPerRunner){
            n = connPerRunner;
            this.pool = pool;
        }
        
        public void run(){
            ExecutionContext context1 = createContext("Jack", true); //$NON-NLS-1$
            ExecutionContext context2 = createContext("Tom", true); //$NON-NLS-1$
            try {
            	ConnectionWrapper conn = null;
                
                for(int i=0; i<n; i++) {
                	if(Math.random() < .5) {
                    	conn = pool.obtain(context1);
                    } else {
                        conn = pool.obtain(context2);
                    }
                    try {
    					Thread.sleep(5);
    				} catch (InterruptedException e) {
    					//ignore
    				}
                    pool.release(conn, false);
                }    
            } catch (ConnectionPoolException e) {
                TestConnectionPool.EXCEPTIONS.add(e);
            }
        }
    }
    
    @Test public void testMaxWithUserPool() throws Exception {
        userIDPool = new ConnectionPool(new ConnectorWrapper(new FakeUserIdentityConnectionFactory()));   
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1000"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1000"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, "false"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        userIDPool.initialize(createConnectorEnvironment(poolProperties));
        
        ConnectionWrapper conn = userIDPool.obtain(createContext("x", true)); //$NON-NLS-1$
        userIDPool.release(conn,false);

        assertEquals(1, userIDPool.getTotalConnectionCount());
        
        userIDPool.obtain(createContext("y", true)); //$NON-NLS-1$

        assertEquals(1, userIDPool.getTotalConnectionCount());
    }
        
}
