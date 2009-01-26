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

/*
 */
package com.metamatrix.data.pool;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.monitor.AliveStatus;

public class TestConnectionPool extends TestCase{
    private ConnectionPool userIDPool;
    private ConnectionPool singleIDPool;
    private DisabledConnectionPool disabledPool;
    private static ArrayList<Exception> EXCEPTIONS = new ArrayList<Exception>();
        
    public TestConnectionPool(String name) {
        super(name);
    	System.setProperty("metamatrix.config.none", "true");

    }
    
    public void setUp() throws Exception{
        FakeSourceConnectionFactory.alive = true;        
        FakeSourceConnectionFactory.failed = false;
        
        singleIDPool = new ConnectionPool(new FakeSingleIdentityConnectionFactory());
        userIDPool = new ConnectionPool(new FakeUserIdentityConnectionFactory());
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "10"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "5"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, "true"); //$NON-NLS-1$
        poolProperties.put(SourceConnection.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        
        
        Properties poolProperties2 = new Properties();
        disabledPool = new DisabledConnectionPool(new FakeSingleIdentityConnectionFactory());
        
        singleIDPool.initialize(poolProperties);
        userIDPool.initialize(poolProperties);
        disabledPool.initialize(poolProperties2);
    }
    
    public void tearDown() throws Exception{
        singleIDPool.shutDown();
        userIDPool.shutDown();
        disabledPool.shutDown();
    }
        
    public static SecurityContext createContext(final String user) {
        SecurityContext context = new SecurityContext() {
            public String getVirtualDatabaseName() {
                return null;
            }

            public String getVirtualDatabaseVersion() {
                return null;
            }

            public String getUser() {
                return user;
            }

            public Serializable getTrustedPayload() {
                return null;
            }

            public Serializable getExecutionPayload() {
                return null;
            }

            public String getRequestIdentifier() {
                return "1"; //$NON-NLS-1$
            }

            public String getPartIdentifier() {
                return "1"; //$NON-NLS-1$
            }

//            public SecurityContext getSecurityContext() {
//                return null;
//            }

			public String getConnectorIdentifier() {
				return null;
			}
			
			public String getConnectionIdentifier() {
				return null;
			}
			
			public String getExecutionCountIdentifier() {
				return "1"; //$NON-NLS-1$
			}
			
			public boolean useResultSetCache() {
				return false;
			}
			
		    public void keepExecutionAlive(boolean alive) {
		    	
		    }

        };

        return context;        
    }
    
    //=== tests ===//
    public void testPoolUsingSingleIdentity() throws Exception {
        SecurityContext context = createContext("x");//$NON-NLS-1$

        SourceConnection conn1 = singleIDPool.obtain(context);
        SourceConnection conn2 = singleIDPool.obtain(context);
        SourceConnection conn3 = singleIDPool.obtain(context);
        SourceConnection conn4 = singleIDPool.obtain(context);         
        singleIDPool.release(conn2);
        singleIDPool.error(conn4);
        
        List unusedConns = singleIDPool.getUnusedConnections(conn1);
        assertEquals(1, unusedConns.size());
        assertTrue(unusedConns.contains(conn2));
        List usedConns = singleIDPool.getUsedConnections(conn2);
        assertEquals(2, usedConns.size());
        assertTrue(usedConns.contains(conn1));
        assertTrue(usedConns.contains(conn3));    
        assertEquals(3, singleIDPool.getTotalConnectionCount());        
    }

    public void testDisabledPool() throws Exception {
        SecurityContext context = createContext("x");//$NON-NLS-1$

        SourceConnection conn1 = disabledPool.obtain(context);
        
        assertEquals(AliveStatus.ALIVE, disabledPool.getStatus().getStatus());
        SourceConnection conn2 = disabledPool.obtain(context);
        SourceConnection conn3 = disabledPool.obtain(context);
        SourceConnection conn4 = disabledPool.obtain(context);         
        assertEquals(AliveStatus.ALIVE, disabledPool.getStatus().getStatus());
        disabledPool.release(conn1);
        disabledPool.release(conn2);
        disabledPool.release(conn3);
        disabledPool.error(conn4);
    }
    
    public void testMaxConnectionTest() throws Exception {
        ConnectionPool pool = new ConnectionPool(new FakeSingleIdentityConnectionFactory());
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "0"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "5"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, "true"); //$NON-NLS-1$
        poolProperties.put(SourceConnection.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        
        try {
            pool.initialize(poolProperties);
            fail("should have failed to use 0 as max connections"); //$NON-NLS-1$
        }catch (ConnectionPoolException e) {
            // pass
        }
    }
    
    public void testMaxConnectionTest1() throws Exception {
        ConnectionPool pool = new ConnectionPool(new FakeSingleIdentityConnectionFactory());
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "-1"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "5"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, "true"); //$NON-NLS-1$
        poolProperties.put(SourceConnection.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        
        try {
            pool.initialize(poolProperties);
            fail("should have failed to use 0 as max connections"); //$NON-NLS-1$
        }catch (ConnectionPoolException e) {
            // pass
        }
    }
    
    public void testMaxConnectionTest2() throws Exception {
        ConnectionPool pool = new ConnectionPool(new FakeUserIdentityConnectionFactory());
        
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "10"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "0"); //$NON-NLS-1$ 
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "500"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, "true"); //$NON-NLS-1$
        poolProperties.put(SourceConnection.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        
        try {
            pool.initialize(poolProperties);
            fail("should have failed to use 0 as max connections"); //$NON-NLS-1$
        }catch (ConnectionPoolException e) {
            // pass
        }
    }

    public void testMessageWhenPoolMaxedOutPerIdentity() throws Exception {
        SecurityContext context = createContext("x");//$NON-NLS-1$

        // Max out the pool - 5 connections for same ID
        for(int i=0; i<5; i++) {
            singleIDPool.obtain(context);
        }
        
        // Ask for one more - this should time out
        try {
            singleIDPool.obtain(context);
            fail("No exception received when maxing out the pool"); //$NON-NLS-1$
        } catch(ConnectionPoolException e) {
            assertEquals("The connection pool for identity \"SingleIdentity: atomic-request=1.1.1\" is at the maximum connection count \"5\" and no connection became available in the timeout period.  Consider increasing the number of connections allowed per identity or the wait time.", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testMessageWhenPoolTimedOut() throws Exception {
        FakeSourceConnectionFactory.alive = true;        
        FakeSourceConnectionFactory.failed = false;
        
        singleIDPool = new ConnectionPool(new FakeSingleIdentityConnectionFactory());
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, "true"); //$NON-NLS-1$
        poolProperties.put(SourceConnection.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        singleIDPool.initialize(poolProperties);
        
        SecurityContext context = createContext("x");//$NON-NLS-1$

        singleIDPool.obtain(context);
        
        try {            
            // Ask for a connection... should time out.
            singleIDPool.obtain(context);
            
            fail("No exception received on pool timeout"); //$NON-NLS-1$           
        } catch (ConnectionPoolException e) {
            assertEquals(e.getMessage(), "The connection pool for identity \"SingleIdentity: atomic-request=1.1.1\" exceeded wait time for connection, \"1\" ms, and no connection became available in the timeout period.  Consider increasing the number of connections allowed per identity or the wait time."); //$NON-NLS-1$
        }
    }

    public void testPoolUsingUserIdentity() throws Exception {
        SecurityContext context1 = createContext("Jack"); //$NON-NLS-1$
        SecurityContext context2 = createContext("Tom"); //$NON-NLS-1$
        userIDPool.obtain(context1);
        SourceConnection conn2 = userIDPool.obtain(context1);
        SourceConnection conn3 = userIDPool.obtain(context2);
        SourceConnection conn4 = userIDPool.obtain(context2);         
        userIDPool.release(conn2);
        userIDPool.error(conn4);
        
        List unusedConns = userIDPool.getUnusedConnections(conn2);
        assertEquals(1, unusedConns.size());
        assertTrue(unusedConns.contains(conn2));
        List usedConns = userIDPool.getUsedConnections(conn3);
        assertEquals(1, usedConns.size());
        assertTrue(usedConns.contains(conn3));    
        assertEquals(3, userIDPool.getTotalConnectionCount());        
    }
    
    public void testPoolCleanUp() throws Exception {
        SecurityContext context = createContext("x");       //$NON-NLS-1$ 

        SourceConnection conn1 = singleIDPool.obtain(context);
        SourceConnection conn2 = singleIDPool.obtain(context);
        SourceConnection conn3 = singleIDPool.obtain(context);
        SourceConnection conn4 = singleIDPool.obtain(context);         
        singleIDPool.release(conn2);
        singleIDPool.error(conn4);
        
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
    
    public void testMultiThreading() throws Exception {
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
        
        SecurityContext context1 = createContext("Jack"); //$NON-NLS-1$
        SecurityContext context2 = createContext("Tom"); //$NON-NLS-1$

        //List unusedConns1 = pool.getUnusedConneections(id1);
        List usedConns1 = userIDPool.getUsedConnections(userIDPool.obtain(context1));

        assertEquals(1, usedConns1.size());
        //List unusedConns2 = pool.getUnusedConneections(id2);
        List usedConns2 = userIDPool.getUsedConnections(userIDPool.obtain(context2));
        assertEquals(1, usedConns2.size()); 
    }
    
    /**
     * Tests ConnectionPool.getStatus() with a SingleIdentity 
     * @since 4.3
     */
    public void testGetStatusSingleIdentity() throws Exception {
        //connection is open: status should be ALIVE
        singleIDPool.lastConnectionAttemptFailed = false;
        FakeSourceConnectionFactory.alive = true;
        FakeSourceConnectionFactory.failed = false;
        assertEquals(AliveStatus.ALIVE, singleIDPool.getStatus().aliveStatus); 
        
        //connection can't be reached: status should be DEAD
        singleIDPool.lastConnectionAttemptFailed = false;
        FakeSourceConnectionFactory.alive = false;
        FakeSourceConnectionFactory.failed = true;
        assertEquals(AliveStatus.DEAD, singleIDPool.getStatus().aliveStatus); 
        
        //connection can't be reached: status should be DEAD        
        singleIDPool.lastConnectionAttemptFailed = true;
        assertEquals(AliveStatus.DEAD, singleIDPool.getStatus().aliveStatus);
    }
    
    /**
     * Tests ConnectionPool.getStatus() with a UserIdentity 
     * @since 4.3
     */
    public void testGetStatusUserIdentity() throws Exception {
        userIDPool.lastConnectionAttemptFailed = false;

        //status should always be UNKNOWN
        userIDPool.lastConnectionAttemptFailed = false;
        FakeSourceConnectionFactory.alive = true;
        FakeSourceConnectionFactory.failed = false;
        assertEquals(AliveStatus.UNKNOWN, userIDPool.getStatus().aliveStatus); 
        
        //status should always be UNKNOWN
        userIDPool.lastConnectionAttemptFailed = false;
        FakeSourceConnectionFactory.alive = false;
        FakeSourceConnectionFactory.failed = true;
        assertEquals(AliveStatus.UNKNOWN, userIDPool.getStatus().aliveStatus);
        
        //status should always be UNKNOWN
        userIDPool.lastConnectionAttemptFailed = true;
        assertEquals(AliveStatus.UNKNOWN, userIDPool.getStatus().aliveStatus);

    }
    
    /**
     * Tests DisabledConnectionPool.getStatus() with a UserIdentity 
     * @since 4.3
     */
    public void testGetStatusDisabled() throws Exception {
    	// status for disabled pool is always alive
        assertEquals(AliveStatus.ALIVE, disabledPool.getStatus().aliveStatus); 
    }
    
    static class LoadRunner extends Thread {        
    	ConnectionPool pool;
        private int n;
        
        LoadRunner(ConnectionPool pool, int connPerRunner){
            n = connPerRunner;
            this.pool = pool;
        }
        
        public void run(){
            SecurityContext context1 = createContext("Jack"); //$NON-NLS-1$
            SecurityContext context2 = createContext("Tom"); //$NON-NLS-1$
            try {
            	SourceConnection conn = null;
                
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
                    pool.release(conn);
                }    
            } catch (ConnectionPoolException e) {
                TestConnectionPool.EXCEPTIONS.add(e);
            }
        }
    }
    
    public void testMaxWithUserPool() throws Exception {
        userIDPool = new ConnectionPool(new FakeUserIdentityConnectionFactory());   
        Properties poolProperties = new Properties();
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.MAX_CONNECTIONS_FOR_EACH_ID, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.LIVE_AND_UNUSED_TIME, "1000"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.WAIT_FOR_SOURCE_TIME, "1"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.CLEANING_INTERVAL, "1000"); //$NON-NLS-1$
        poolProperties.put(ConnectionPool.ENABLE_SHRINKING, "false"); //$NON-NLS-1$
        poolProperties.put(SourceConnection.SOURCE_CONNECTION_TEST_INTERVAL, "-1"); //$NON-NLS-1$
        userIDPool.initialize(poolProperties);
        
        SourceConnection conn = userIDPool.obtain(createContext("x")); //$NON-NLS-1$
        userIDPool.release(conn);

        assertEquals(1, userIDPool.getTotalConnectionCount());
        
        userIDPool.obtain(createContext("y")); //$NON-NLS-1$

        assertEquals(1, userIDPool.getTotalConnectionCount());
    }
    
}
