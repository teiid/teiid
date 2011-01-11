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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import javax.resource.spi.XATerminator;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Transaction;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.common.queue.FakeWorkManager;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.TransactionContext;

public class TestTransactionServer {

    private TransactionServerImpl server;
    private XATerminator xaTerminator;
    private TransactionManager tm;
	private javax.transaction.Transaction txn;
    
    private static final String THREAD1 = "abc1"; //$NON-NLS-1$
    private static final String THREAD2 = "abc2"; //$NON-NLS-1$

    private static final XidImpl XID1 = new XidImpl(0, new byte[] {
        1
    }, new byte[0]);
    private static final XidImpl XID2 = new XidImpl(0, new byte[] {
        2
    }, new byte[0]);

    @Before public void setUp() throws Exception {
        server = new TransactionServerImpl();
        xaTerminator = Mockito.mock(XATerminator.class);
        tm = Mockito.mock(TransactionManager.class);
        txn = Mockito.mock(javax.transaction.Transaction.class);
        Mockito.stub(tm.getTransaction()).toReturn(txn);
        Mockito.stub(tm.suspend()).toReturn(txn);
        server.setXaTerminator(xaTerminator);
        server.setTransactionManager(tm);
        server.setWorkManager(new FakeWorkManager());
    }

    /**
     * once in a local, cannot start a global
     */
    @Test public void testTransactionExclusion() throws Exception {
        server.begin(THREAD1);

        try {
            server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100, false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }

    /**
     * once in a global, cannot start a local
     */
    @Test public void testTransactionExclusion1() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100, false);

        try {
            server.begin(THREAD1);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }

    /**
     * global can only be started once
     */
    @Test public void testTransactionExclusion2() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);

        try {
            server.start(THREAD2, XID1, XAResource.TMNOFLAGS, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Global transaction Teiid-Xid global:1 branch:null format:0 already exists.", ex.getMessage()); //$NON-NLS-1$
        }
    }

    /**
     * global cannot be nested
     */
    @Test public void testTransactionExclusion3() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);

        try {
            server.start(THREAD1, XID2, XAResource.TMNOFLAGS, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }

    /**
     * local cannot be nested
     */
    @Test public void testTransactionExclusion4() throws Exception {
        server.begin(THREAD1);

        try {
            server.begin(THREAD1);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }
    
    /**
     * global cannot be nested
     */
    @Test public void testTransactionExclusion5() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.start(THREAD2, XID2, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD2, XID2, XAResource.TMSUCCESS,false);

        try {
            server.start(THREAD1, XID2, XAResource.TMJOIN, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }

    @Test public void testLocalCommit() throws Exception {
        server.begin(THREAD1);
        server.commit(THREAD1);
        
        Mockito.verify(tm).commit();

        try {
            server.commit(THREAD1);
        } catch (XATransactionException e) {
            assertEquals("No transaction found for client abc1.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testTwoPhaseCommit() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        server.commit(THREAD1, XID1, false, false);
        
        Mockito.verify(xaTerminator).commit(XID1, false);
    }     
    
    @Test public void testLocalRollback() throws Exception {
        server.begin(THREAD1);
        server.rollback(THREAD1);
        Mockito.verify(tm).rollback();
        
        try {
            server.rollback(THREAD1);
        } catch (XATransactionException e) {
            assertEquals("No transaction found for client abc1.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testConcurrentEnlistment() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);

        try {
            server.start(THREAD1, XID1, XAResource.TMJOIN, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Concurrent enlistment in global transaction Teiid-Xid global:1 branch:null format:0 is not supported.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }

    @Test public void testSuspend() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);

        try {
            server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client is not currently enlisted in transaction Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    @Test public void testSuspendResume() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);
        server.start(THREAD1, XID1, XAResource.TMRESUME, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);

        try {
            server.start(THREAD2, XID1, XAResource.TMRESUME, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Cannot resume, transaction Teiid-Xid global:1 branch:null format:0 was not suspended by client abc2.", ex.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testUnknownFlags() throws Exception {
        try {
            server.start(THREAD1, XID1, Integer.MAX_VALUE, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Unknown flags", ex.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testUnknownGlobalTransaction() throws Exception {
        try {
            server.end(THREAD1, XID1, XAResource.TMSUCCESS,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("No global transaction found for Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    @Test public void testPrepareWithSuspended() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);

        try {
            server.prepare(THREAD1, XID1,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Suspended work still exists on transaction Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    @Test public void testGetTransactionContext() throws Exception {
        assertSame(server.getOrCreateTransactionContext(THREAD1), server.getOrCreateTransactionContext(THREAD1));
    }
    
    @Test public void testGetTransactions() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.begin(THREAD2);
        
        assertEquals(2, server.getTransactions().size());
        
        server.commit(THREAD2);
        assertEquals(1, server.getTransactions().size());
        
        Transaction t = server.getTransactions().iterator().next();
        assertEquals(THREAD1, t.getAssociatedSession());
        assertNotNull(t.getId());
    }
    
    @Test public void testGlobalPrepare() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
    	server.prepare(THREAD1, XID1, false);
    	
    	Mockito.verify(xaTerminator).prepare(tc.getXid());
    	
    	server.commit(THREAD1, XID1, true, false);
    }
    
    @Test public void testGlobalPrepareFail() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMFAIL, false);
        Mockito.verify(txn).setRollbackOnly();
    }    
    
    @Test public void testGlobalOnePhaseCommit() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
        server.prepare(THREAD1, XID1, false);

		
		server.commit(THREAD1, XID1, true, false);
		Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }  
    
    @Test public void testGlobalOnePhaseCommit_force_prepare_through() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
		
		server.commit(THREAD1, XID1, true, false);
		
		Mockito.verify(xaTerminator).prepare(tc.getXid());
		Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }  
    
    @Test public void testGlobalOnePhaseCommit_force_prepare() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
		
		server.commit(THREAD1, XID1, true, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator).prepare(tc.getXid());
		Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }  
    
    
    @Test public void testGlobalOnePhase_teiid_multiple() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
        server.prepare(THREAD1, XID1, false);

		
		server.commit(THREAD1, XID1, true, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }    
    
    @Test public void testGlobalOnePhaseRoolback() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
        server.prepare(THREAD1, XID1, false);

		
		server.rollback(THREAD1, XID1, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator).rollback(tc.getXid());
    }     
    
    @Test public void testRequestCommit() throws Exception{
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	server.begin(tc);
    	server.commit(tc);
    	Mockito.verify(tm).commit();
    }
    
    @Test public void testRequestRollback() throws Exception{
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	server.begin(tc);
    	
    	server.rollback(tc);
    	Mockito.verify(tm).rollback();
    }     
    
    @Test public void testLocalCancel() throws Exception {
        server.begin(THREAD1);
        
        server.cancelTransactions(THREAD1, false);
        
        Mockito.verify(txn).setRollbackOnly();
    }  
    
    @Test public void testRequestCancel() throws Exception{
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	server.begin(tc);
    	
    	server.cancelTransactions(THREAD1, true);
    	Mockito.verify(txn).setRollbackOnly();
    }      
}
