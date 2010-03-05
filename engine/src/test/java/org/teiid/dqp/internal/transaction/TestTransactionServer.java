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

package org.teiid.dqp.internal.transaction;

import javax.resource.spi.XATerminator;
import javax.transaction.xa.XAResource;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.adminapi.Transaction;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.dqp.service.TransactionContext;

public class TestTransactionServer extends TestCase {

    private TransactionServerImpl server;
    private XATerminator xaTerminator;
    
    private static final String THREAD1 = "1"; //$NON-NLS-1$
    private static final String THREAD2 = "2"; //$NON-NLS-1$

    private static final MMXid XID1 = new MMXid(0, new byte[] {
        1
    }, new byte[0]);
    private static final MMXid XID2 = new MMXid(0, new byte[] {
        2
    }, new byte[0]);

    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        server = new TransactionServerImpl();
        TransactionProvider provider = Mockito.mock(TransactionProvider.class);
        xaTerminator = Mockito.mock(XATerminator.class);
        Mockito.stub(provider.getXATerminator()).toReturn(xaTerminator);
        server.setTransactionProvider(provider); 
        server.setXidFactory(new XidFactory());
    }

    /**
     * once in a local, cannot start a global
     */
    public void testTransactionExclusion() throws Exception {
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
    public void testTransactionExclusion1() throws Exception {
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
    public void testTransactionExclusion2() throws Exception {
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
    public void testTransactionExclusion3() throws Exception {
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
    public void testTransactionExclusion4() throws Exception {
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
    public void testTransactionExclusion5() throws Exception {
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

    public void testLocalCommit() throws Exception {
        server.begin(THREAD1);
        server.commit(THREAD1);

        try {
            server.commit(THREAD1);
        } catch (XATransactionException e) {
            assertEquals("No transaction found for client 1.", e.getMessage()); //$NON-NLS-1$
        }
    }

    public void testLocalSetRollback() throws Exception {
        TransactionContext tc = server.begin(THREAD1);
        tc.incrementPartcipatingSourceCount("s1");
        tc.setRollbackOnly();
        
        server.commit(THREAD1);
        
        Mockito.verify(xaTerminator).rollback(tc.getXid());
    }    
    
    public void testSinglePhaseCommit() throws Exception {
        TransactionContext tc = server.begin(THREAD1);
        tc.incrementPartcipatingSourceCount("S1");
        
        server.commit(THREAD1);
        
        Mockito.verify(xaTerminator).commit(tc.getXid(), true);
        
        tc = server.begin(THREAD1);
        tc.incrementPartcipatingSourceCount("S1");
        tc.incrementPartcipatingSourceCount("S1");
        
        server.commit(THREAD1);
        
        Mockito.verify(xaTerminator).commit(tc.getXid(), true);        
    }      
    
    public void testTwoPhaseCommit() throws Exception {
        TransactionContext tc = server.begin(THREAD1);
        tc.incrementPartcipatingSourceCount("S1");
        tc.incrementPartcipatingSourceCount("S2");
        
        server.commit(THREAD1);
        
        Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }     
    
    public void testLocalRollback() throws Exception {
        TransactionContext tc = server.begin(THREAD1);
        tc.incrementPartcipatingSourceCount("s1");
        server.rollback(THREAD1);
        Mockito.verify(xaTerminator).rollback(tc.getXid());
        
        try {
            server.rollback(THREAD1);
        } catch (XATransactionException e) {
            assertEquals("No transaction found for client 1.", e.getMessage()); //$NON-NLS-1$
        }
    }

    public void testConcurrentEnlistment() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);

        try {
            server.start(THREAD1, XID1, XAResource.TMJOIN, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Concurrent enlistment in global transaction Teiid-Xid global:1 branch:null format:0 is not supported.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }

    public void testSuspend() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);

        try {
            server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client is not currently enlisted in transaction Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testSuspendResume() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);
        server.start(THREAD1, XID1, XAResource.TMRESUME, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);

        try {
            server.start(THREAD2, XID1, XAResource.TMRESUME, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Cannot resume, transaction Teiid-Xid global:1 branch:null format:0 was not suspended by client 2.", ex.getMessage()); //$NON-NLS-1$
        }
    }

    public void testUnknownFlags() throws Exception {
        try {
            server.start(THREAD1, XID1, Integer.MAX_VALUE, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Unknown flags", ex.getMessage()); //$NON-NLS-1$
        }
    }

    public void testUnknownGlobalTransaction() throws Exception {
        try {
            server.end(THREAD1, XID1, XAResource.TMSUCCESS,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("No global transaction found for Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testPrepareWithSuspended() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);

        try {
            server.prepare(THREAD1, XID1,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Suspended work still exists on transaction Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testGetTransactionContext() throws Exception {
        assertSame(server.getOrCreateTransactionContext(THREAD1), server.getOrCreateTransactionContext(THREAD1));
    }
    
    public void testGetTransactions() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.begin(THREAD2);
        
        assertEquals(2, server.getTransactions().size());
        
        server.commit(THREAD2);
        assertEquals(1, server.getTransactions().size());
        
        Transaction t = server.getTransactions().iterator().next();
        assertEquals(Long.parseLong(THREAD1), t.getAssociatedSession());
        assertNotNull(t.getXid());
    }
    
    public void testGlobalPrepare() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
    	server.prepare(THREAD1, XID1, false);
    	
    	Mockito.verify(xaTerminator).prepare(tc.getXid());
    	
    	server.commit(THREAD1, XID1, true, false);
    }
    
    public void testGlobalPrepareFail() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMFAIL, false);
        
    	try {
			server.prepare(THREAD1, XID1, false);
			fail("should have failed to prepare as end resulted in TMFAIL");
		} catch (Exception e) {
		}
		
		server.forget(THREAD1, XID1, false);
    }    
    
    public void testGlobalOnePhaseCommit() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
    	tc.incrementPartcipatingSourceCount("S1");
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
        server.prepare(THREAD1, XID1, false);

		
		server.commit(THREAD1, XID1, true, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator).commit(tc.getXid(), true);
    }  
    
    public void testGlobalOnePhaseCommit_force_prepare_through() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
    	tc.incrementPartcipatingSourceCount("S1");
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
		
		server.commit(THREAD1, XID1, true, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator, Mockito.times(0)).prepare(tc.getXid());
		Mockito.verify(xaTerminator).commit(tc.getXid(), true);
    }  
    
    public void testGlobalOnePhaseCommit_force_prepare() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
    	tc.incrementPartcipatingSourceCount("S1");
    	tc.incrementPartcipatingSourceCount("S2");
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
		
		server.commit(THREAD1, XID1, true, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator).prepare(tc.getXid());
		Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }  
    
    
    public void testGlobalOnePhase_teiid_multiple() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
    	tc.incrementPartcipatingSourceCount("S1");
    	tc.incrementPartcipatingSourceCount("S2");
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
        server.prepare(THREAD1, XID1, false);

		
		server.commit(THREAD1, XID1, true, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }    
    
    public void testGlobalOnePhaseRoolback() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	
    	tc.incrementPartcipatingSourceCount("S1");
    	
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        
        server.prepare(THREAD1, XID1, false);

		
		server.rollback(THREAD1, XID1, false);
		
		// since there are two sources the commit is not single phase
		Mockito.verify(xaTerminator).rollback(tc.getXid());
    }     
    
    public void testLocalCommit_rollback() throws Exception {
        TransactionContext tc = server.begin(THREAD1);
        tc.incrementPartcipatingSourceCount("s1");
        tc.setRollbackOnly();
        server.commit(THREAD1);

        Mockito.verify(xaTerminator).rollback(tc.getXid());
    }    
    
    public void testLocalCommit_not_in_Tx() throws Exception {
        TransactionContext tc = server.begin(THREAD1);
        server.commit(THREAD1);

        Mockito.verify(xaTerminator,Mockito.times(0)).commit(tc.getXid(), true);
    }       
    
    public void testRequestCommit() throws Exception{
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	server.start(tc);
    	tc.incrementPartcipatingSourceCount("s1");
    	server.commit(tc);
    	Mockito.verify(xaTerminator,Mockito.times(0)).prepare(tc.getXid());
    	Mockito.verify(xaTerminator).commit(tc.getXid(), true);
    }
    
    public void testRequestCommit2() throws Exception{
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	server.start(tc);
    	tc.incrementPartcipatingSourceCount("s1");
    	tc.incrementPartcipatingSourceCount("s2");
    	server.commit(tc);
    	
    	Mockito.verify(xaTerminator).prepare(tc.getXid());
    	Mockito.verify(xaTerminator).commit(tc.getXid(), false);
    }    
    
    public void testRequestRollback() throws Exception{
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	server.start(tc);
    	tc.incrementPartcipatingSourceCount("s1");
    	tc.incrementPartcipatingSourceCount("s2");
    	
    	server.rollback(tc);
    	Mockito.verify(xaTerminator).rollback(tc.getXid());
    }     
    
    public void testLocalCancel() throws Exception {
        TransactionContext tc = server.begin(THREAD1);
        tc.incrementPartcipatingSourceCount("S1");
        tc.incrementPartcipatingSourceCount("S2");
        
        server.cancelTransactions(THREAD1, false);
        
        Mockito.verify(xaTerminator).rollback(tc.getXid());
    }  
    
    public void testRequestCancel() throws Exception{
    	TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
    	server.start(tc);
    	tc.incrementPartcipatingSourceCount("s1");
    	tc.incrementPartcipatingSourceCount("s2");
    	
    	 server.cancelTransactions(THREAD1, true);
    	Mockito.verify(xaTerminator).rollback(tc.getXid());
    }      
}
