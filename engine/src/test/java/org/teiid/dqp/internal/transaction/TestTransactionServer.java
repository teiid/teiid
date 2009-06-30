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

import javax.transaction.xa.XAResource;

import org.teiid.adminapi.Transaction;

import junit.framework.TestCase;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.util.SimpleMock;

public class TestTransactionServer extends TestCase {

    private TransactionServerImpl server;

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
        server.setTransactionProvider(SimpleMock.createSimpleMock(TransactionProvider.class)); 
    }

    /**
     * once in a local, cannot start a global
     */
    public void testTransactionExclusion() throws Exception {
        server.begin(THREAD1);

        try {
            server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);
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
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);

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
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);

        try {
            server.start(THREAD2, XID1, XAResource.TMNOFLAGS, 100);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Global transaction MMXid global:1 branch:null format:0 already exists.", ex.getMessage()); //$NON-NLS-1$
        }
    }

    /**
     * global cannot be nested
     */
    public void testTransactionExclusion3() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);

        try {
            server.start(THREAD1, XID2, XAResource.TMNOFLAGS, 100);
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
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);
        server.start(THREAD2, XID2, XAResource.TMNOFLAGS, 100);
        server.end(THREAD2, XID2, XAResource.TMSUCCESS);

        try {
            server.start(THREAD1, XID2, XAResource.TMJOIN, 100);
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

    public void testLocalRollback() throws Exception {
        server.begin(THREAD1);
        server.rollback(THREAD1);

        try {
            server.rollback(THREAD1);
        } catch (XATransactionException e) {
            assertEquals("No transaction found for client 1.", e.getMessage()); //$NON-NLS-1$
        }
    }

    public void testConcurrentEnlistment() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);

        try {
            server.start(THREAD1, XID1, XAResource.TMJOIN, 100);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Concurrent enlistment in global transaction MMXid global:1 branch:null format:0 is not supported.", //$NON-NLS-1$
                         ex.getMessage());
        }
    }

    public void testSuspend() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND);

        try {
            server.end(THREAD1, XID1, XAResource.TMSUSPEND);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Client is not currently enlisted in transaction MMXid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testSuspendResume() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND);
        server.start(THREAD1, XID1, XAResource.TMRESUME, 100);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND);

        try {
            server.start(THREAD2, XID1, XAResource.TMRESUME, 100);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Cannot resume, transaction MMXid global:1 branch:null format:0 was not suspended by client 2.", ex.getMessage()); //$NON-NLS-1$
        }
    }

    public void testUnknownFlags() throws Exception {
        try {
            server.start(THREAD1, XID1, Integer.MAX_VALUE, 100);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Unknown flags", ex.getMessage()); //$NON-NLS-1$
        }
    }

    public void testUnknownGlobalTransaction() throws Exception {
        try {
            server.end(THREAD1, XID1, XAResource.TMSUCCESS);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("No global transaction found for MMXid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testPrepareWithSuspended() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND);

        try {
            server.prepare(THREAD1, XID1);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("Suspended work still exists on transaction MMXid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testGetTransactionContext() throws Exception {
        assertSame(server.getOrCreateTransactionContext(THREAD1), server.getOrCreateTransactionContext(THREAD1));
    }
    
    public void testGetTransactions() throws Exception {
    	server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100);
        server.begin(THREAD2);
        
        assertEquals(2, server.getTransactions().size());
        
        server.commit(THREAD2);
        assertEquals(1, server.getTransactions().size());
        
        Transaction t = server.getTransactions().iterator().next();
        assertEquals(THREAD1, t.getAssociatedSession());
        assertNotNull(t.getXid());
    }
}
