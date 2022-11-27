/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Transaction;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.resource.api.XAImporter;

public class TestTransactionServer {

    private TransactionServerImpl server;
    private XAImporter xaImporter;
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

    static int TIMEOUT = 100;

    @Before public void setUp() throws Exception {
        server = new TransactionServerImpl();
        tm = Mockito.mock(TransactionManager.class);
        txn = Mockito.mock(javax.transaction.Transaction.class);
        Mockito.stub(tm.getTransaction()).toReturn(txn);
        Mockito.stub(tm.suspend()).toReturn(txn);
        xaImporter = Mockito.mock(XAImporter.class);
        Mockito.stub(xaImporter.importTransaction(Mockito.any(), Mockito.any(), Mockito.eq(TIMEOUT))).toReturn(txn);
        server.setXaImporter(xaImporter);
        server.setTransactionManager(tm);
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
            assertEquals("TEIID30517 Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
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
            assertEquals("TEIID30517 Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
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
            assertEquals("TEIID30522 Global transaction Teiid-Xid global:1 branch:null format:0 already exists.", ex.getMessage()); //$NON-NLS-1$
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
            assertEquals("TEIID30517 Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
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
            assertEquals("TEIID30517 Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
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
            assertEquals("TEIID30517 Client thread already involved in a transaction. Transaction nesting is not supported. The current transaction must be completed first.", //$NON-NLS-1$
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
            assertEquals("TEIID30526 javax.transaction.InvalidTransactionException: No transaction found for client abc1.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testTwoPhaseCommit() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);
        server.commit(THREAD1, XID1, false, false);

        Mockito.verify(xaImporter).commit(XID1, false);
    }

    @Test public void testLocalRollback() throws Exception {
        server.begin(THREAD1);
        server.rollback(THREAD1);
        Mockito.verify(tm).rollback();

        try {
            server.rollback(THREAD1);
        } catch (XATransactionException e) {
            assertEquals("TEIID30526 javax.transaction.InvalidTransactionException: No transaction found for client abc1.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testConcurrentEnlistment() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);

        try {
            server.start(THREAD1, XID1, XAResource.TMJOIN, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("TEIID30525 Concurrent enlistment in global transaction Teiid-Xid global:1 branch:null format:0 is not supported.", //$NON-NLS-1$
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
            assertEquals("TEIID30524 Client is not currently enlisted in transaction Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
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
            assertEquals("TEIID30518 Cannot resume, transaction Teiid-Xid global:1 branch:null format:0 was not suspended by client abc2.", ex.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testUnknownFlags() throws Exception {
        try {
            server.start(THREAD1, XID1, Integer.MAX_VALUE, 100,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("TEIID30519 Unknown START flags", ex.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testUnknownGlobalTransaction() throws Exception {
        try {
            server.end(THREAD1, XID1, XAResource.TMSUCCESS,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("TEIID30521 No global transaction found for Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testPrepareWithSuspended() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        server.end(THREAD1, XID1, XAResource.TMSUSPEND,false);

        try {
            server.prepare(THREAD1, XID1,false);
            fail("exception expected"); //$NON-NLS-1$
        } catch (XATransactionException ex) {
            assertEquals("TEIID30505 Suspended work still exists on transaction Teiid-Xid global:1 branch:null format:0.", ex.getMessage()); //$NON-NLS-1$
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

        Mockito.verify(xaImporter).prepare(tc.getXid());

        server.commit(THREAD1, XID1, true, false);
    }

    @Test public void testGlobalPrepareFail() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, TIMEOUT,false);
        server.end(THREAD1, XID1, XAResource.TMFAIL, false);
        Mockito.verify(txn).setRollbackOnly();
    }

    @Test public void testGlobalOnePhaseCommit() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);

        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);

        server.prepare(THREAD1, XID1, false);


        server.commit(THREAD1, XID1, true, false);
        Mockito.verify(xaImporter).commit(tc.getXid(), false);
    }

    @Test public void testGlobalOnePhaseCommit_force_prepare_through() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);

        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);


        server.commit(THREAD1, XID1, true, false);

        Mockito.verify(xaImporter).prepare(tc.getXid());
        Mockito.verify(xaImporter).commit(tc.getXid(), false);
    }

    @Test public void testGlobalOnePhaseCommit_force_prepare() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);

        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);


        server.commit(THREAD1, XID1, true, false);

        // since there are two sources the commit is not single phase
        Mockito.verify(xaImporter).prepare(tc.getXid());
        Mockito.verify(xaImporter).commit(tc.getXid(), false);
    }


    @Test public void testGlobalOnePhase_teiid_multiple() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);

        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);

        server.prepare(THREAD1, XID1, false);


        server.commit(THREAD1, XID1, true, false);

        // since there are two sources the commit is not single phase
        Mockito.verify(xaImporter).commit(tc.getXid(), false);
    }

    @Test public void testGlobalOnePhaseRoolback() throws Exception {
        server.start(THREAD1, XID1, XAResource.TMNOFLAGS, 100,false);
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);

        server.end(THREAD1, XID1, XAResource.TMSUCCESS, false);

        server.prepare(THREAD1, XID1, false);


        server.rollback(THREAD1, XID1, false);

        // since there are two sources the commit is not single phase
        Mockito.verify(xaImporter).rollback(tc.getXid());
    }

    @Test public void testRequestCommit() throws Exception{
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
        server.begin(tc);
        server.commit(tc);
        assertEquals(TransactionContext.Scope.NONE, tc.getTransactionType());
        Mockito.verify(tm).commit();
    }

    @Test public void testRequestRollback() throws Exception{
        TransactionContext tc = server.getOrCreateTransactionContext(THREAD1);
        server.begin(tc);

        server.rollback(tc);
        assertEquals(TransactionContext.Scope.NONE, tc.getTransactionType());
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
