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

package org.teiid.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings({"nls", "unchecked"})
public class TestTempTables extends TempTableTestHarness {

    private Transaction txn;
    private Synchronization synch;

    @Before public void setUp() {
        FakeDataManager fdm = new FakeDataManager();
        TestProcessor.sampleData1(fdm);
        this.setUp(RealMetadataFactory.example1Cached(), fdm);
    }

    @Test public void testRollbackNoExisting() throws Exception {
        setupTransaction(Connection.TRANSACTION_SERIALIZABLE);
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        execute("update x set e1 = e2 where e2 > 1", new List[] {Arrays.asList(2)}); //$NON-NLS-1$

        Mockito.verify(txn).registerSynchronization((Synchronization) Mockito.anyObject());
        synch.afterCompletion(Status.STATUS_ROLLEDBACK);

        try {
            execute("select * from x", new List[] {});
            fail();
        } catch (Exception e) {

        }
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
    }

    @Test public void testRollbackExisting() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        setupTransaction(Connection.TRANSACTION_SERIALIZABLE);
        //execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        for (int i = 0; i < 86; i++) {
            execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        }
        execute("update x set e1 = e2 where e2 > 1", new List[] {Arrays.asList(172)}); //$NON-NLS-1$

        synch.afterCompletion(Status.STATUS_ROLLEDBACK);

        execute("select * from x", new List[] {});
    }

    @Test public void testCommitExistingRemoved() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        setupTransaction(Connection.TRANSACTION_SERIALIZABLE);
        execute("drop table x", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        synch.afterCompletion(Status.STATUS_COMMITTED);
        try {
            execute("select * from x", new List[] {});
            fail();
        } catch (Exception e) {

        }
    }

    @Test public void testUpdateLock() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        setupTransaction(Connection.TRANSACTION_SERIALIZABLE);
        execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        tc = null;
        try {
            execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
            fail();
        } catch (Exception e) {

        }
        synch.afterCompletion(Status.STATUS_COMMITTED);
        execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
    }

    @Test public void testRollbackExisting1() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        for (int i = 0; i < 86; i++) {
            execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        }
        setupTransaction(Connection.TRANSACTION_SERIALIZABLE);
        //execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        for (int i = 0; i < 86; i++) {
            execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        }
        execute("update x set e1 = e2 where e2 > 1", new List[] {Arrays.asList(344)}); //$NON-NLS-1$

        synch.afterCompletion(Status.STATUS_ROLLEDBACK);
        this.tc = null;

        execute("select count(*) from x", new List[] {Arrays.asList(516)});

        execute("delete from x", new List[] {Arrays.asList(516)});
    }

    @Test public void testIsolateReads() throws Exception {
        GlobalTableStoreImpl gtsi = new GlobalTableStoreImpl(BufferManagerFactory.getStandaloneBufferManager(), RealMetadataFactory.example1Cached().getVdbMetaData(), RealMetadataFactory.example1Cached());
        tempStore = gtsi.getTempTableStore();
        metadata = new TempMetadataAdapter(RealMetadataFactory.example1Cached(), tempStore.getMetadataStore());
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        for (int i = 0; i < 300; i++) {
            execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        }
        setupTransaction(Connection.TRANSACTION_SERIALIZABLE);
        execute("select count(e1) from x", new List[] {Arrays.asList(1500)});
        gtsi.updateMatViewRow("X", Arrays.asList(2L), true);
        tc=null;
        //outside of the transaction we can see the row removed
        execute("select count(e1) from x", new List[] {Arrays.asList(1499)});

        //back in the transaction we see the original state
        setupTransaction(Connection.TRANSACTION_SERIALIZABLE);
        execute("select count(e1) from x", new List[] {Arrays.asList(1500)});

        synch.afterCompletion(Status.STATUS_COMMITTED);
    }

    private void setupTransaction(int isolation) throws RollbackException, SystemException {
        txn = Mockito.mock(Transaction.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                synch = (Synchronization)invocation.getArguments()[0];
                return null;
            }
        }).when(txn).registerSynchronization((Synchronization)Mockito.anyObject());
        Mockito.stub(txn.toString()).toReturn("txn");
        tc = new TransactionContext();
        tc.setTransaction(txn);
        tc.setIsolationLevel(isolation);
        tc.setTransactionType(Scope.REQUEST);
    }

    @Test public void testInsertWithQueryExpression() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) select e2, e1 from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        execute("update x set e1 = e2 where e2 > 1", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
    }

    @Test public void testOutofOrderInsert() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select e1, e2 from x", new List[] {Arrays.asList("one", 1)}); //$NON-NLS-1$
    }

    @Test public void testUpdate() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select e1, e2 into x from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        execute("update x set e1 = e2 where e2 > 1", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
        execute("select e1 from x where e2 > 0 order by e1", new List[] { //$NON-NLS-1$
                Arrays.asList((String)null),
                Arrays.asList("2"), //$NON-NLS-1$
                Arrays.asList("3"), //$NON-NLS-1$
                Arrays.asList("c"), //$NON-NLS-1$
                Arrays.asList("one")}); //$NON-NLS-1$
    }

    @Test public void testDelete() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("select e1, e2 into x from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        execute("delete from x where ascii(e1) > e2", new List[] {Arrays.asList(5)}); //$NON-NLS-1$
        execute("select e1 from x order by e1", new List[] {Arrays.asList((String)null)}); //$NON-NLS-1$
    }

    @Test public void testDelete1() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("select e1, e2 into x from pm1.g1", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        execute("delete from x", new List[] {Arrays.asList(6)}); //$NON-NLS-1$
        execute("select e1 from x order by e1", new List[] {}); //$NON-NLS-1$
    }

    @Test(expected=TeiidProcessingException.class) public void testDuplicatePrimaryKey() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
    }

    @Test public void testUpsert() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, e3 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1, e3) values (1, 'one', 2)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("upsert into x (e2, e1) values (1, 'x')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("upsert into x (e2, e1) values (2, 'two')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select e1, e2, e3 from x", new List[] {Arrays.asList("x", 1, 2), Arrays.asList("two", 2, null)}); //$NON-NLS-1$
    }

    @Test public void testUpsertFails() throws Exception {
        execute("create local temporary table x (e1 string not null, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("upsert into x (e2, e1) values (1, 'two'), (2, 'three')", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
        try {
            execute("upsert into x (e2, e1) values (1, 3), (1, 4), (1, rand() || null)", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
            fail();
        } catch (TeiidProcessingException e) {

        }
        execute("select e1, e2 from x", new List[] {Arrays.asList("3", 1), Arrays.asList("three", 2)}); //$NON-NLS-1$
    }

    @Test public void testAtomicUpdate() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        try {
            execute("update x set e2 = 3", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        } catch (TeiidProcessingException e) {
            //should be a duplicate key
        }
        //should revert back to original
        execute("select count(*) from x", new List[] {Arrays.asList(2)}); //$NON-NLS-1$

        Thread t = new Thread() {
            public void run() {
                try {
                    execute("select count(e1) from x", new List[] {Arrays.asList(2)});
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        t.start();
        t.join(2000);
        assertFalse(t.isAlive());
    }

    @Test public void testAtomicDelete() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        try {
            execute("delete from x where 1/(e2 - 2) <> 4", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        } catch (TeiidProcessingException e) {
            //should be a duplicate key
        }
        //should revert back to original
        execute("select count(*) from x", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
    }

    @Test public void testPrimaryKeyMetadata() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        Collection<?> c = metadata.getUniqueKeysInGroup(metadata.getGroupID("x"));
        assertEquals(1, c.size());
        assertEquals(1, (metadata.getElementIDsInKey(c.iterator().next()).size()));
    }

    @Test public void testProjection() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x where e2 = 1", new List[] {Arrays.asList("one", 1)}); //$NON-NLS-1$
        execute("select e2, e1 from x where e2 = 1", new List[] {Arrays.asList(1, "one")}); //$NON-NLS-1$
    }

    @Test public void testOrderByWithIndex() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'one')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x as y order by e2 desc", new List[] {Arrays.asList("one", 3), Arrays.asList("one", 2)}); //$NON-NLS-1$
        execute("select * from x as y where e2 in (2, 3) order by e2 desc", new List[] {Arrays.asList("one", 3), Arrays.asList("one", 2)}); //$NON-NLS-1$
        execute("select * from x as y where e2 in (3, 2) order by e2", new List[] {Arrays.asList("one", 2), Arrays.asList("one", 3)}); //$NON-NLS-1$
        execute("select * from x as y where e2 in (3, 2) order by e2 desc", new List[] {Arrays.asList("one", 3), Arrays.asList("one", 2)}); //$NON-NLS-1$
    }

    @Test public void testOrderByWithoutIndex() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x order by e1", new List[] {Arrays.asList("a", 3), Arrays.asList("b", 2)}); //$NON-NLS-1$
    }

    @Test public void testCompareEqualsWithIndex() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x where e2 = 3", new List[] {Arrays.asList("a", 3)}); //$NON-NLS-1$
    }

    @Test public void testCompareLessThan() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (4, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        ProcessorPlan plan = execute("select * from x where e2 < 4", new List[] {Arrays.asList("a", 3)}); //$NON-NLS-1$
        TestOptimizer.checkAtomicQueries(new String[] {"SELECT x.e1, x.e2 FROM x WHERE x.e2 < 4"}, plan);
    }

    @Test public void testLikeWithIndex() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x where e1 like 'z%'", new List[0]); //$NON-NLS-1$
    }

    @Test public void testLikeRegexWithIndex() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'ab')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x where e1 like_regex '^b?.*'", new List[] {Arrays.asList("ab", 3), Arrays.asList("b", 2)}); //$NON-NLS-1$
        execute("select * from x where e1 like_regex '^ab+.*'", new List[] {Arrays.asList("ab", 3)}); //$NON-NLS-1$
        execute("select * from x where e1 like_regex '^ab|b'", new List[] {Arrays.asList("ab", 3), Arrays.asList("b", 2)}); //$NON-NLS-1$
    }

    @Test public void testIsNullWithIndex() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, null)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x where e1 is null", new List[] {Arrays.asList(null, 3)}); //$NON-NLS-1$
    }

    @Test public void testInWithIndex() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'c')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (0, 'd')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (-1, 'e')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x where e1 in ('a', 'c', 'e', 'f', 'g')", new List[] {Arrays.asList("a", 3), Arrays.asList("c", 1), Arrays.asList("e", -1)}); //$NON-NLS-1$
    }

    @Test public void testInWithIndexUpdate() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, e3 string, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'c')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (0, 'd')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1, e3) values (-1, 'e', 'e')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("update x set e2 = 5 where e1 in ('a', 'c')", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
        execute("select * from x where e1 in ('b', e3)", new List[] {Arrays.asList("b", 2, null), Arrays.asList("e", -1, "e")}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyCompareEquals() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1, e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'c')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x where e1 = 'b' and e2 = 2", new List[] {Arrays.asList("b", 2)}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyPartial() throws Exception {
        sampleTable();
        execute("select * from x where e1 = 'b'", new List[] {Arrays.asList("b", 2), Arrays.asList("b", 3)}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyPartial1() throws Exception {
        sampleTable();
        execute("select * from x where e1 < 'c'", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("b", 3)}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyPartial2() throws Exception {
        sampleTable();
        execute("select * from x where e2 = 1", new List[] {Arrays.asList("a", 1), Arrays.asList("c", 1)}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyPartial3() throws Exception {
        sampleTable();
        execute("select * from x where e1 >= 'b'", new List[] {Arrays.asList("b", 2), Arrays.asList("b", 3), Arrays.asList("c", 1)}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyPartial4() throws Exception {
        sampleTable();
        execute("select * from x where e1 >= 'b' order by e1 desc, e2 desc", new List[] {Arrays.asList("c", 1), Arrays.asList("b", 3), Arrays.asList("b", 2)}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyPartial5() throws Exception {
        sampleTable();
        execute("select * from x where e1 in ('a', 'b')", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("b", 3)}); //$NON-NLS-1$
    }

    @Test public void testCompositeKeyPartial6() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1, e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("select * from x where e1 in ('a', 'b') order by e1 desc", new List[0]); //$NON-NLS-1$
    }

    @Test public void testCountStar() throws Exception {
        sampleTable();
        execute("select count(*) a from x", new List[] {Arrays.asList(4)});
        execute("select count(*) a from x where e2 = 1 order by a", new List[] {Arrays.asList(2)});
    }

    @Test public void testAutoIncrement() throws Exception {
        execute("create local temporary table x (e1 serial, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2) values (1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2) values (3)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x", new List[] {Arrays.asList(1, 1), Arrays.asList(2, 3)});
    }

    @Test public void testAutoIncrement1() throws Exception {
        execute("create local temporary table x (e1 serial, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2) values (1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2) values (3)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select * from x", new List[] {Arrays.asList(1, 1), Arrays.asList(2, 3)});
    }

    @Test(expected=TeiidProcessingException.class) public void testNotNull() throws Exception {
        execute("create local temporary table x (e1 serial, e2 integer not null, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e1, e2) values ((select null), 1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
    }

    @Test(expected=TeiidProcessingException.class) public void testNotNull1() throws Exception {
        execute("create local temporary table x (e1 serial, e2 integer not null, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2) values ((select null))", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
    }

    @Test(expected=TeiidProcessingException.class) public void testNotNull2() throws Exception {
        execute("create local temporary table x (e1 string not null, e2 integer, e3 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, (select null))", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
    }

    @Test(expected=TeiidProcessingException.class) public void testNotNullUpdate() throws Exception {
        execute("create local temporary table x (e1 string not null, e2 integer, e3 integer, primary key (e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("update x set e1 = (select null)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
    }

    /**
     * If the session metadata is still visible, then the procedure will fail due to the conflicting
     * definitions of temp_table
     */
    @Test public void testSessionResolving() throws Exception {
        execute("create local temporary table temp_table (column1 integer)", new List[] {Arrays.asList(0)});
        execute("exec pm1.vsp60()", new List[] {Arrays.asList("First"), Arrays.asList("Second"), Arrays.asList("Third")});
    }

    /**
     * Note that the order by reflects the key order, not the order in which the criteria was entered
     */
    @Test public void testCompositeKeyJoinUsesKeyOrder() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1, e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("create local temporary table x1 (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        TestOptimizer.helpPlan("select * from /*+ makenotdep */ x, /*+ makenotdep */ x1 where x.e2 = x1.e2 and x.e1 = x1.e1", this.metadata, new String[] {"SELECT x1.e2, x1.e1 FROM x1", "SELECT x.e2, x.e1 FROM x ORDER BY x.e1, x.e2"}, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testUnneededMergePredicate() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("create local temporary table x1 (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        TestOptimizer.helpPlan("select x.e1 from x makenotdep, x1 makenotdep where x.e2 = x1.e2 and x.e1 = x1.e1", this.metadata, new String[] {"SELECT x.e2, x.e1 FROM x ORDER BY x.e1", "SELECT x1.e2, x1.e1 FROM x1"}, ComparisonMode.EXACT_COMMAND_STRING);
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x1 (e2, e1) values (3, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("select x.e1 from x makenotdep, x1 makenotdep where x.e2 = x1.e2 and x.e1 = x1.e1", new List[0]); //$NON-NLS-1$
        //ensure join is preserved
        execute("select x.e1 from x left outer join x1 on x.e2 = x1.e2 and x.e1 = x1.e1", new List[] {Arrays.asList("b")}); //$NON-NLS-1$
    }

    @Test public void testUnneededMergePredicate1() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, e3 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("create local temporary table x1 (e1 string, e2 integer, e3 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        execute("insert into x (e3, e2, e1) values (4, 2, '1')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e3, e2, e1) values (4, 2, '2')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x1 (e3, e2, e1) values (5, 2, '1')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x1 (e3, e2, e1) values (5, 1, '2')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        //ensure predicates are preserved
        execute("select x.e1, upper(x.e1),  x1.e2 from /*+ makenotdep */ x inner join  /*+ makenotdep */ x1 on x.e1 = x1.e1 and upper(x.e1) = x1.e2", new List[0]); //$NON-NLS-1$
        execute("select x.e1 from /*+ makenotdep */ x inner join  /*+ makenotdep */ x1 on x.e1 = x1.e1 and upper(x.e1) = x1.e2 and x1.e3 = x.e3", new List[0]); //$NON-NLS-1$
        //ensure there's no bad interaction with makedep
        execute("select x.e1 from /*+ makedep */ x inner join  x1 on x.e1 = x1.e1 and x.e2 = x1.e2", new List[] {Arrays.asList("1")}); //$NON-NLS-1$
    }

    private void sampleTable() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1, e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (3, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'c')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
    }

    @Test public void testForeignTemp() throws Exception {
        HardcodedDataManager hdm = new HardcodedDataManager(metadata);
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        SessionAwareCache<CachedResults> cache = new SessionAwareCache<CachedResults>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
        cache.setTupleBufferCache(bm);
        dataManager = new TempTableDataManager(hdm, bm, cache);

        execute("create foreign temporary table x (e1 string options (nameinsource 'a'), e2 integer, e3 string, primary key (e1)) options (cardinality 1000, updatable true, \"other\" 'prop') on pm1", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        TempMetadataID id = this.tempStore.getMetadataStore().getData().get("x");

        //ensure that we're using the actual metadata
        assertNotNull(id);
        assertNotNull(this.metadata.getPrimaryKey(id));
        assertEquals(1000, this.metadata.getCardinality(id), 0);
        assertEquals("pm1", this.metadata.getName(this.metadata.getModelID(id)));
        assertEquals("prop", this.metadata.getExtensionProperty(id, "other", false));

        hdm.addData("SELECT x.a, x.e2, x.e3 FROM x", new List[] {Arrays.asList(1, 2, "3")});
        execute("select * from x", new List[] {Arrays.asList(1, 2, "3")}); //$NON-NLS-1$

        hdm.addData("SELECT g_0.e2 AS c_0, g_0.e3 AS c_1, g_0.a AS c_2 FROM x AS g_0 ORDER BY c_1, c_0", new List[] {Arrays.asList(2, "3", "1")});
        hdm.addData("SELECT g_0.e3, g_0.e2 FROM x AS g_0", new List[] {Arrays.asList("3", 2)});
        hdm.addData("DELETE FROM x WHERE x.a = '1'", new List[] {Arrays.asList(1)});

        //ensure compensation behaves as if physical - not temp
        execute("delete from x where e2 = (select max(e2) from x as z where e3 = x.e3)", new List[] {Arrays.asList(1)}, TestOptimizer.getGenericFinder()); //$NON-NLS-1$

        hdm.addData("SELECT g_0.e1 FROM g1 AS g_0, x AS g_1 WHERE g_1.a = g_0.e1", new List[] {Arrays.asList(1)});

        //ensure pushdown support
        execute("select g1.e1 from pm1.g1 g1, x where x.e1 = g1.e1", new List[] {Arrays.asList(1)}, TestOptimizer.getGenericFinder()); //$NON-NLS-1$

        try {
            execute("create local temporary table x (e1 string)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
            fail();
        } catch (QueryResolverException e) {

        }

        //ensure that drop works
        execute("drop table x", new List[] {Arrays.asList(0)});

        try {
            execute("drop table x", new List[] {Arrays.asList(0)});
            fail();
        } catch (QueryResolverException e) {

        }
    }

    @Test public void testInherentUpdateUsingTemp() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(
                "create foreign table g1 (e1 string primary key, e2 integer, e3 boolean, e4 double, FOREIGN KEY (e1) REFERENCES G2 (e1)) options (updatable true);"
                + " create foreign table g2 (e1 string primary key, e2 integer, e3 boolean, e4 double) options (updatable true);"
                + " create view v options (updatable true) as select g2.e1, g1.e2 from g1 inner join g2 on g1.e1 = g2.e1;"
                , "x", "pm1");
        HardcodedDataManager hdm = new HardcodedDataManager(metadata);
        setUp(metadata, hdm);

        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        SessionAwareCache<CachedResults> cache = new SessionAwareCache<CachedResults>("resultset", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.RESULTSET, 0);
        cache.setTupleBufferCache(bm);
        dataManager = new TempTableDataManager(hdm, bm, cache);

        execute("create temporary table x (e1 string, e2 integer, e3 string, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        execute("insert into x values ('a', 1, 'b')", new List[] {Arrays.asList(1)});

        TempMetadataID id = this.tempStore.getMetadataStore().getData().get("x");

        //ensure that we're using the actual metadata
        assertNotNull(id);
        assertNotNull(this.metadata.getPrimaryKey(id));

        hdm.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE g_0.e1 IS NOT NULL AND g_0.e2 = 1", new List[] {Arrays.asList("a")});
        hdm.addData("DELETE FROM g1 WHERE g1.e1 = 'a'", new List[] {Arrays.asList(1)});

        execute("delete from v where e2 = (select max(e2) from x as z where e3 = z.e3)", new List[] {Arrays.asList(1)}, TestOptimizer.getGenericFinder()); //$NON-NLS-1$
    }

    @Test public void testDependentArrayType() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, e3 integer, primary key (e2, e3))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("create local temporary table x1 (e1 string, e2 integer, e3 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        execute("insert into x (e3, e2, e1) values (4, 2, '1')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e3, e2, e1) values (3, 2, '2')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x1 (e3, e2, e1) values (4, 2, '1')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x1 (e3, e2, e1) values (3, 2, '2')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$

        //single value
        execute("select x.e1 from x inner join /*+ makeind */ x1 on x.e2 = x1.e2 and x.e3 = x1.e3 and x1.e1='1'", new List[] {Arrays.asList("1")}); //$NON-NLS-1$

        //multiple values
        execute("select x.e1 from x inner join /*+ makeind */ x1 on x.e2 = x1.e2 and x.e3 = x1.e3", new List[] {Arrays.asList("2"), Arrays.asList("1")}); //$NON-NLS-1$

        //multiple out of order values
        execute("select x.e1 from x inner join /*+ makeind */ x1 on x.e3 = x1.e3 and x.e2 = x1.e2", new List[] {Arrays.asList("2"), Arrays.asList("1")}); //$NON-NLS-1$
    }

    @Test public void testSubquery() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, e3 integer, primary key (e2, e3))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        execute("insert into x (e3, e2, e1) values (4, 2, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        execute("insert into x (e3, e2, e1) values (4, 3, '1')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$

        execute("update x set e1 = 1 where e1 in (select e1 from pm1.g1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$

    }

    @Test public void testIndexInPredicate() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        //the issue is only apparent when the values are from different pages
        for (int i = 0; i < 2048; i++) {
            execute("insert into x (e2, e1) values ("+i+", '"+i+"')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        }

        execute("select e2, e1 from x where e1 in ('2000', '1')", new List[] {Arrays.asList(1, "1"), Arrays.asList(2000, "2000")}); //$NON-NLS-1$

    }

    @Test public void testDeleteRemovingPage() throws Exception {
        helpTestDelete();

        //test under other buffer scenarios - with restricted memory / small batch

        FakeDataManager fdm = new FakeDataManager();
        TestProcessor.sampleData1(fdm);
        BufferManager bm = BufferManagerFactory.getTestBufferManager(100000, 10);
        setUp(RealMetadataFactory.example1Cached(), fdm, bm);

        helpTestDelete();

        //with restricted memory / normal batch

        bm = BufferManagerFactory.getTestBufferManager(100000, 250);
        setUp(RealMetadataFactory.example1Cached(), fdm, bm);

        helpTestDelete();
    }

    private void helpTestDelete() throws Exception {
        execute("insert into #tmp_params "
                + "select parsetimestamp('2016-04-01','yyyy-MM-dd') as starttime, parsetimestamp('2016-04-15','yyyy-MM-dd') as endtime", new List[] {Arrays.asList(1)});
        execute("insert into #tmp_dates "
                + "select cast(parsetimestamp('2016-03-20','yyyy-MM-dd') as date) as datum, 'somevalue' as somevalue "
                + "UNION select cast(parsetimestamp('2016-04-02','yyyy-MM-dd') as date) as datum, 'somevalue' as somevalue "
                + "UNION select cast(parsetimestamp('2016-04-20','yyyy-MM-dd') as date) as datum, 'somevalue' as somevalue", new List[] {Arrays.asList(3)});
        for (int i = 0; i < 1277; i++) {
            execute("insert into #tmp_dates select DATE '2016-05-01' as datum, 'somevalue' as somevalue", new List[] {Arrays.asList(1)});
        }
        execute("delete from #tmp_dates where datum > (select cast(endtime as date) from #tmp_params)", new List[] {Arrays.asList(1278)});
        execute("delete from #tmp_dates where datum < (select cast(starttime as date) from #tmp_params)", new List[] {Arrays.asList(1)});
        execute("select count(*) from #tmp_dates", new List[] {Arrays.asList(1)});
    }
    @Test public void testDeleteRemovingPageSmallPartialKey() throws Exception {
        execute("create temporary table #temp (c1 integer, c2 integer, primary key (c1, c2))", new List[] {Arrays.asList(0)});
        execute("insert into #temp (c1, c2) values (1, 1)", new List[] {Arrays.asList(1)});
        execute("delete from #temp where c1 = 1", new List[] {Arrays.asList(1)});
    }

    @Test public void testUpdateSmallPartialKey() throws Exception {
        execute("create temporary table #temp (c1 integer, c2 integer, primary key (c1, c2))", new List[] {Arrays.asList(0)});
        execute("insert into #temp (c1, c2) values (1, 1)", new List[] {Arrays.asList(1)});
        execute("update #temp set c2 = 2 where c1 = 1", new List[] {Arrays.asList(1)});
    }

    @Test public void testUpdateSmallNoKey() throws Exception {
        execute("create temporary table #temp (c1 integer, c2 integer)", new List[] {Arrays.asList(0)});
        execute("insert into #temp (c1, c2) values (1, 1)", new List[] {Arrays.asList(1)});
        execute("update #temp set c2 = 2 where c1 = 1", new List[] {Arrays.asList(1)});
    }

    @Test public void testImplicitResolvingWithoutColumns() throws Exception {
        execute("insert into #tmp_dates "
                + "select cast(parsetimestamp('2016-03-20','yyyy-MM-dd') as date) as datum, 'somevalue' as somevalue "
                + "UNION select cast(parsetimestamp('2016-04-02','yyyy-MM-dd') as date) as datum, 'somevalue' as somevalue "
                + "UNION select cast(parsetimestamp('2016-04-20','yyyy-MM-dd') as date) as datum, 'somevalue' as somevalue", new List[] {Arrays.asList(3)});
        execute("insert into #tmp_dates select DATE '2016-05-01', somevalue from #tmp_dates", new List[] {Arrays.asList(3)});
    }

    @Test public void testNotInPredicateDelete() throws Exception {
        execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        for (int i = 0; i < 2048; i++) {
            execute("insert into x (e2, e1) values ("+i+", '"+i+"')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        }

        execute("delete from x where e1 not in ('2000', '1')", new List[] {Arrays.asList(2046)}); //$NON-NLS-1$

        execute("drop table x", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("create local temporary table x (e1 string, e2 integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$

        for (int i = 0; i < 2048; i++) {
            execute("insert into x (e2, e1) values ("+i+", '"+i+"')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        }

        execute("delete from x where e1 not in ('2000', '1')", new List[] {Arrays.asList(2046)}); //$NON-NLS-1$
    }

    @Test public void testLargeException() throws Exception {
        BufferManagerImpl bm = BufferManagerFactory.getTestBufferManager(20480, 256);
        bm.setMaxBatchManagerSizeEstimate(100000);
        bm.setEnforceMaxBatchManagerSizeEstimate(true);
        FakeDataManager fdm = new FakeDataManager();
        TestProcessor.sampleData1(fdm);
        setUp(RealMetadataFactory.example1Cached(), fdm, bm);

        execute("create local temporary table x (e1 string, e2 string)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$

        for (int i = 0; i < 10; i++) {
            try {
                execute("insert into x (e2, e1) select * from x", new List[] {Arrays.asList(1<<i)}); //$NON-NLS-1$
            } catch (TeiidComponentException e) {
                assertTrue(e.getCode().equals("TEIID31261"));
                return;
            }
        }
        fail();
    }

    @Test public void testLargeNoException() throws Exception {
        BufferManagerImpl bm = BufferManagerFactory.getTestBufferManager(20480, 256);
        bm.setMaxBatchManagerSizeEstimate(100000);
        bm.setEnforceMaxBatchManagerSizeEstimate(false);
        FakeDataManager fdm = new FakeDataManager();
        TestProcessor.sampleData1(fdm);
        setUp(RealMetadataFactory.example1Cached(), fdm, bm);

        execute("create local temporary table x (e1 string, e2 string)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("insert into x (e2, e1) values (1, 1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$

        for (int i = 0; i < 10; i++) {
            execute("insert into x (e2, e1) select * from x", new List[] {Arrays.asList(1<<i)}); //$NON-NLS-1$
        }
    }

    @Test public void testNameWithPeriod() throws Exception {
        try {
            execute("create local temporary table \"pm1.g1\" (a string, b integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
            fail();
        } catch (QueryResolverException e) {

        }
        execute("create local temporary table \"something.g1\" (a string, b integer)", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
        execute("select a, b from \"something.g1\"", new List[0]); //$NON-NLS-1$
    }

}
