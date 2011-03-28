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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"nls", "unchecked"})
public class TestTempTables {
	
	private TempMetadataAdapter metadata;
	private TempTableDataManager dataManager;
	private TempTableStore tempStore;
	
	private ProcessorPlan execute(String sql, List[] expectedResults) throws Exception {
		ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);
		execute(plan, expectedResults);
		return plan;
	}
	
	private void execute(ProcessorPlan processorPlan, List[] expectedResults) throws Exception {
		CommandContext cc = TestProcessor.createCommandContext();
		cc.setMetadata(metadata);
		cc.setTempTableStore(tempStore);
		TestProcessor.doProcess(processorPlan, dataManager, expectedResults, cc);
		assertTrue(Determinism.SESSION_DETERMINISTIC.compareTo(cc.getDeterminismLevel()) <= 0);
	}

	@Before public void setUp() {
		tempStore = new TempTableStore("1"); //$NON-NLS-1$
		metadata = new TempMetadataAdapter(FakeMetadataFactory.example1Cached(), tempStore.getMetadataStore());
		metadata.setSession(true);
		FakeDataManager fdm = new FakeDataManager();
	    TestProcessor.sampleData1(fdm);
	    BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
	    SessionAwareCache<CachedResults> cache = new SessionAwareCache<CachedResults>();
	    cache.setBufferManager(bm);
	    Executor executor = new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
	    };
		dataManager = new TempTableDataManager(fdm, bm, executor, cache, null, null);
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
		Collection c = metadata.getUniqueKeysInGroup(metadata.getGroupID("x"));
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
	
	@Test public void testLikeWithIndex() throws Exception {
		execute("create local temporary table x (e1 string, e2 integer, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
		execute("insert into x (e2, e1) values (3, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
		execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
		execute("select * from x where e1 like 'z%'", new List[0]); //$NON-NLS-1$
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
	
	@Test(expected=TeiidProcessingException.class) public void testNotNull() throws Exception {
		execute("create local temporary table x (e1 serial, e2 integer not null, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
		execute("insert into x (e1, e2) values ((select null), 1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
	}
	
	@Test(expected=TeiidProcessingException.class) public void testNotNull1() throws Exception {
		execute("create local temporary table x (e1 serial, e2 integer not null, primary key (e1))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
		execute("insert into x (e2) values ((select null))", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
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
		TestOptimizer.helpPlan("select * from x, x1 where x.e2 = x1.e2 and x.e1 = x1.e1", this.metadata, new String[] {"SELECT x1.e2, x1.e1 FROM x1 ORDER BY x1.e1, x1.e2", "SELECT x.e2, x.e1 FROM x ORDER BY x.e1, x.e2"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	private void sampleTable() throws Exception {
		execute("create local temporary table x (e1 string, e2 integer, primary key (e1, e2))", new List[] {Arrays.asList(0)}); //$NON-NLS-1$
		execute("insert into x (e2, e1) values (3, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
		execute("insert into x (e2, e1) values (2, 'b')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
		execute("insert into x (e2, e1) values (1, 'c')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
		execute("insert into x (e2, e1) values (1, 'a')", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
	}
	
}
