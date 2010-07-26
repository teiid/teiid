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

import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.unittest.FakeMetadataFactory;

@SuppressWarnings("nls")
public class TestTempTables {
	
	private TempMetadataAdapter metadata;
	private TempTableDataManager dataManager;

	private void execute(String sql, List[] expectedResults) throws Exception {
		execute(expectedResults, TestProcessor.helpGetPlan(sql, metadata));
	}
	
	private void execute(List[] expectedResults, ProcessorPlan processorPlan) throws Exception {
		TestProcessor.doProcess(processorPlan, dataManager, expectedResults, TestProcessor.createCommandContext());
	}

	@Before public void setUp() {
		TempTableStore tempStore = new TempTableStore(BufferManagerFactory.getStandaloneBufferManager(), "1", null); //$NON-NLS-1$
		metadata = new TempMetadataAdapter(FakeMetadataFactory.example1Cached(), tempStore.getMetadataStore());
		FakeDataManager fdm = new FakeDataManager();
	    TestProcessor.sampleData1(fdm);
		dataManager = new TempTableDataManager(fdm, tempStore);
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
	
}
