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

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.processor.TempTableDataManager;
import org.teiid.query.tempdata.TempTableStoreImpl;
import org.teiid.query.unittest.FakeMetadataFactory;


public class TestTempTables {
	
	private TempMetadataAdapter metadata;
	private TempTableDataManager dataManager;

	private void execute(String sql, List[] expectedResults) throws Exception {
		TestProcessor.doProcess(TestProcessor.helpGetPlan(sql, metadata), dataManager, expectedResults, TestProcessor.createCommandContext());
	}

	@Before public void setUp() throws TeiidComponentException {
		TempTableStoreImpl tempStore = new TempTableStoreImpl(BufferManagerFactory.getStandaloneBufferManager(), "1", null); //$NON-NLS-1$
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

}
