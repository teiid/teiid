/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2009 Red Hat, Inc.
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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.QueryProcessorFactoryImpl;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.tempdata.GlobalTableStoreImpl.MatTableInfo;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestMaterialization {
	
	private TempMetadataAdapter metadata;
	private TempTableDataManager dataManager;
	private TempTableStore tempStore;
	private GlobalTableStoreImpl globalStore;
	private ProcessorPlan previousPlan;
	private HardcodedDataManager hdm;
	
	@Before public void setUp() {
		tempStore = new TempTableStore("1", TransactionMode.ISOLATE_WRITES); //$NON-NLS-1$
	    BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
	    QueryMetadataInterface actualMetadata = RealMetadataFactory.exampleMaterializedView();
	    
	    globalStore = new GlobalTableStoreImpl(bm, actualMetadata);
		metadata = new TempMetadataAdapter(actualMetadata, tempStore.getMetadataStore());
		hdm = new HardcodedDataManager();
		hdm.addData("SELECT matsrc.x FROM matsrc", new List[] {Arrays.asList((String)null), Arrays.asList("one"), Arrays.asList("two"), Arrays.asList("three")});
		hdm.addData("SELECT mattable.info.e1, mattable.info.e2 FROM mattable.info", new List[] {Arrays.asList("a", 1), Arrays.asList("a", 2)});
		hdm.addData("SELECT mattable.info.e2, mattable.info.e1 FROM mattable.info", new List[] {Arrays.asList(1, "a"), Arrays.asList(2, "a")});
		
	    SessionAwareCache<CachedResults> cache = new SessionAwareCache<CachedResults>();
	    cache.setBufferManager(bm);
		dataManager = new TempTableDataManager(hdm, bm, cache);
	}
	
	private void execute(String sql, List<?>... expectedResults) throws Exception {
		CommandContext cc = TestProcessor.createCommandContext();
		cc.setTempTableStore(tempStore);
		cc.setGlobalTableStore(globalStore);
		cc.setMetadata(metadata);
		
		CapabilitiesFinder finder = new DefaultCapabilitiesFinder();
		previousPlan = TestProcessor.helpGetPlan(TestProcessor.helpParse(sql), metadata, finder, cc);
		cc.setQueryProcessorFactory(new QueryProcessorFactoryImpl(BufferManagerFactory.getStandaloneBufferManager(), dataManager, finder, null, metadata));
		TestProcessor.doProcess(previousPlan, dataManager, expectedResults, cc);
	}

	@Test public void testPopulate() throws Exception {
		execute("SELECT * from vgroup3 where x = 'one'", Arrays.asList("one", "zne"));
		assertEquals(1, hdm.getCommandHistory().size());
		execute("SELECT * from vgroup3 where x is null", Arrays.asList(null, null));
		assertEquals(1, hdm.getCommandHistory().size());
	}
	
	@Test public void testReadWrite() throws Exception {
		execute("SELECT * from vgroup3 where x = 'one'", Arrays.asList("one", "zne"));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		String matTableName = RelationalPlanner.MAT_PREFIX + "MATVIEW.VGROUP3";
		this.globalStore.getState(matTableName, baos);
		MatTableInfo matTableInfo = this.globalStore.getMatTableInfo(matTableName);
		long time = matTableInfo.getUpdateTime();
		this.globalStore.failedLoad(matTableName);
		this.globalStore.setState(matTableName, new ByteArrayInputStream(baos.toByteArray()));
		assertEquals(time, matTableInfo.getUpdateTime());
		execute("SELECT * from vgroup3 where x = 'one'", Arrays.asList("one", "zne"));
		
		execute("select lookup('mattable.info', 'e1', 'e2', 5)", Arrays.asList((String)null));
		baos = new ByteArrayOutputStream();
		String codeTableName = "#CODE_MATTABLE.INFO.E2.E1";
		this.globalStore.getState(codeTableName, baos);
		this.globalStore.setState(codeTableName, new ByteArrayInputStream(baos.toByteArray()));
	}
	
    @Test(expected=TeiidProcessingException.class) public void testCodeTableResponseException() throws Exception {
    	//duplicate key
    	execute("select lookup('mattable.info', 'e2', 'e1', 'a')");
    }
    
    @Test public void testCodeTable() throws Exception {
    	execute("select lookup('mattable.info', 'e1', 'e2', 5)", Arrays.asList((String)null));
    	assertEquals(1, hdm.getCommandHistory().size());
    	execute("select lookup('mattable.info', 'e1', 'e2', 1)", Arrays.asList("a"));
    	assertEquals(1, hdm.getCommandHistory().size());
    }
    
	@Test public void testTtl() throws Exception {
		
		execute("SELECT * from vgroup4 where x = 'one'", Arrays.asList("one"));
		assertEquals(1, hdm.getCommandHistory().size());
		execute("SELECT * from vgroup4 where x is null", Arrays.asList((String)null));
		assertEquals(1, hdm.getCommandHistory().size());
		Thread.sleep(150);
		execute("SELECT * from vgroup4 where x is null", Arrays.asList((String)null));
		assertEquals(2, hdm.getCommandHistory().size());
	}
	
	@Test public void testProcedureCache() throws Exception {
		execute("call sp1('one')", Arrays.asList("one"));
		assertEquals(1, hdm.getCommandHistory().size());
		execute("call sp1('one')", Arrays.asList("one"));
		assertEquals(1, hdm.getCommandHistory().size());
		execute("call sp1('one') option nocache sp.sp1", Arrays.asList("one"));
		assertEquals(2, hdm.getCommandHistory().size());
		execute("call sp1(null)");
		assertEquals(3, hdm.getCommandHistory().size());
		execute("call sp1(null)");
		assertEquals(3, hdm.getCommandHistory().size());
	}
	
	@Test public void testCoveringSecondaryIndex() throws Exception {
		execute("SELECT * from vgroup3 where y in ('zne', 'zwo') order by y desc", Arrays.asList("two", "zwo"), Arrays.asList("one", "zne"));
		execute("SELECT * from vgroup3 where y is null", Arrays.asList((String)null, (String)null));
	}
	
	@Test public void testNonCoveringSecondaryIndex() throws Exception {
		execute("SELECT * from vgroup5 where y in ('zne', 'zwo') order by y desc", Arrays.asList("two", "zwo", 1), Arrays.asList("one", "zne", 1));
		execute("SELECT * from vgroup5 where y is null", Arrays.asList((String)null, (String)null, 1));
		execute("SELECT * from vgroup5 where y is null and z = 2");
	}
	
	@Test public void testNonCoveringSecondaryIndexWithoutPrimaryKey() throws Exception {
		execute("SELECT * from vgroup6 where y in ('zne', 'zwo') order by y desc", Arrays.asList("two", "zwo"), Arrays.asList("one", "zne"));
		execute("SELECT * from vgroup6 where y is null", Arrays.asList((String)null, (String)null));
	}
	
	@Test public void testPrimaryKeyOnOtherColumn() throws Exception {
		execute("SELECT * from vgroup7 where y is null", Arrays.asList("1", null, 1));
	}
    
}
