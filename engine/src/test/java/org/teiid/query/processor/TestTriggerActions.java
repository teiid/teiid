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
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.TestUpdateValidator;

@SuppressWarnings("nls")
public class TestTriggerActions {
    
	private static final String GX = "GX";
	private static final String VM1 = "VM1";

	@Test public void testInsert() throws Exception {
		TransformationMetadata metadata = TestUpdateValidator.example1();
		TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
		Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
		t.setDeletePlan("");
		t.setUpdatePlan("");
		t.setInsertPlan("FOR EACH ROW BEGIN insert into pm1.g1 (e1) values (new.x); END");
		
		String sql = "insert into gx (x, y) values (1, 2)";
		
		FakeDataManager dm = new FakeDataManager();
		FakeDataStore.addTable("pm1.g1", dm, metadata);
		
		CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List[] expected = new List[] {Arrays.asList(1)};
    	helpProcess(plan, context, dm, expected);
	}
	
	@Test public void testInsertWithQueryExpression() throws Exception {
		TransformationMetadata metadata = TestUpdateValidator.example1();
		TestUpdateValidator.createView("select '1' as x, 2 as y", metadata, GX);
		Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
		t.setDeletePlan("");
		t.setUpdatePlan("");
		t.setInsertPlan("FOR EACH ROW BEGIN insert into pm1.g1 (e1) values (new.x); END");
		
		String sql = "insert into gx (x, y) select e1, e2 from pm1.g1";
		
		FakeDataManager dm = new FakeDataManager();
		FakeDataStore.addTable("pm1.g1", dm, metadata);
		CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List[] expected = new List[] {Arrays.asList(6)};
    	helpProcess(plan, context, dm, expected);
	}
	
	@Test public void testDelete() throws Exception {
		TransformationMetadata metadata = TestUpdateValidator.example1();
		TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
		Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
		t.setDeletePlan("FOR EACH ROW BEGIN delete from pm1.g1 where e2 = old.x; END");
		t.setUpdatePlan("");
		t.setInsertPlan("");
		
		String sql = "delete from gx where y = 2";
		
		FakeDataManager dm = new FakeDataManager();
		FakeDataStore.addTable("pm1.g1", dm, metadata);
		
		CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List[] expected = new List[] {Arrays.asList(1)};
    	helpProcess(plan, context, dm, expected);
	}
	
	@Test public void testUpdate() throws Exception {
		TransformationMetadata metadata = TestUpdateValidator.example1();
		TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
		Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
		t.setDeletePlan("");
		t.setUpdatePlan("FOR EACH ROW BEGIN update pm1.g1 set e2 = new.y where e2 = old.y; END");
		t.setInsertPlan("");
		
		String sql = "update gx set y = 5";
		
		FakeDataManager dm = new FakeDataManager();
		FakeDataStore.addTable("pm1.g1", dm, metadata);
		
		CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List[] expected = new List[] {Arrays.asList(1)};
    	helpProcess(plan, context, dm, expected);
    	assertEquals("UPDATE pm1.g1 SET e2 = 5 WHERE e2 = 2", dm.getQueries().get(0));
	}
	
	@Test public void testUpdateWithChanging() throws Exception {
		TransformationMetadata metadata = TestUpdateValidator.example1();
		TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
		Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
		t.setDeletePlan("");
		t.setUpdatePlan("FOR EACH ROW BEGIN update pm1.g1 set e2 = case when changing.y then new.y end where e2 = old.y; END");
		t.setInsertPlan("");
		
		String sql = "update gx set y = 5";
		
		FakeDataManager dm = new FakeDataManager();
		FakeDataStore.addTable("pm1.g1", dm, metadata);
		
		CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
    	helpProcess(plan, context, dm, expected);
    	assertEquals("UPDATE pm1.g1 SET e2 = 5 WHERE e2 = 2", dm.getQueries().get(0));
	}
	
	@Test public void testUpdateWithNonConstant() throws Exception {
		TransformationMetadata metadata = TestUpdateValidator.example1();
		TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
		Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
		t.setDeletePlan("");
		t.setUpdatePlan("FOR EACH ROW BEGIN update pm1.g1 set e2 = new.y where e2 = old.y; END");
		t.setInsertPlan("");
		
		String sql = "update gx set y = x";
		
		FakeDataManager dm = new FakeDataManager();
		FakeDataStore.addTable("pm1.g1", dm, metadata);
		
		CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List[] expected = new List[] {Arrays.asList(1)};
    	helpProcess(plan, context, dm, expected);
    	assertEquals("UPDATE pm1.g1 SET e2 = 1 WHERE e2 = 2", dm.getQueries().get(0));
	}
    
}
