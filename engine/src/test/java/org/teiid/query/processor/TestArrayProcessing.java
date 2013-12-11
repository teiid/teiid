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
import static org.teiid.query.resolver.TestResolver.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.TestParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestArrayProcessing {
	
	@Test public void testArrayCast() throws Exception {
		String sql = "select cast(cast((1,2) as object) as integer[])"; //$NON-NLS-1$
        
        helpResolve(sql, RealMetadataFactory.example1Cached());

        //should succeed
        sql = "select cast(cast((1,2) as object) as integer[])"; //$NON-NLS-1$
        
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached());

        helpProcess(plan, dataManager, null);

        //should fail
        sql = "select cast(cast((1,2) as object) as string[])"; //$NON-NLS-1$
        
    	try {
    		helpGetPlan(helpParse(sql), RealMetadataFactory.exampleBQTCached(), DefaultCapabilitiesFinder.INSTANCE, createCommandContext());
    		fail();
    	} catch (TeiidProcessingException e) {
    		
    	}
	}
	
	@Test public void testArrayComparison() {
		String sql = "select count(e1) from pm1.g1 where (e1, e2) = ('a', 1)";
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", Arrays.asList("a", 2), Arrays.asList("a", 1));
    	ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});
        
        dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0 WHERE (g_0.e1, g_0.e2) = ('a', 1)", Arrays.asList("a"));
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.ARRAY_TYPE, true);
    	plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc));

        helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});
	}

	@Test public void testArraySort() {
		String sql = "select (e1, e2) from pm1.g1 order by (e1, e2), e3";
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3 FROM pm1.g1", Arrays.asList("b", 4, true), Arrays.asList("a", 2, true), Arrays.asList("a", 1, false));
    	ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(new ArrayImpl("a", 1)),
        		Arrays.asList(new ArrayImpl("a", 2)),
        		Arrays.asList(new ArrayImpl("b", 4))});
	}
	
	@Test public void testArrayGetTyping() {
		String sql = "select array_agg(e1)[1], array_agg(e2)[3] from pm1.g1"; //$NON-NLS-1$
        
        Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
        assertEquals(DataTypeManager.DefaultDataClasses.STRING, command.getProjectedSymbols().get(0).getType());
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, command.getProjectedSymbols().get(1).getType());
	}
	
	@Test(expected=QueryResolverException.class) public void testArrayGetTypingFails() throws QueryResolverException, TeiidComponentException {
		String sql = "select array_agg(e1)[1][2] from pm1.g1"; //$NON-NLS-1$
		QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
	}
	
	@Test public void testArrayParsing() throws Exception {
		TestParser.helpTestExpression("()", "()", new Array(new ArrayList<Expression>()));
		TestParser.helpTestExpression("(,)", "()", new Array(new ArrayList<Expression>()));
		TestParser.helpTestExpression("(1,)", "(1,)", new Array(Arrays.asList((Expression)new Constant(1))));
		TestParser.helpTestExpression("(1,2)", "(1, 2)", new Array(Arrays.asList((Expression)new Constant(1), (Expression)new Constant(2))));
		TestParser.helpTestExpression("(1,2,)", "(1, 2)", new Array(Arrays.asList((Expression)new Constant(1), (Expression)new Constant(2))));
	}
	
	@Test public void testArrayTable() throws Exception {
		String sql = "select x.* from arraytable(('a', 2-1, {d'2001-01-01'}) COLUMNS x string, y integer) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList("a", 1),
        };    

        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, new HardcodedDataManager(), expected);
	}
	
	@Test public void testMultiDimensionalGet() throws Exception {
		String sql = "select -((e2, e2), (e2, e2))[1][1] from pm1.g1"; //$NON-NLS-1$
		QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
		Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
	    assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, command.getProjectedSymbols().get(0).getType());
	}
	
	@Test public void testMultiDimensionalCast() throws Exception {
		String sql = "select cast( ((e2, e2), (e2, e2)) as object[])  from pm1.g1"; //$NON-NLS-1$
		QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
		Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
	    assertEquals(Object[].class, command.getProjectedSymbols().get(0).getType());
	    
	    ProcessorPlan pp = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
	    HardcodedDataManager dataManager = new HardcodedDataManager();
	    dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList(1), Arrays.asList(2));
		TestProcessor.helpProcess(pp, dataManager, new List[] {
				Arrays.asList(new ArrayImpl((Object[])new Integer[][] {new Integer[] {1,1}, new Integer[] {1,1}})),
				Arrays.asList(new ArrayImpl((Object[])new Integer[][] {new Integer[] {2,2}, new Integer[] {2,2}}))});
	    
	    sql = "select cast(cast( ((e2, e2), (e2, e2)) as object[]) as integer[][])  from pm1.g1"; //$NON-NLS-1$
		QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
		command = helpResolve(sql, RealMetadataFactory.example1Cached());
	    assertEquals(Integer[][].class, command.getProjectedSymbols().get(0).getType());
	}
	
	@Test public void testMultiDimensionalArrayRewrite() throws Exception {
		String sql = "select (('a', 'b'),('c','d'))"; //$NON-NLS-1$
		QueryResolver.resolveCommand(helpParse(sql), RealMetadataFactory.example1Cached());
		Command command = helpResolve(sql, RealMetadataFactory.example1Cached());
	    assertEquals(String[][].class, command.getProjectedSymbols().get(0).getType());
	    
	    command = QueryRewriter.rewrite(command, RealMetadataFactory.example1Cached(), null);
	    Expression ex = SymbolMap.getExpression(command.getProjectedSymbols().get(0));
	    Constant c = (Constant)ex;
	    assertTrue(c.getValue() instanceof ArrayImpl);
	}
	
	/**
	 * TODO
 	@Test public void testArrayLobs() {
		//ensure that we introspect arrays for lob references
	}
	 */

}
