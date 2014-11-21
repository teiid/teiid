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
import static org.teiid.query.optimizer.TestOptimizer.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.RealMetadataFactory.DDLHolder;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings({"nls", "unchecked"})
public class TestFunctionPushdown {

	@Test public void testMustPushdownOverMultipleSourcesWithoutSupport() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2"; //$NON-NLS-1$
        
        helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {}, ComparisonMode.FAILED_PLANNING); //$NON-NLS-1$ 
	}

	@Test public void testMustPushdownOverMultipleSources() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {"SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1, "a")});
        dataManager.addData("SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1), Arrays.asList(2)});
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList("a")});
	}
	
	@Test public void testSimpleFunctionPushdown() throws Exception {
		TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign function func (param integer) returns integer; create foreign table g1 (e1 integer)", "x", "y");
		BasicSourceCapabilities bsc = new BasicSourceCapabilities();
		bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
		bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
		final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        
		CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {
			
			@Override
			public PreparedPlan getPreparedPlan(String query, String recursionGroup,
					CommandContext commandContext, QueryMetadataInterface metadata)
					throws TeiidProcessingException, TeiidComponentException {
				return null;
			}
			
			@Override
			public CapabilitiesFinder getCapabiltiesFinder() {
				return capFinder;
			}
			
			@Override
			public QueryProcessor createQueryProcessor(String query,
					String recursionGroup, CommandContext commandContext,
					Object... params) throws TeiidProcessingException,
					TeiidComponentException {
				// TODO Auto-generated method stub
				return null;
			}
		});
        cc.setMetadata(tm);
		
		String sql = "select func(1)"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(2)});
        
        //ensure that pseudo-correlation works
        sql = "select func(0) from g1 where func(e1) = 2"; //$NON-NLS-1$
        
        plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {"SELECT y.g1.e1 FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT y.g1.e1 FROM y.g1", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT func(0)", new List[] {Arrays.asList(1)});
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});
        dataManager.addData("SELECT func(2)", new List[] {Arrays.asList(3)});
        
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(1)});
        
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        
        //ensure that pseudo-correlation works
        sql = "select case when hasrole('x') then func(0) else 2 end from g1"; //$NON-NLS-1$
        
        plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {"SELECT func(0) FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT func(0) FROM g1", new List[] {Arrays.asList(1), Arrays.asList(1)});
        
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(1), Arrays.asList(1)});
	}
	
	@Test public void testSimpleFunctionPushdown1() throws Exception {
		TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
		BasicSourceCapabilities bsc = new BasicSourceCapabilities();
		bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
		bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
		bsc.setFunctionSupport("parseDate_", true);
		final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        
		CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {
			
			@Override
			public PreparedPlan getPreparedPlan(String query, String recursionGroup,
					CommandContext commandContext, QueryMetadataInterface metadata)
					throws TeiidProcessingException, TeiidComponentException {
				return null;
			}
			
			@Override
			public CapabilitiesFinder getCapabiltiesFinder() {
				return capFinder;
			}
			
			@Override
			public QueryProcessor createQueryProcessor(String query,
					String recursionGroup, CommandContext commandContext,
					Object... params) throws TeiidProcessingException,
					TeiidComponentException {
				// TODO Auto-generated method stub
				return null;
			}
		});
        cc.setMetadata(tm);
		
		String sql = "select parseDate_('2011-11-11')"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT parsedate_('2011-11-11')", new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});
        cc.setDQPWorkContext(RealMetadataFactory.buildWorkContext(tm));
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});
        
		sql = "select misc.namespace.func('2011-11-11')"; //$NON-NLS-1$
        
        plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT parseDate_('2011-11-11')", new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});
        try {
        	TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(TimestampUtil.createDate(0, 0, 0))});
        	fail();
        } catch (TeiidProcessingException e) {
        	//not supported by any source
        }
	}
	
	@Test public void testSimpleFunctionPushdown2() throws Exception {
		TransformationMetadata tm = RealMetadataFactory.fromDDL("x", new DDLHolder("y", "CREATE FOREIGN FUNCTION func(a object, b object) RETURNS string;"), 
				new DDLHolder("z", "CREATE FOREIGN FUNCTION func1(a object, b object) RETURNS string; create foreign table g1 (e1 object)"));
		BasicSourceCapabilities bsc = new BasicSourceCapabilities();
		bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
		bsc.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
		final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        
		CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {
			
			@Override
			public PreparedPlan getPreparedPlan(String query, String recursionGroup,
					CommandContext commandContext, QueryMetadataInterface metadata)
					throws TeiidProcessingException, TeiidComponentException {
				return null;
			}
			
			@Override
			public CapabilitiesFinder getCapabiltiesFinder() {
				return capFinder;
			}
			
			@Override
			public QueryProcessor createQueryProcessor(String query,
					String recursionGroup, CommandContext commandContext,
					Object... params) throws TeiidProcessingException,
					TeiidComponentException {
				// TODO Auto-generated method stub
				return null;
			}
		});
        cc.setMetadata(tm);
		
        String sql = "select e1 from g1 where func(1, 1) = '2'"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {"SELECT z.g1.e1 FROM z.g1 WHERE func(1, 1) = '2'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT func(1, 1)", new List[] {Arrays.asList("hello world")});
    	TestProcessor.helpProcess(plan, cc, dataManager, new List[] {});
    	
    	sql = "select e1 from g1 where func1(1, 1) = '2'"; //$NON-NLS-1$
        
        plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {"SELECT z.g1.e1 FROM z.g1 WHERE func1(1, 1) = '2'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        dataManager = new HardcodedDataManager(tm);
        dataManager.addData("SELECT g1.e1 FROM g1 WHERE func1(1, 1) = '2'", new List[] {Arrays.asList("hello world")});
    	TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList("hello world")});
	}

	@Test public void testMustPushdownOverMultipleSourcesWithView() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from (select x.* from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2 order by e1 limit 10) as x"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {"SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1)});
        dataManager.addData("SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1, "aa", "a"), Arrays.asList(2, "bb", "b")});
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList("aa")});
	}
	
	@Test public void testMustPushdownOverMultipleSourcesWithViewDupRemoval() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from (select distinct x.* from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2 order by e1 limit 10) as x"; //$NON-NLS-1$
        
        helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {}, ComparisonMode.FAILED_PLANNING); //$NON-NLS-1$ 
	}
	
	@Test public void testDDLMetadata() throws Exception {
		String ddl = "CREATE VIRTUAL FUNCTION SourceFunc(msg varchar) RETURNS varchar " +
				"OPTIONS(CATEGORY 'misc', DETERMINISM 'DETERMINISTIC', " +
				"\"NULL-ON-NULL\" 'true', JAVA_CLASS '"+TestFunctionPushdown.class.getName()+"', JAVA_METHOD 'sourceFunc');" +
				"CREATE VIEW X (Y varchar) as SELECT e1 from pm1.g1;";

		MetadataFactory mf = TestDDLParser.helpParse(ddl, "model");
		mf.getSchema().setPhysical(false);
		MetadataStore ms = mf.asMetadataStore();
		ms.merge(RealMetadataFactory.example1Cached().getMetadataStore());
		
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(ms, "example1");
		
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("model.SourceFunc", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpPlan("select sourceFunc(y) from x", metadata, null, capFinder, 
                new String[] {"SELECT sourceFunc(g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$         

        caps.setFunctionSupport("model.SourceFunc", false);
        
        helpPlan("select sourceFunc(y) from x", metadata, null, capFinder, 
                new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$         
	}
	
	@Test public void testDDLMetadata1() throws Exception {
		String ddl = "CREATE foreign FUNCTION sourceFunc(msg varchar) RETURNS varchar options (nameinsource 'a.sourcefunc') " +
		              "CREATE foreign FUNCTION b.sourceFunc(msg varchar) RETURNS varchar " +
				"CREATE foreign table X (Y varchar);";

		QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");
		
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("phy", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan("SELECT sourceFunc(g_0.Y), phy.b.sourceFunc(g_0.Y) FROM phy.X AS g_0", metadata, null, capFinder, 
                new String[] {"SELECT sourceFunc(g_0.Y), phy.b.sourceFunc(g_0.Y) FROM phy.X AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        //ensure that the source query contains the function schemas
        HardcodedDataManager dm = new HardcodedDataManager(metadata);
        dm.addData("SELECT a.sourcefunc(g_0.Y), b.sourceFunc(g_0.Y) FROM X AS g_0", new List[0]);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        TestProcessor.helpProcess(plan, cc, dm, new List[0]);
	}
	
	@Test public void testDDLMetadataNameConflict() throws Exception {
		String ddl = "CREATE foreign FUNCTION \"convert\"(msg integer, type varchar) RETURNS varchar " +
				"CREATE foreign table X (Y integer);";

		QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");
		
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("phy", caps); //$NON-NLS-1$
        
        helpPlan("select phy.convert(y, 'z') from x", metadata, null, capFinder, 
                new String[] {"SELECT phy.convert(g_0.Y, 'z') FROM phy.X AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
	}
	
	@Test public void testConcat2() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT2, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        String sql = "select concat2(x.e1, x.e1) from pm1.g1 as x"; //$NON-NLS-1$
        
        helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {"SELECT concat2(g_0.e1, g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        //cannot pushdown
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT2, false);
        
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, 
                new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("a"), Arrays.asList((String)null)});
        TestProcessor.helpProcess(plan, dataManager, new List[] {Arrays.asList("aa"), Arrays.asList((String)null)});
        
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        caps.setFunctionSupport(SourceSystemFunctions.IFNULL, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        
        //will get replaced in the LanguageBridgeFactory
        helpPlan(sql, metadata, null, capFinder, 
                new String[] {"SELECT concat2(g_0.e1, g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
	}
	
	@Test public void testMustPushdownOverGrouping() throws Exception {
		TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign function func (param integer) returns integer; create foreign table g1 (e1 integer)", "x", "y");
		BasicSourceCapabilities bsc = new BasicSourceCapabilities();
		bsc.setCapabilitySupport(Capability.SELECT_WITHOUT_FROM, true);
		bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
		final DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        
		CommandContext cc = TestProcessor.createCommandContext();
        cc.setQueryProcessorFactory(new QueryProcessor.ProcessorFactory() {
			
			@Override
			public PreparedPlan getPreparedPlan(String query, String recursionGroup,
					CommandContext commandContext, QueryMetadataInterface metadata)
					throws TeiidProcessingException, TeiidComponentException {
				return null;
			}
			
			@Override
			public CapabilitiesFinder getCapabiltiesFinder() {
				return capFinder;
			}
			
			@Override
			public QueryProcessor createQueryProcessor(String query,
					String recursionGroup, CommandContext commandContext,
					Object... params) throws TeiidProcessingException,
					TeiidComponentException {
				// TODO Auto-generated method stub
				return null;
			}
		});
        cc.setMetadata(tm);
		
        String sql = "select func(e1) from g1 group by e1"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, tm, null, capFinder, 
                                      new String[] {"SELECT y.g1.e1 FROM y.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT y.g1.e1 FROM y.g1", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT func(1)", new List[] {Arrays.asList(2)});
        dataManager.addData("SELECT func(2)", new List[] {Arrays.asList(3)});
        
        TestProcessor.helpProcess(plan, cc, dataManager, new List[] {Arrays.asList(2), Arrays.asList(3)});
	}
	
	public static String sourceFunc(String msg) {
		return msg;
	}
	
}
