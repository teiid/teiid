package org.teiid.query.processor;

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestAggregatePushdown;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"nls", "unchecked"})
public class TestWithClauseProcessing {
	
	@Test public void testSingleItem() {
	    String sql = "with a (x, y, z) as (select e1, e2, e3 from pm1.g1) SELECT pm1.g2.e2, a.x from pm1.g2, a where e1 = x and z = 1 order by x"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	        Arrays.asList(0, "a"),
	        Arrays.asList(3, "a"),
	        Arrays.asList(0, "a"),
	        Arrays.asList(1, "c"),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    dataManager.setBlockOnce();
	    sampleData1(dataManager);
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testMultipleItems() {
	    String sql = "with a (x, y, z) as (select e1, e2, e3 from pm1.g1), b as (SELECT * from pm1.g2, a where e1 = x and z = 1 order by e2 limit 2) SELECT a.x, b.e1 from a, b where a.x = b.e1"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    dataManager.setBlockOnce();
	    sampleData1(dataManager);
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testWithPushdown() throws TeiidException {
		 FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
         BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
         caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
         caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
         capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
	    String sql = "with a (x, y, z) as (select e1, e2, e3 from pm1.g1) SELECT a.x from a, a z"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager();
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a", 1, Boolean.FALSE),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS (SELECT g_0.e1, g_0.e2, g_0.e3 FROM pm1.g1 AS g_0) SELECT g_0.x FROM a AS g_0, a AS g_1", expected);
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS (SELECT g_0.e1, g_0.e2, g_0.e3 FROM pm1.g1 AS g_0) SELECT g_0.x FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testWithPushdown1() throws TeiidException {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       
	    String sql = "with a as (select x, y, z from (select e1 as x, e2 as y, e3 as z from pm1.g1) v) SELECT count(a.x) from a, a z"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a", 1, Boolean.FALSE),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS (SELECT g_0.e1, g_0.e2, g_0.e3 FROM g1 AS g_0) SELECT COUNT(g_0.x) FROM a AS g_0, a AS g_1", expected);
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS (SELECT g_0.e1, g_0.e2, g_0.e3 FROM pm1.g1 AS g_0) SELECT COUNT(g_0.x) FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	/**
	 * This tests both an intervening parent plan construct (count) and a reference to a parent with in a subquery
	 */
	@Test public void testWithPushdownNotFullyPushed() throws TeiidException {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       
	    String sql = "with a as (select x, y, z from (select e1 as x, e2 as y, e3 as z from pm1.g1) v), b as (select e4 from pm1.g3) SELECT count(a.x), max(a.y) from a, a z group by z.x having max(a.y) < (with b as (select e1 from pm1.g1) select a.y from a, b where a.x = z.x)"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a", 1, "a"),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS (SELECT g_0.e1, g_0.e2, g_0.e3 FROM g1 AS g_0) SELECT g_1.x, g_0.y, g_0.x FROM a AS g_0, a AS g_1", expected);
	    dataManager.addData("WITH b (e1) AS (SELECT g_0.e1 FROM g1 AS g_0), a (x, y, z) AS (SELECT g_0.e1, g_0.e2, g_0.e3 FROM g1 AS g_0) SELECT g_0.y FROM a AS g_0, b AS g_1 WHERE g_0.x = 'a'", 
	    		new List[] {Arrays.asList(2)});
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS (SELECT g_0.e1, g_0.e2, g_0.e3 FROM pm1.g1 AS g_0) SELECT g_1.x, g_0.y, g_0.x FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager, new List[] { 
		        Arrays.asList(1, 1),
		    });
	}
	
	/**
	 * Tests source affinity
	 */
	@Test public void testWithPushdownNotFullyPushed1() throws TeiidException {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
       
	    String sql = "with a as (select e1 from pm1.g1), b as (select e1 from pm2.g2), c as (select count(*) as x from pm1.g1) SELECT a.e1, (select max(x) from c), pm1.g1.e2 from pm1.g1, a, b"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());

	    dataManager.addData("WITH a (e1) AS (SELECT g_0.e1 FROM g1 AS g_0) SELECT g_0.e1 FROM a AS g_0",  new List[] { 
		        Arrays.asList("a"),
		    });
	    dataManager.addData("WITH b (e1) AS (SELECT g_0.e1 FROM g2 AS g_0) SELECT 1 FROM b AS g_0",  new List[] { 
		        Arrays.asList("b"),
		    });
	    dataManager.addData("SELECT g_0.e2 FROM g1 AS g_0", new List[] { 
		        Arrays.asList(1), Arrays.asList(2)
		    });
	    dataManager.addData("SELECT 1 FROM g1 AS g_0", new List[] { 
		        Arrays.asList(1), Arrays.asList(1), Arrays.asList(1)
		    });
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {
	    	"SELECT g_0.e2 FROM pm1.g1 AS g_0", 
	    	"WITH b (e1) AS (SELECT g_0.e1 FROM pm2.g2 AS g_0) SELECT 1 FROM b AS g_0", 
	    	"WITH a (e1) AS (SELECT g_0.e1 FROM pm1.g1 AS g_0) SELECT g_0.e1 FROM a AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager,  new List[] { 
		        Arrays.asList("a", 3, 1),
		        Arrays.asList("a", 3, 2),
		    });
	}

	@Test public void testWithPushdownWithConstants() throws TeiidException {
		 FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       
	    String sql = "with a (x, y) as (select 1, 2 from pm1.g1) SELECT a.x from a, a z"; //$NON-NLS-1$
	    
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y) AS (SELECT 1, 2 FROM pm1.g1 AS g_0) SELECT g_0.x FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithOrderBy() throws TeiidException {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
      
	    String sql = "with a (x, y) as (select 1, 2 from pm1.g1) SELECT a.x from a, a z order by x"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("WITH a (x, y) AS (SELECT 1, 2 FROM g1 AS g_0) SELECT g_0.x AS c_0 FROM a AS g_0, a AS g_1 ORDER BY c_0", new List[0]);
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y) AS (SELECT 1, 2 FROM pm1.g1 AS g_0) SELECT g_0.x AS c_0 FROM a AS g_0, a AS g_1 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager,  new List[0]);
	}
	
	@Test public void testWithJoinPlanning() throws TeiidException {
		TransformationMetadata metadata = RealMetadataFactory.example1();
		RealMetadataFactory.setCardinality("pm1.g2", 100000, metadata);
	    String sql = "with a (x) as (select e1 from pm1.g1) SELECT a.x from pm1.g2, a where (pm1.g2.e1 = a.x)"; //$NON-NLS-1$
	    
	    TestOptimizer.helpPlan(sql, metadata, null, TestOptimizer.getGenericFinder(false), new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT a.x FROM a ORDER BY a.x"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithJoinPlanning1() throws TeiidException {
		TransformationMetadata metadata = RealMetadataFactory.example1Cached();
	    String sql = "with a (x) as (select e1 from pm1.g1) SELECT a.x from pm1.g2, a where (pm1.g2.e1 = a.x)"; //$NON-NLS-1$
	    
	    TestOptimizer.helpPlan(sql, metadata, null, TestOptimizer.getGenericFinder(false), new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT a.x FROM a ORDER BY a.x"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithBlockingJoin() throws TeiidException {
	      
	    String sql = "with a (x, y) as (select e1, e2 from pm1.g1) SELECT a.x, a.y, pm1.g2.e1 from a left outer join pm1.g2 makenotdep on (rtrim(a.x) = pm1.g2.e1) order by a.y"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager() {
	    	@Override
	    	public TupleSource registerRequest(CommandContext context,
	    			Command command, String modelName,
	    			RegisterRequestParameter parameterObject)
	    			throws TeiidComponentException {
	    		final TupleSource ts = super.registerRequest(context, command, modelName, parameterObject);
	    		return new TupleSource() {
	    			int i = 0;
					
					@Override
					public List<?> nextTuple() throws TeiidComponentException,
							TeiidProcessingException {
						if ((i++ % 100)<3) {
							throw BlockedException.INSTANCE;
						}
						return ts.nextTuple();
					}
					
					@Override
					public void closeSource() {
						ts.closeSource();
					}
				};
	    	}
	    };
	    List<?>[] rows = new List[10];
	    for (int i = 0; i < rows.length; i++) {
	    	rows[i] = Arrays.asList(String.valueOf(i));
	    }
	    dataManager.addData("SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0", rows);
	    rows = new List[100];
	    for (int i = 0; i < rows.length; i++) {
	    	rows[i] = Arrays.asList(String.valueOf(i), i);
	    }
	    dataManager.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", rows);
	    
	    dataManager.addData("WITH a (x, y) AS (SELECT 1, 2 FROM g1 AS g_0) SELECT g_0.x AS c_0 FROM a AS g_0, a AS g_1 ORDER BY c_0", new List[0]);
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(TestOptimizer.getTypicalCapabilities()), new String[] {"SELECT a.x, a.y FROM a", "SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);
	    //check the full pushdown command
	    
	    List<?>[] result = new List[100];
	    for (int i = 0; i < result.length; i++) {
	    	result[i] = Arrays.asList(String.valueOf(i), i, i < 10?String.valueOf(i):null);
	    }
	    
	    helpProcess(plan, dataManager, result);
	}
	
	@Test public void testSingleItemInOn() {
	    String sql = "with a (x, y, z) as (select e1, e2, e3 from pm1.g1 limit 1) SELECT pm2.g1.e1 from pm2.g1 left outer join pm2.g2 on (pm2.g1.e2 = pm2.g2.e2 and pm2.g1.e1 = (select a.x from a))"; //$NON-NLS-1$
	    
	    List[] expected = new List[] {Arrays.asList("a")};    
	
	    HardcodedDataManager dataManager = new HardcodedDataManager() {

	    	boolean block = true;

	    	@Override
	    	public TupleSource registerRequest(CommandContext context,
	    			Command command, String modelName,
	    			RegisterRequestParameter parameterObject)
	    			throws TeiidComponentException {
	    		if (block) {
	    			block = false;
	    			throw BlockedException.INSTANCE;
	    		}
	    		return super.registerRequest(context, command, modelName, parameterObject);
	    	}
	    };
	    dataManager.addData("SELECT g_0.e1, g_0.e2, g_0.e3 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("a", 1, true)});
	    dataManager.addData("SELECT g_0.e1 FROM pm2.g1 AS g_0 LEFT OUTER JOIN pm2.g2 AS g_1 ON g_0.e2 = g_1.e2 AND g_0.e1 = 'a'", new List[] {Arrays.asList("a")});
	    
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
	    bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, true);
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc));
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testWithGroupingAndMultiElement() {
		String sql = "WITH qry_0 as (SELECT floor(t.e3) AS a1, floor(t2.e3) as b1 FROM pm1.g1 AS t, pm2.g2 as t2 WHERE (t.e3=t2.e3) GROUP BY t.e3, t2.e3) SELECT * from qry_0 GROUP BY a1, b1";
		
		List[] expected = new List[] {Arrays.asList(3.0, 3.0)};    
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
	    dataManager.addData("SELECT g_0.e3 AS c_0 FROM pm1.g1 AS g_0 GROUP BY g_0.e3 ORDER BY c_0", Arrays.asList(2.1), Arrays.asList(3.2));
	    dataManager.addData("SELECT g_0.e3 AS c_0 FROM pm2.g2 AS g_0 GROUP BY g_0.e3 ORDER BY c_0", Arrays.asList(2.0), Arrays.asList(3.2));
		
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testSubqueryWithGrouping() {
		String sql = "select q.str_a, q.a from(WITH qry_0 as (SELECT e2 AS a1, e1 as str FROM pm1.g1 AS t) SELECT a1 as a, str as str_a from qry_0) as q group by q.str_a, q.a";
		
		List[] expected = new List[] {Arrays.asList("a", 1)};    
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
	    dataManager.addData("SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0", Arrays.asList(1, "a"));
		
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testFunctionEvaluation() throws Exception {
		String sql = "with test as (select user() as u from pm1.g1) select u from test";
		
		List[] expected = new List[] {Arrays.asList("user")};    
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("WITH test (u) AS (SELECT 'user' FROM g1 AS g_0) SELECT g_0.u FROM test AS g_0", Arrays.asList("user"));
		CommandContext cc = TestProcessor.createCommandContext();
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc);
	    
	    helpProcess(plan, cc, dataManager, expected);
	}

	@Test public void testWithAndUncorrelatedSubquery() throws TeiidComponentException, TeiidProcessingException {
	    String sql = "WITH t(n) AS ( select e1 from pm2.g1 ) SELECT n FROM t as t1, pm1.g1 where e1 = (select n from t)"; //$NON-NLS-1$

	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
	    CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT 1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = (WITH t (n) AS (SELECT g_0.e1 FROM pm2.g1 AS g_0) SELECT g_0.n FROM t AS g_0)", "WITH t (n) AS (SELECT g_0.e1 FROM pm2.g1 AS g_0) SELECT g_0.n FROM t AS g_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testScalarInlining() throws TeiidComponentException, TeiidProcessingException {
	    String sql = "WITH t(n) AS ( select 1 ) SELECT n FROM t as t1, pm1.g1"; //$NON-NLS-1$

	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT 1 FROM pm1.g1 AS g_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testEvaluatableSubqueryPushedWithCTE() throws TeiidComponentException, TeiidProcessingException {
		String sql = "WITH qry_0 as /*+ no_inline */ (SELECT e2 AS a1, e1 as str FROM pm1.g1 AS t), qry_1 as /*+ no_inline */ (SELECT 'b' AS a1) select (select a1 || 'a' from qry_1) as x, a1 from qry_0";
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
	    dataManager.addData("WITH qry_0 (a1, str) AS (SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0) SELECT g_0.a1 FROM qry_0 AS g_0", Arrays.asList(1));
		
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"WITH qry_0 (a1, str) AS (SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0) SELECT (SELECT concat(a1, 'a') FROM qry_1 LIMIT 2), g_0.a1 FROM qry_0 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
	    
	    TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList("ba", 1)});
	}
	
	@Test public void testSubqueryWith() throws Exception {
	    //full push down
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
		String sql = "with eee as (select * from pm1.g1) select * from pm1.g2 where pm1.g2.e1 in (select e1 from eee)";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    hdm.addData("WITH eee (e1, e2, e3, e4) AS (SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM g1 AS g_0) SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM g2 AS g_0 WHERE g_0.e1 IN (SELECT g_1.e1 FROM eee AS g_1)", Arrays.asList("a", 1, 3.0, true));
	    TestProcessor.helpProcess(plan, hdm, null);
	}
	
}
