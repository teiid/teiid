package org.teiid.query.processor;

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.api.exception.query.QueryValidatorException;
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
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;

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
	
	@Test public void testSingleItemInView() throws TeiidComponentException, TeiidProcessingException {
	    String sql = "select * from (with a (x, y, z) as /*+ no_inline */ (select e1, e2, e3 from pm1.g1) SELECT pm1.g2.e2, a.x, z from pm1.g2, a where e1 = x order by x) as x where z = 1"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	        Arrays.asList(0, "a", true),
	        Arrays.asList(3, "a", true),
	        Arrays.asList(1, "c", true),
	        Arrays.asList(0, "a", true),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    dataManager.setBlockOnce();
	    sampleData1(dataManager);
	    
	    BasicSourceCapabilities typicalCapabilities = TestOptimizer.getTypicalCapabilities();
	    typicalCapabilities.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
		ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(typicalCapabilities), 
	    		new String[] {"SELECT a.x, a.z FROM a WHERE a.z = TRUE ORDER BY a.x", "SELECT g_0.e1, g_0.e2 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager, expected);
	    
	    //combined when inlined
	    sql = "SELECT g_0.e2, g_1.e1, g_1.e3 FROM pm1.g2 AS g_0, pm1.g1 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_1.e3 = TRUE)";
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(typicalCapabilities), 
	    		new String[] {"SELECT g_0.e2, g_1.e1, g_1.e3 FROM pm1.g2 AS g_0, pm1.g1 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_1.e3 = TRUE)"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testMultipleItems() {
	    String sql = "with a (x, y, z) as /*+ no_inline */ (select e1, e2, e3 from pm1.g1), b as /*+ no_inline */ (SELECT * from pm1.g2, a where e1 = x and z = 1 order by e2 limit 2) SELECT a.x, b.e1 from a, b where a.x = b.e1"; //$NON-NLS-1$
	    
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
        
	    String sql = "with a (x, y, z) as /*+ no_inline */ (select e1, e2, e3 from pm1.g1) SELECT a.x from a, a z"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager();
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a"),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS /*+ no_inline */ (SELECT g_0.e1, null, UNKNOWN FROM pm1.g1 AS g_0) SELECT g_0.x FROM a AS g_0, a AS g_1", expected);
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS /*+ no_inline */ (SELECT g_0.e1, null, UNKNOWN FROM pm1.g1 AS g_0) SELECT g_0.x FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testWithPushdownUnused() throws TeiidException {
		 FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       
	    String sql = "with a (x, y, z) as /*+ no_inline */ (select e1, e2, e3 from pm1.g1) SELECT e1 from pm1.g1, a"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager();
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a"),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS /*+ no_inline */ (SELECT null, null, UNKNOWN FROM pm1.g1 AS g_0) SELECT g_0.e1 FROM pm1.g1 AS g_0, a AS g_1", expected);
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS /*+ no_inline */ (SELECT null, null, UNKNOWN FROM pm1.g1 AS g_0) SELECT g_0.e1 FROM pm1.g1 AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager, expected);
	    
	    sql = "with a (x, y, z) as (select e1, e2, e3 from pm1.g1) SELECT e1 from pm1.g1, a"; //$NON-NLS-1$
	    
	    plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g1 AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithPushdownMultiple() throws TeiidException {
		 FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
       BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
       caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
       caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
       capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
      
	    String sql = "with a (x, y, z) as  /*+ no_inline */ (select e1, e2, e3 from pm1.g1), b (x, y) as /*+ no_inline */ (select e1, y from pm1.g2, a) SELECT e1 from pm1.g1, b"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager();
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a"),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS /*+ no_inline */ (SELECT null, g_0.e2, UNKNOWN FROM pm1.g1 AS g_0), b (x, y) AS /*+ no_inline */ (SELECT null, null FROM pm1.g2 AS g_0, a AS g_1) SELECT g_0.e1 FROM pm1.g1 AS g_0, b AS g_1", expected);
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS /*+ no_inline */ (SELECT null, g_0.e2, UNKNOWN FROM pm1.g1 AS g_0), b (x, y) AS /*+ no_inline */ (SELECT null, null FROM pm1.g2 AS g_0, a AS g_1) SELECT g_0.e1 FROM pm1.g1 AS g_0, b AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testWithPushdown1() throws TeiidException {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       
	    String sql = "with a as /*+ no_inline */ (select x, y, z from (select e1 as x, e2 as y, e3 as z from pm1.g1) v) SELECT count(a.x) from a, a z"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a", 1, Boolean.FALSE),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS (SELECT g_0.e1, NULL, NULL FROM g1 AS g_0) SELECT COUNT(g_0.x) FROM a AS g_0, a AS g_1", expected);
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS /*+ no_inline */ (SELECT g_0.e1, null, UNKNOWN FROM pm1.g1 AS g_0) SELECT COUNT(g_0.x) FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
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
       
	    String sql = "with a as /*+ no_inline */ (select x, y, z from (select e1 as x, e2 as y, e3 as z from pm1.g1) v), b as /*+ no_inline */ (select e4 from pm1.g3) SELECT count(a.x), max(a.y) from a, a z group by z.x having max(a.y) < (with b as /*+ no_inline */ (select e1 from pm1.g1) select a.y from a, b where a.x = z.x)"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a", 1, "a"),
		    };    

	    dataManager.addData("WITH a (x, y, z) AS (SELECT g_0.e1, g_0.e2, NULL FROM g1 AS g_0) SELECT g_1.x, g_0.y, g_0.x FROM a AS g_0, a AS g_1", expected);
	    dataManager.addData("WITH b__1 (e1) AS (SELECT NULL FROM g1 AS g_0), a (x, y, z) AS (SELECT g_0.e1, g_0.e2, NULL FROM g1 AS g_0) SELECT g_0.y FROM a AS g_0, b__1 AS g_1 WHERE g_0.x = 'a'", 
	    		new List[] {Arrays.asList(2)});
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y, z) AS /*+ no_inline */ (SELECT g_0.e1, g_0.e2, UNKNOWN FROM pm1.g1 AS g_0) SELECT g_1.x, g_0.y, g_0.x FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
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
       
	    String sql = "with a as /*+ no_inline */ (select e1 from pm1.g1), b as /*+ no_inline */ (select e1 from pm2.g2), c as /*+ no_inline */ (select count(*) as x from pm1.g1) SELECT a.e1, (select max(x) from c), pm1.g1.e2 from pm1.g1, a, b"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());

	    dataManager.addData("WITH a (e1) AS (SELECT g_0.e1 FROM g1 AS g_0) SELECT g_0.e1 FROM a AS g_0",  new List[] { 
		        Arrays.asList("a"),
		    });
	    dataManager.addData("WITH b (e1) AS (SELECT NULL FROM g2 AS g_0) SELECT 1 FROM b AS g_0",  new List[] { 
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
	    	"WITH b (e1) AS /*+ no_inline */ (SELECT null FROM pm2.g2 AS g_0) SELECT 1 FROM b AS g_0", 
	    	"WITH a (e1) AS /*+ no_inline */ (SELECT g_0.e1 FROM pm1.g1 AS g_0) SELECT g_0.e1 FROM a AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
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
       
	    String sql = "with a (x, y) as /*+ no_inline */ (select 1, 2 from pm1.g1) SELECT a.x from a, a z"; //$NON-NLS-1$
	    
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y) AS /*+ no_inline */ (SELECT 1, null FROM pm1.g1 AS g_0) SELECT g_0.x FROM a AS g_0, a AS g_1"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithOrderBy() throws TeiidException {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
      
	    String sql = "with a (x, y) as /*+ no_inline */ (select 1, 2 from pm1.g1) SELECT a.x from a, a z order by x"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("WITH a (x, y) AS (SELECT 1, NULL FROM g1 AS g_0) SELECT g_0.x AS c_0 FROM a AS g_0, a AS g_1 ORDER BY c_0", new List[0]);
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, y) AS /*+ no_inline */ (SELECT 1, null FROM pm1.g1 AS g_0) SELECT g_0.x AS c_0 FROM a AS g_0, a AS g_1 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    helpProcess(plan, dataManager,  new List[0]);
	}
	
	@Test public void testWithJoinPlanning() throws TeiidException {
		TransformationMetadata metadata = RealMetadataFactory.example1();
		RealMetadataFactory.setCardinality("pm1.g2", 100000, metadata);
	    String sql = "with a (x) as /*+ no_inline */ (select e1 from pm1.g1) SELECT a.x from pm1.g2, a where (pm1.g2.e1 = a.x)"; //$NON-NLS-1$
	    
	    TestOptimizer.helpPlan(sql, metadata, null, TestOptimizer.getGenericFinder(false), new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT a.x FROM a ORDER BY a.x"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithJoinPlanning1() throws TeiidException {
		TransformationMetadata metadata = RealMetadataFactory.example1Cached();
	    String sql = "with a (x) as /*+ no_inline */ (select e1 from pm1.g1) SELECT a.x from pm1.g2, a where (pm1.g2.e1 = a.x)"; //$NON-NLS-1$
	    
	    TestOptimizer.helpPlan(sql, metadata, null, TestOptimizer.getGenericFinder(false), new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT a.x FROM a ORDER BY a.x"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithBlockingJoin() throws TeiidException {
	      
	    String sql = "with a (x, y) as /*+ no_inline */ (select e1, e2 from pm1.g1) SELECT a.x, a.y, pm1.g2.e1 from a left outer join pm1.g2 makenotdep on (rtrim(a.x) = pm1.g2.e1) order by a.y"; //$NON-NLS-1$
	    
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
	    dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("a")});
	    dataManager.addData("SELECT g_0.e1 FROM pm2.g1 AS g_0 LEFT OUTER JOIN pm2.g2 AS g_1 ON g_0.e2 = g_1.e2 AND g_0.e1 = 'a'", new List[] {Arrays.asList("a")});
	    
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
	    bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, true);
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc));
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testWithGroupingAndMultiElement() {
		String sql = "WITH qry_0 as (SELECT floor(t.e4) AS a1, floor(t2.e4) as b1 FROM pm1.g1 AS t, pm2.g2 as t2 WHERE (t.e4=t2.e4) GROUP BY t.e4, t2.e4) SELECT * from qry_0 GROUP BY a1, b1";
		
		List[] expected = new List[] {Arrays.asList(3.0, 3.0)};    
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
	    dataManager.addData("SELECT g_0.e4 AS c_0 FROM pm1.g1 AS g_0 GROUP BY g_0.e4 ORDER BY c_0", Arrays.asList(2.1), Arrays.asList(3.2));
	    dataManager.addData("SELECT g_0.e4 AS c_0 FROM pm2.g2 AS g_0 GROUP BY g_0.e4 ORDER BY c_0", Arrays.asList(2.0), Arrays.asList(3.2));
		
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testSubqueryWithGrouping() {
		String sql = "select q.str_a, q.a from(WITH qry_0 as /*+ no_inline */ (SELECT e2 AS a1, e1 as str FROM pm1.g1 AS t) SELECT a1 as a, str as str_a from qry_0) as q group by q.str_a, q.a";
		
		List[] expected = new List[] {Arrays.asList("a", 1)};    
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
	    dataManager.addData("SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0", Arrays.asList(1, "a"));
		
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
	    
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testFunctionEvaluation() throws Exception {
		String sql = "with test as /*+ no_inline */ (select user() as u from pm1.g1) select u from test";
		
		List[] expected = new List[] {Arrays.asList("user")};    
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("WITH test (u) AS (SELECT 'user' FROM g1 AS g_0) SELECT g_0.u FROM test AS g_0", Arrays.asList("user"));
		CommandContext cc = TestProcessor.createCommandContext();
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc);
	    
	    helpProcess(plan, cc, dataManager, expected);
	    
	    sql = "with test as (select user() as u from pm1.g1) select u from test";
		
		dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("SELECT 'user' FROM g1 AS g_0", Arrays.asList("user"));
	    plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc);
	    
	    helpProcess(plan, cc, dataManager, expected);
	}
	
	@Test public void testRecursive() throws Exception {
	    String sql = "WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 64 ) SELECT sum(n) FROM t;"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	        Arrays.asList(2080l),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    dataManager.setBlockOnce();
	    sampleData1(dataManager);
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
	    CommandContext cc = createCommandContext();
	    cc.setSession(new SessionMetadata());
	    helpProcess(plan, cc, dataManager, expected);
	}
	
	@Test(expected=TeiidProcessingException.class) public void testMaxRecursive() throws Exception {
	    String sql = "WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 64 ) SELECT sum(n) FROM t;"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	        Arrays.asList(2080l),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
	    CommandContext cc = createCommandContext();
	    cc.setSession(new SessionMetadata());
	    cc.setSessionVariable(TempTableStore.TEIID_MAX_RECURSION, 10);
	    helpProcess(plan, cc, dataManager, expected);
	}
	
	@Test public void testRecursiveUnion() throws Exception {
	    String sql = "WITH t(n) AS ( (VALUES (1) union all values(2)) UNION (SELECT n+1 FROM t WHERE n < 64 union all SELECT e2 from pm1.g1) ) SELECT sum(n) FROM t;"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	        Arrays.asList(2080l),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    dataManager.setBlockOnce();
	    sampleData1(dataManager);
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
	    CommandContext cc = createCommandContext();
	    cc.setSession(new SessionMetadata());
	    helpProcess(plan, cc, dataManager, expected);
	}
	
	@Test public void testRecursivePushdown() throws TeiidComponentException, TeiidProcessingException {
	    String sql = "WITH t(n) AS ( select e2 from pm1.g1 UNION SELECT n+1 FROM t WHERE n < 64 ) SELECT n FROM t"; //$NON-NLS-1$

	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
	    bsc.setCapabilitySupport(Capability.RECURSIVE_COMMON_TABLE_EXPRESSIONS, true);
	    bsc.setFunctionSupport("+", true);
	    CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"WITH t (n) AS (SELECT g_0.e2 FROM pm1.g1 AS g_0 UNION SELECT (g_0.n + 1) FROM t AS g_0 WHERE g_0.n < 64) SELECT g_0.n FROM t AS g_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testScalarInlining() throws TeiidComponentException, TeiidProcessingException {
	    String sql = "WITH t(n) AS ( select 1 ) SELECT n FROM t as t1, pm1.g1"; //$NON-NLS-1$

	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT 1 FROM pm1.g1 AS g_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
	}

	@Test public void testWithAndUncorrelatedSubquery() throws TeiidComponentException, TeiidProcessingException {
	    String sql = "WITH t(n) AS /*+ no_inline */ ( select e1 from pm2.g1 ) SELECT n FROM t as t1, pm1.g1 where e1 = (select n from t)"; //$NON-NLS-1$

	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
	    CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT 1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = (WITH t (n) AS /*+ no_inline */ (SELECT g_0.e1 FROM pm2.g1 AS g_0) SELECT g_0.n FROM t AS g_0)", "WITH t (n) AS /*+ no_inline */ (SELECT g_0.e1 FROM pm2.g1 AS g_0) SELECT g_0.n FROM t AS g_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithPushdownNested() throws TeiidException {
		 FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        caps.setCapabilitySupport(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       
	    String sql = "SELECT (with b (x) as /*+ no_inline */ (select e1 from pm1.g1) select b.x || c.x from b,b b1), x from (with a (x, b, c) as /*+ no_inline */  (select e1, e2, e3 from pm1.g1) select * from a limit 1) as c"; //$NON-NLS-1$
	    
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, b, c) AS /*+ no_inline */ (SELECT g_0.e1, g_0.e2, g_0.e3 FROM pm1.g1 AS g_0) SELECT (WITH b (x) AS /*+ no_inline */ (SELECT g_0.e1 FROM pm1.g1 AS g_0) SELECT concat(g_1.x, v_0.c_0) FROM b AS g_1, b AS g_2), v_0.c_0 FROM (SELECT g_0.x AS c_0 FROM a AS g_0 LIMIT 1) AS v_0"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    caps.setCapabilitySupport(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, false);
	    
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"WITH a (x, b, c) AS /*+ no_inline */ (SELECT g_0.e1, g_0.e2, g_0.e3 FROM pm1.g1 AS g_0) SELECT g_0.x AS c_0 FROM a AS g_0 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    sql = "SELECT (with b (x) as (select e1 from pm1.g1) select b.x || c.x from b,b b1), x from (with a (x, b, c) as (select e1, e2, e3 from pm1.g1) select * from a limit 1) as c"; //$NON-NLS-1$
	    
	    caps.setCapabilitySupport(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, true);
	    
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"SELECT (WITH b (x) AS (SELECT g_0.e1 FROM pm1.g1 AS g_0) SELECT concat(g_1.x, v_0.c_0) FROM b AS g_1, b AS g_2), v_0.c_0 FROM (SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 LIMIT 1) AS v_0"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testWithPushdownNestedInsert() throws Exception {
	    FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
      
	    String sql = "insert into pm1.g1 (e1) with a (x) as /*+ no_inline */ (select e1 from pm1.g1) select a.x from a, a y"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
		        Arrays.asList(1),
		    };
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("INSERT INTO g1 (e1) WITH a (x) AS (SELECT g_0.e1 FROM g1 AS g_0) SELECT g_0.x FROM a AS g_0, a AS g_1", Arrays.asList(1));
		CommandContext cc = TestProcessor.createCommandContext();
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc);
	    
	    helpProcess(plan, cc, dataManager, expected);
	    
	    //should be the same either way.  up to the translator to deal with the with clause
	    caps.setCapabilitySupport(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, false);
	    cc = TestProcessor.createCommandContext();
	    plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc);
	    helpProcess(plan, cc, dataManager, expected);
	}
	
	@Test public void testWithPushdownNestedUpdate() throws Exception {
	    FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.SUBQUERY_COMMON_TABLE_EXPRESSIONS, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
      
	    String sql = "update pm1.g1 set e1 = (with a (x) as /*+ no_inline */ (select e1 from pm1.g2 limit 1) select a.x || pm1.g1.e1 from a)"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
		        Arrays.asList(1),
		    };
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("UPDATE g1 SET e1 = (WITH a (x) AS (SELECT g_0.e1 AS c_0 FROM g2 AS g_0 LIMIT 1) SELECT concat(g_0.x, g1.e1) AS c_0 FROM a AS g_0)", Arrays.asList(1));
		CommandContext cc = TestProcessor.createCommandContext();
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc);
	    
	    helpProcess(plan, cc, dataManager, expected);
	}
	
	@Test public void testWithPushdownAndConstants() throws Exception {
		String sql = "WITH tmp as  /*+ no_inline */ (SELECT * FROM pm1.g1 ) SELECT 123 as col2, tmp.* FROM tmp";
		
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    dataManager.addData("WITH tmp (e1, e2, e3, e4) AS (SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM g1 AS g_0) SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM tmp AS g_0", Arrays.asList("a", 1, true, 1.1));
		CommandContext cc = TestProcessor.createCommandContext();
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    
	    List<?>[] expected = new List[] { 
		        Arrays.asList(123, "a", 1, true, 1.1),
		    };
	    
	    helpProcess(plan, cc, dataManager, expected);
	}
	
	/**
	 * Expected to fail as we shouldn't allow a reference to p.e2 in the windowed sum
	 * @throws Exception
	 */
	@Test(expected=QueryValidatorException.class) public void testWithAggregation() throws Exception {
		String sql = "WITH x as (SELECT e1 FROM pm1.g1) SELECT p.e1, SUM(p.e2) OVER (partition by p.e1) as y FROM pm1.g1 p JOIN x ON x.e1 = p.e1 GROUP BY p.e1";
		
		CommandContext cc = TestProcessor.createCommandContext();
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	}
	
	@Test public void testWithAggregation1() throws Exception {
		String sql = "WITH x as /*+ no_inline */ (SELECT e1 FROM pm1.g1) SELECT p.e1, SUM(max(p.e2)) OVER (partition by p.e1) as y FROM pm1.g1 p JOIN x ON x.e1 = p.e1 GROUP BY p.e1";
		
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
		dataManager.addData("SELECT g_0.e1 FROM g1 AS g_0", Arrays.asList("a"), Arrays.asList("b"), Arrays.asList("a"));
		dataManager.addData("SELECT g_0.e1, g_0.e2 FROM g1 AS g_0 WHERE g_0.e1 IN ('a', 'b')", Arrays.asList("a", 1), Arrays.asList("a", 2), Arrays.asList("b", 3)); 
		CommandContext cc = TestProcessor.createCommandContext();
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    
	    List<?>[] expected = new List[] { 
		        Arrays.asList("a", 2l),
		        Arrays.asList("b", 3l),
		    };
	    
	    helpProcess(plan, cc, dataManager, expected);
	    
	    //full push down
	    cc = TestProcessor.createCommandContext();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
		bsc.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
		bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
		bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
	    plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    TestOptimizer.checkAtomicQueries(new String[] {"WITH x (e1) AS /*+ no_inline */ (SELECT g_0.e1 FROM pm1.g1 AS g_0) SELECT g_0.e1, SUM(MAX(g_0.e2)) OVER (PARTITION BY g_0.e1) FROM pm1.g1 AS g_0, x AS g_1 WHERE g_1.e1 = g_0.e1 GROUP BY g_0.e1"}, plan);
	}
	
	@Test public void testSubqueryWith() throws Exception {
	    //full push down
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
		String sql = "with eee as /*+ no_inline */ (select * from pm1.g1) select * from pm1.g2 where pm1.g2.e1 in (select e1 from eee)";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    hdm.addData("WITH eee (e1, e2, e3, e4) AS (SELECT g_0.e1, NULL, NULL, NULL FROM g1 AS g_0) SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM g2 AS g_0 WHERE g_0.e1 IN (SELECT g_1.e1 FROM eee AS g_1)", Arrays.asList("a", 1, 3.0, true));
	    TestProcessor.helpProcess(plan, hdm, null);
	}
	
	@Test public void testSubqueryWithSemijoin() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
		String sql = "with eee as /*+ no_inline */ (select * from pm2.g1) select * from pm1.g2 where pm1.g2.e1 in (select e1 from eee)";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    //we need the with associated with the subquery
	    hdm.addData("WITH eee (e1, e2, e3, e4) AS (SELECT g_0.e1, NULL, NULL, NULL FROM g1 AS g_0) SELECT g_0.e1 AS c_0 FROM eee AS g_0 ORDER BY c_0", Arrays.asList("a"), Arrays.asList("b"));
	    hdm.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM g2 AS g_0 WHERE g_0.e1 IN ('a', 'b') ORDER BY c_0", Arrays.asList("a", 1, 2.0, true), Arrays.asList("b", 2, 3.0, false));
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", 1, 2.0, true), Arrays.asList("b", 2, 3.0, false)});
	    
	    sql = "with eee as (select * from pm2.g1) select * from pm1.g2 where pm1.g2.e1 in (select e1 from eee)";
	    plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    hdm.addData("SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM g2 AS g_0", Arrays.asList("a", 1, 2.0, true), Arrays.asList("b", 2, 3.0, false));
	    hdm.addData("SELECT g_0.e1 FROM g1 AS g_0", Arrays.asList("a"), Arrays.asList("b"));
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", 1, 2.0, true), Arrays.asList("b", 2, 3.0, false)});
	}
	
	@Test public void testSubqueryWithSemijoinMultiLevel() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
		String sql = "with eee as /*+ no_inline */ (with aaa as /*+ no_inline */ (select e1 from pm3.g1) select e1 from pm2.g1 where e1 in (select e1 from aaa)) select * from pm1.g2 where pm1.g2.e1 in (select e1 from eee)";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    //we need the with associated with the subquery
	    hdm.addData("WITH aaa (e1) AS (SELECT g_0.e1 FROM g1 AS g_0) SELECT g_0.e1 AS c_0 FROM aaa AS g_0 ORDER BY c_0", Arrays.asList("a"));
	    hdm.addData("SELECT g_0.e1 AS c_0 FROM g1 AS g_0 WHERE g_0.e1 = 'a' ORDER BY c_0", Arrays.asList("a"));
	    hdm.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM g2 AS g_0 WHERE g_0.e1 = 'a' ORDER BY c_0", Arrays.asList("a", 1, 2.0, true));
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", 1, 2.0, true)});
	    
	    sql = "with eee as (with aaa as (select e1 from pm3.g1) select e1 from pm2.g1 where e1 in (select e1 from aaa)) select * from pm1.g2 where pm1.g2.e1 in (select e1 from eee)";
	    plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    hdm.addData("SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM g2 AS g_0", Arrays.asList("a", 1, 2.0, true), Arrays.asList("b", 1, 2.0, true));
	    hdm.addData("SELECT g_0.e1 FROM g1 AS g_0", Arrays.asList("a"), Arrays.asList("c"));
	    //we need the with associated with the subquery
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", 1, 2.0, true)});
	}
	
	@Test public void testRecursiveWithPushdownNotFullyPushed() throws Exception {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       
	    String sql = "WITH t(n, i) AS ( select 1,2 from pm1.g2 UNION ALL SELECT n+1, e2 FROM t, pm1.g1 WHERE n < 64 and pm1.g1.e2 = t.n ) SELECT * FROM t;"; //$NON-NLS-1$
	    
	    HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());

	    dataManager.addData("SELECT g_0.e2 AS c_0 FROM g1 AS g_0 WHERE g_0.e2 < 64 AND g_0.e2 = 1 ORDER BY c_0", Arrays.asList(1));
	    dataManager.addData("SELECT g_0.e2 AS c_0 FROM g1 AS g_0 WHERE g_0.e2 < 64 AND g_0.e2 = 2 ORDER BY c_0");
	    dataManager.addData("SELECT 2 FROM g2 AS g_0", Arrays.asList(2));
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"SELECT t.n, t.i FROM t"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    CommandContext cc = createCommandContext();
	    cc.setSession(new SessionMetadata());
	    
	    helpProcess(plan, cc, dataManager, new List[] { 
		        Arrays.asList(1, 2),Arrays.asList(2, 1),
		    });
	    
	    caps.setCapabilitySupport(Capability.RECURSIVE_COMMON_TABLE_EXPRESSIONS, true);
	    
	    plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, new String[] {"SELECT t.n, t.i FROM t"}, ComparisonMode.EXACT_COMMAND_STRING);
	    
	    cc = createCommandContext();
	    cc.setSession(new SessionMetadata());
	    
	    helpProcess(plan, cc, dataManager, new List[] { 
		        Arrays.asList(1, 2),Arrays.asList(2, 1),
		    });
	}
	
	@Test public void testMultiplePreviousReferences() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
		String sql = "WITH cte1 as /*+ no_inline */ (SELECT e1, e2 from pm1.g1), cte2 as /*+ no_inline */ (select * from cte1), cte3 as /*+ no_inline */ (select * from cte1) "
				+ "SELECT * FROM cte2 join cte3 on cte2.e1=cte3.e1";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    
	    //cte1 should appear once
	    hdm.addData("WITH cte1 (e1, e2) AS (SELECT g_0.e1, g_0.e2 FROM g1 AS g_0), "
	    		+ "cte2 (e1, e2) AS (SELECT g_0.e1, g_0.e2 FROM cte1 AS g_0), "
	    		+ "cte3 (e1, e2) AS (SELECT g_0.e1, g_0.e2 FROM cte1 AS g_0) SELECT g_1.e1, g_1.e2, g_0.e1, g_0.e2 FROM cte2 AS g_0, cte3 AS g_1 WHERE g_0.e1 = g_1.e1", Arrays.asList("a", 1, "b", 2));
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", 1, "b", 2)});
	}
	
	@Test public void testMultiplePreviousReferencesInlined() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
		String sql = "WITH cte1 as (SELECT e1, e2 from pm1.g1), cte2 as (select * from cte1), cte3 as (select * from cte1) "
				+ "SELECT * FROM cte2 join cte3 on cte2.e1=cte3.e1";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    
	    //cte1 should appear once
	    hdm.addData("WITH cte1 (e1, e2) AS (SELECT g_0.e1, g_0.e2 FROM g1 AS g_0) SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM cte1 AS g_0 ORDER BY c_0", Arrays.asList("a", 1));
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", 1, "a", 1)});
	}
	
	@Test public void testNestedWith() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
		String sql = "WITH cte1 as (SELECT 1 as a), cte3 as (with cte3_1 as (select cte1.a from cte1 join pm1.g1 t1 on cte1.a=t1.e2) select * from cte3_1) SELECT * FROM cte3";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    
	    hdm.addData("SELECT 1 FROM g1 AS g_0 WHERE g_0.e2 = 1", Arrays.asList(1));
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1)});
	}
	
	@Test public void testNestedWith1() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
	    String sql = "WITH cte1 as (SELECT 1 as a), cte3 as  /*+ no_inline */ (with cte3_1 as /*+ no_inline */ (select cte1.a from cte1 join pm1.g1 t1 on cte1.a=t1.e2) select * from cte3_1) SELECT * FROM cte3";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    
	    hdm.addData("WITH cte3_1 (a) AS (SELECT 1 FROM g1 AS g_0 WHERE g_0.e2 = 1), cte3 (a) AS (SELECT g_0.a FROM cte3_1 AS g_0) SELECT g_0.a FROM cte3 AS g_0", Arrays.asList(1));
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1)});
	}
	
	@Test public void testNestedWithRepeated() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
		String sql = "begin WITH cte1 as (SELECT 1 as a), cte3 as (with cte3_1 as /*+ no_inline */ (select cte1.a from cte1 join pm1.g1 t1 on cte1.a=t1.e2) select * from cte3_1) SELECT * FROM cte3;"
				+ " WITH cte1 as (SELECT 1 as a), cte3 as (with cte3_1 as /*+ no_inline */ (select cte1.a from cte1 join pm1.g1 t1 on cte1.a=t1.e2) select * from cte3_1) SELECT * FROM cte3; end";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
	    
	    hdm.addData("WITH cte3_1 (a) AS (SELECT 1 FROM g1 AS g_0 WHERE g_0.e2 = 1) SELECT g_0.a FROM cte3_1 AS g_0", Arrays.asList(1));
	    hdm.addData("WITH cte3_1__2 (a) AS (SELECT 1 FROM g1 AS g_0 WHERE g_0.e2 = 1) SELECT g_0.a FROM cte3_1__2 AS g_0", Arrays.asList(1));
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1)});
	}
	
	@Test public void testViewPlanning() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create view v1 as WITH mycte as (SELECT 1 as col1) SELECT col1 FROM mycte;", "x", "y");
		
		String sql = "WITH mycte as (SELECT * FROM y.v1) SELECT * from mycte;";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(metadata);
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1)});
	}
	
	@Test public void testSubqueryPushedWithCTE() throws TeiidComponentException, TeiidProcessingException {
		String sql = "WITH qry_0 as /*+ no_inline */ (SELECT e2 AS a1, e1 as str FROM pm1.g1 AS t) select (select e1 from pm1.g1) as x, a1 from qry_0";
		
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
	    bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
	    bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
	    
	    TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"WITH qry_0 (a1, str) AS /*+ no_inline */ (SELECT g_0.e2, null FROM pm1.g1 AS g_0) SELECT (SELECT g_1.e1 FROM pm1.g1 AS g_1), g_0.a1 FROM qry_0 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testEvaluatableSubqueryPushedWithCTE() throws TeiidComponentException, TeiidProcessingException {
		String sql = "WITH qry_0 as /*+ no_inline */ (SELECT e2 AS a1, e1 as str FROM pm1.g1 AS t), qry_1 as /*+ no_inline */ (SELECT 'b' AS a1) select (select a1 || 'a' from qry_1) as x, a1 from qry_0";
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
	    dataManager.addData("WITH qry_0 (a1, str) AS /*+ no_inline */ (SELECT g_0.e2, null FROM pm1.g1 AS g_0) SELECT g_0.a1 FROM qry_0 AS g_0", Arrays.asList(1));
		
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
	    bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
	    
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"WITH qry_0 (a1, str) AS /*+ no_inline */ (SELECT g_0.e2, null FROM pm1.g1 AS g_0) SELECT (SELECT concat(a1, 'a') FROM qry_1 LIMIT 2), g_0.a1 FROM qry_0 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
	    
	    
	    TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList("ba", 1)});
	}
	
	@Test public void testViewPlanningDeeplyNested() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table test_a (a varchar); "
				+ "create view tv1 as WITH alias as /*+ no_inline */ (SELECT a from test_a) "
				+ ",alias2 as /*+ no_inline */ (select t2.a as a1, t1.a from alias t1 join (SELECT a from test_a) t2 on t1.a=t2.a) "
				+ ",alias3 as /*+ no_inline */ (select t2.a as a1, t1.a from alias t1 join alias2 t2 on t1.a=t2.a) "
				+ "SELECT alias.a as a1 FROM alias;", "x", "y");
		
		String sql = "with CTE1 as /*+ no_inline */ (  select a1 from tv1), CTE2 as /*+ no_inline */ ( select a1 from tv1) select * from CTE1 as T1 join CTE1 as T2 on T1.a1=T2.a1";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(metadata);
	    hdm.addData("WITH alias (a) AS (SELECT g_0.a FROM test_a AS g_0), CTE1 (a1) AS (SELECT g_0.a FROM alias AS g_0) SELECT g_0.a1 AS c_0 FROM CTE1 AS g_0 ORDER BY c_0", Arrays.asList("a"));
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", "a")});
	}
	
	@Test public void testViewPlanningDeeplyNestedInline() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table test_a (a varchar); "
				+ "create view tv1 as WITH alias as (SELECT a from test_a), alias2 as (select t2.a as a1, t1.a from alias t1 join (SELECT a from test_a) t2 on t1.a=t2.a), alias3 as (select t2.a as a1, t1.a from alias t1 join alias2 t2 on t1.a=t2.a) SELECT alias3.a1 FROM alias2 join alias3 on alias3.a=alias2.a;", "x", "y");
		
		String sql = "with CTE1 as (  select a1 from (  with CTE11 as (select a1 from tv1) select a1 from CTE11 ) as  SUBQ1), CTE2 as ( select a1 from (  with CTE21 as (select a1 from tv1) select a1 from CTE21 ) as  SUBQ2) select * from CTE1 as T1 join CTE2 as T2 on T1.a1=T2.a1";
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(metadata);
	    
	    hdm.addData("WITH alias (a) AS (SELECT g_0.a FROM test_a AS g_0), alias2 (a1, a) AS (SELECT NULL, g_0.a FROM alias AS g_0, test_a AS g_1 WHERE g_0.a = g_1.a) SELECT g_1.a AS c_0, g_0.a AS c_1 FROM alias AS g_0, alias2 AS g_1 WHERE g_0.a = g_1.a ORDER BY c_0", Arrays.asList("a", "a"));
	    hdm.addData("WITH alias (a) AS (SELECT g_0.a FROM test_a AS g_0), alias2 (a1, a) AS (SELECT NULL, g_0.a FROM alias AS g_0, test_a AS g_1 WHERE g_0.a = g_1.a) SELECT g_0.a AS c_0 FROM alias2 AS g_0 ORDER BY c_0", Arrays.asList("a"));
	    hdm.addData("WITH alias (a) AS (SELECT g_0.a FROM test_a AS g_0), alias2 (a1, a) AS (SELECT NULL, g_0.a FROM alias AS g_0, test_a AS g_1 WHERE g_0.a = g_1.a) SELECT g_0.a AS c_0, g_1.a AS c_1 FROM alias AS g_0, alias2 AS g_1 WHERE g_0.a = g_1.a ORDER BY c_0", Arrays.asList("a", "a"));
	    hdm.addData("WITH alias__1 (a) AS (SELECT g_0.a FROM test_a AS g_0), alias2__1 (a1, a) AS (SELECT NULL, g_0.a FROM alias__1 AS g_0, test_a AS g_1 WHERE g_0.a = g_1.a) SELECT g_0.a AS c_0 FROM alias2__1 AS g_0 ORDER BY c_0", Arrays.asList("a"));
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("a", "a")});
	}
	
	@Test public void testViewPlanningDeeplyNestedInlineRepeatedCTEName() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
		
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table test_a (a varchar, b varchar); "
				+ "	create view tv0 as WITH alias2 (a) AS (SELECT a FROM (SELECT 1 AS a) AS cte1) SELECT cte3.a FROM alias2 INNER JOIN (SELECT a FROM alias2) AS cte3 ON cte3.a = alias2.a;"
				+ " create view tv1 as WITH cte1 as (SELECT a from test_a), alias2 as (select a from cte1), cte3 as (select a from alias2) SELECT cte3.a FROM alias2 join cte3 on cte3.a=alias2.a;"
				+ " create view tv2 as WITH alias2 as (select b, a from test_a), cte4 as (select a from alias2) SELECT cte4.a FROM cte4 join alias2 on cte4.a=alias2.a ;", "x", "y");
		
		String sql = "with CTE1 as ( select a from ( with CTE11 as (select a from tv0) select a from CTE11 ) as  SUBQ1), CTE2 as ( select a from ( with CTE21 as (select a from tv1) select a from CTE21 ) as  SUBQ2) select * from CTE1 as T1 join CTE2 as T2 on T1.a=T2.a";

		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(metadata);
	    hdm.addData("WITH alias2__1 (a) AS (SELECT g_0.a FROM test_a AS g_0) SELECT g_1.a AS c_0 FROM alias2__1 AS g_0, alias2__1 AS g_1 WHERE g_1.a = g_0.a AND g_1.a = '1' ORDER BY c_0", Arrays.asList("1"));
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("1", 1)});
	}
	
	@Test public void testViewPlanningDeeplyNestedInlineRepeatedCTEName1() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
		
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table test_a (a varchar, b varchar); "
				+ "	create view tv0 as WITH alias2 (a) AS (SELECT a FROM (SELECT 1 AS a) AS cte1) SELECT cte3.a FROM alias2 INNER JOIN (SELECT a FROM alias2) AS cte3 ON cte3.a = alias2.a;"
				+ " create view tv1 as WITH cte1 as (SELECT a from test_a), alias2 as (select a from cte1), cte3 as (select a from alias2) SELECT cte3.a FROM alias2 join cte3 on cte3.a=alias2.a;"
				+ " create view tv2 as WITH alias2 as (select b, a from test_a), cte4 as (select a from alias2) SELECT cte4.a FROM cte4 join alias2 on cte4.a=alias2.a ;", "x", "y");
		
		String sql = "with CTE1 as ( select a from ( with CTE11 as (select a from tv2) select a from CTE11 ) as  SUBQ1), CTE2 as ( select a from ( with CTE21 as (select a from tv2) select a from CTE21 ) as  SUBQ2) select * from CTE1 as T1 join CTE2 as T2 on T1.a=T2.a";

		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(metadata);
	    hdm.addData("WITH alias2 (b, a) AS (SELECT NULL, g_0.a FROM test_a AS g_0) SELECT g_2.a, g_0.a FROM alias2 AS g_0, alias2 AS g_1, alias2 AS g_2, alias2 AS g_3 WHERE g_2.a = g_3.a AND g_0.a = g_1.a AND g_0.a = g_2.a", Arrays.asList(1, 1));
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, 1)});
	}
	
	@Test public void testProjectionMinimizationWithInlined() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
		
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE foreign TABLE test_a(a integer, b integer)", "x", "y");
		
		String sql = "with CTE1 as (WITH CTE11 as (SELECT a from test_a), CTE21 as (select t1.a from CTE11 t1 join CTE11 t2 on t1.a=t2.a), CTE31 as (select a from CTE21) SELECT CTE31.a FROM CTE21 join CTE31 on CTE31.a=CTE21.a ) select * from CTE1";

		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(metadata);
	    hdm.addData("WITH CTE11 (a) AS (SELECT g_0.a FROM test_a AS g_0), CTE21 (a) AS (SELECT g_0.a FROM CTE11 AS g_0, CTE11 AS g_1 WHERE g_0.a = g_1.a) SELECT g_1.a FROM CTE21 AS g_0, CTE21 AS g_1 WHERE g_1.a = g_0.a", Arrays.asList(1));
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1)});
	}
	
	@Test public void testNestedInlining() throws Exception {
	    CommandContext cc = TestProcessor.createCommandContext();
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.COMMON_TABLE_EXPRESSIONS, true);
		bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
		
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE foreign TABLE test_a(a integer, b integer)", "x", "y");
		
		String sql = "with CTE1 as (WITH alias as (SELECT a from test_a), alias2 as (select t2.a as a1, t1.a from alias t1 join (SELECT 1 as a) t2 on t1.a=t2.a), CTE31 as (select t2.a as a1 from alias2 t2) SELECT CTE31.a1 FROM alias2 join CTE31 on CTE31.a1=alias2.a ), CTE2 as ( WITH alias as (SELECT 1 as a), alias2 as (select t2.a a1, t1.a from alias t1 join (SELECT 1 as a) t2 on t1.a=t2.a), CTE32 as (select t2.a from alias2 t2) SELECT CTE32.a FROM alias2 join CTE32 on CTE32.a=alias2.a ) select * from CTE1 as T1 join CTE2 as T2 on T1.a1=T2.a";

		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
	    HardcodedDataManager hdm = new HardcodedDataManager(metadata);
	    hdm.addData("WITH alias2 (a1, a) AS (SELECT NULL, g_0.a FROM test_a AS g_0 WHERE g_0.a = 1) SELECT g_1.a AS c_0 FROM alias2 AS g_0, alias2 AS g_1 WHERE g_1.a = g_0.a AND g_1.a = 1 ORDER BY c_0", Arrays.asList(1), Arrays.asList(1), Arrays.asList(1), Arrays.asList(1));
	    
	    TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(1, 1), Arrays.asList(1, 1), Arrays.asList(1, 1), Arrays.asList(1, 1)});
	}
	
}

