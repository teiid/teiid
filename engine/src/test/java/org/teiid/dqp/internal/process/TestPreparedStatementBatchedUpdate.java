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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.unittest.RealMetadataFactory;


/**
 * JUnit TestCase to test planning and caching of <code>PreparedStatement</code>
 * plans that contain batched updates.
 *
 */
public class TestPreparedStatementBatchedUpdate {

    @Test public void testBatchedUpdatePushdown() throws Exception {
        // Create query 
		String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        
		// Create a testable prepared plan cache
		SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>();
		
		// Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("UPDATE pm1.g1 SET e1 = ?, e3 = ? WHERE pm1.g1.e2 = ?", new List[] {Arrays.asList(4)}); //$NON-NLS-1$
		// Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BULK_UPDATE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        // batch with two commands
		ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
    	values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1) })));
    	
    	List<?>[] expected = new List[] { 
                Arrays.asList(4)
        };
    	
    	// Create the plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());
    	Update update = (Update)dataManager.getCommandHistory().iterator().next();
    	assertTrue(((Constant)update.getChangeList().getClauses().get(0).getValue()).isMultiValued());
    }
    
    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with same number of commands in the batch.
     * <p>
     * The test verifies that no errors occur when planning and executing the 
     * same batched command SQL with the same number of batched command parameter 
     * value sets.  For example, if the first executeBatch() call were to occur 
     * with two batched commands a repeated call with two batched commands  
     * should not result in an error during planning or execution and the value 
     * used in the second batched command should be used instead of any values 
     * from the first batched command.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager 
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective 
     * batch command.
     * <p>
     * The batched command "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1=null, pm1.g1.e3=false WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1='b', pm1.g1.e3=true WHERE pm1.g1.e2=5
     * <p>
     * The result should be that one command is in the plan cache and 
     * no plan creation, validation, or execution errors will occur and 
     * a predetermined set of queries were executed in the data manager.
     * 
     * @throws Exception
     */
    @Test public void testUpdateSameNumCmds() throws Exception {
        // Create query 
		String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
		// Create a testable prepared plan cache
		SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>();
		
		// Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);    
		
		// Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>(13);
        
		// Create expected results
        // first command should result in 2 rows affected
        // second command should result in 2 rows affected  
        List<?>[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(2) })
        };    

        // batch with two commands
		List<List<Object>> values = new ArrayList<List<Object>>(2);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
    	values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1) })));
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
    	
    	// Create the plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());

    	// Repeat with different number of commands in batch
    	// Create expected results
        // first command should result in 2 rows affected
        expected = new List[] { 
                Arrays.asList(new Object[] { new Integer(2) }),
                Arrays.asList(new Object[] { new Integer(0) })
        };    

        // batch with two commands
		values = new ArrayList<List<Object>>(1);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "b",  Boolean.TRUE, new Integer(5) })));  //$NON-NLS-1$
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'b', e3 = TRUE WHERE pm1.g1.e2 = 5")); //$NON-NLS-1$
    	
    	// Use the cached plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true,RealMetadataFactory.example1VDB());

    	// Verify all the queries that were run
    	assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }
    
    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with same number of commands in the batch.  Update is performed 
     * against a view model instead of a source model.
     * <p>
     * The test verifies that no errors occur when planning and executing the 
     * same batched command SQL with the same number of batched command parameter 
     * value sets.  For example, if the first executeBatch() call were to occur 
     * with two batched commands a repeated call with two batched commands  
     * should not result in an error during planning or execution and the value 
     * used in the second batched command should be used instead of any values 
     * from the first batched command.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager 
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective 
     * batch command.
     * <p>
     * The batched command "UPDATE vm1.g1 SET vm1.g1.e2=? WHERE vm1.g1.e1=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET e2=0 WHERE pm1.g1.e1='a'
     * UPDATE pm1.g1 SET e2=1 WHERE pm1.g1.e1='b'
     * <p>
     * UPDATE pm1.g1 SET e2=2 WHERE pm1.g1.e1='c'
     * UPDATE pm1.g1 SET e2=3 WHERE pm1.g1.e1='d'
     * <p>
     * The result should be that one command is in the plan cache and 
     * no plan creation, validation, or execution errors will occur and 
     * a predetermined set of queries were executed in the data manager.
     * 
     * @throws Exception
     */
    @Test public void testUpdateSameNumCmds_Virtual() throws Exception {
        // Create query 
		String preparedSql = "UPDATE vm1.g1 SET vm1.g1.e2=? WHERE vm1.g1.e1=?"; //$NON-NLS-1$
		// Create a testable prepared plan cache
		SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>();
		
		// Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);    
		
		// Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>();
        
		// Create expected results
        List<?>[] expected = new List[] { 
            Arrays.asList(3),
            Arrays.asList(1)
        };    

        // batch with two commands
		ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(0), "a" })));  //$NON-NLS-1$
    	values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(1), "b" })));  //$NON-NLS-1$
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 0 WHERE pm1.g1.e1 = 'a'")); //$NON-NLS-1$
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 1 WHERE pm1.g1.e1 = 'b'")); //$NON-NLS-1$
    	
    	// Create the plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false, RealMetadataFactory.example1VDB());

    	// Repeat
        expected = new List[] { 
    		Arrays.asList(1),
            Arrays.asList(0)
        };    

        // batch with two commands
		values = new ArrayList<ArrayList<Object>>(1);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(2), "c" })));  //$NON-NLS-1$
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(3), "d" })));  //$NON-NLS-1$
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 2 WHERE pm1.g1.e1 = 'c'")); //$NON-NLS-1$
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 3 WHERE pm1.g1.e1 = 'd'")); //$NON-NLS-1$
    	
    	// Use the cached plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true,RealMetadataFactory.example1VDB());

    	// Verify all the queries that were run
    	assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }
    
    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with varying number of commands in the batch.
     * <p>
     * The test verifies that no errors occur when planning and executing the 
     * same batched command SQL with varying number of batched command parameter 
     * value sets.  For example, if the first executeBatch() call were to occur 
     * with two batched commands a repeated call with only one batched command 
     * should not result in an error during planning or execution.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager 
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective 
     * batch command.
     * <p>
     * The batched command "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1=null, pm1.g1.e3=false WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1=null, pm1.g1.e3=false WHERE pm1.g1.e2=1
     * UPDATE pm1.g1 SET pm1.g1.e1='c', pm1.g1.e3=true WHERE pm1.g1.e2=4
     * UPDATE pm1.g1 SET pm1.g1.e1='b', pm1.g1.e3=true WHERE pm1.g1.e2=5
     * <p>
     * The result should be that three commands are in the plan cache and 
     * no plan creation, validation, or execution errors will occur and 
     * a predetermined set of queries were executed in the data manager.
     * 
     * @throws Exception
     */
    @Test public void testUpdateVarNumCmds() throws Exception {
        // Create query 
		String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
		// Create a testable prepared plan cache
		SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>();
		
		// Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);    
		
		// Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>(13);
        
		// Create expected results
        // first command should result in 2 rows affected
        // second command should result in 2 rows affected  
        List<?>[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(2) })
        };    

        // batch with two commands
		ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
    	values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1) })));
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
    	
    	// Create the plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false, RealMetadataFactory.example1VDB());

    	// Repeat with different number of commands in batch
    	// Create expected results
        // first command should result in 2 rows affected
        expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(2) })
        };    

        // batch with one command
		values = new ArrayList<ArrayList<Object>>(1);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
    	
    	// Use the cached plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true, RealMetadataFactory.example1VDB());

    	// Repeat with different number of commands in batch
		// Create expected results
        // first command should result in 2 rows affected
        // second command should result in 2 rows affected  
        // third command should result in 0 rows affected  
        // fourth command should result in 0 rows affected  
        expected = new List[] { 
                Arrays.asList(new Object[] { new Integer(2) }),
                Arrays.asList(new Object[] { new Integer(2) }),
                Arrays.asList(new Object[] { new Integer(0) }),
                Arrays.asList(new Object[] { new Integer(0) })
        };    

        // batch with four commands
		values = new ArrayList<ArrayList<Object>>(4);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0)} )));  //$NON-NLS-1$
    	values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1)} )));
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "c",  Boolean.TRUE, new Integer(4)} )));  //$NON-NLS-1$
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "b",  Boolean.TRUE, new Integer(5)} )));  //$NON-NLS-1$

    	// Add our expected queries to the final query list
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'c', e3 = TRUE WHERE pm1.g1.e2 = 4")); //$NON-NLS-1$
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'b', e3 = TRUE WHERE pm1.g1.e2 = 5")); //$NON-NLS-1$
    	
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true, RealMetadataFactory.example1VDB());

    	// Verify all the queries that were run
    	assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }
    
    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with varying number of commands in the batch.  Update is 
     * performed against a view model instead of a source model.
     * <p>
     * The test verifies that no errors occur when planning and executing the 
     * same batched command SQL with varying number of batched command parameter 
     * value sets.  For example, if the first executeBatch() call were to occur 
     * with two batched commands a repeated call with only one batched command 
     * should not result in an error during planning or execution.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager 
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective 
     * batch command.
     * <p>
     * The batched command "UPDATE vm1.g1 SET vm1.g1.e1=?, vm1.g1.e3=? WHERE vm1.g1.e2=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET e1='a', e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET e1='b', e3=true WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET e1='c', e3=false WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET e1='d', e3=false WHERE pm1.g1.e2=1
     * UPDATE pm1.g1 SET e1='e', e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET e1='f', e3=true WHERE pm1.g1.e2=2
     * UPDATE pm1.g1 SET e1='g', e3=true WHERE pm1.g1.e2=3
     * <p>
     * The result should be that three commands are in the plan cache and 
     * no plan creation, validation, or execution errors will occur and 
     * a predetermined set of queries were executed in the data manager.
     * 
     * @throws Exception
     */
    @Test public void testUpdateVarNumCmds_Virtual() throws Exception {
        // Create query 
		String preparedSql = "UPDATE vm1.g1 SET vm1.g1.e1=?, vm1.g1.e3=? WHERE vm1.g1.e2=?"; //$NON-NLS-1$
		// Create a testable prepared plan cache
		SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>();
		
		// Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);    
		
		// Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>(13);
        
		// Create expected results
        List<?>[] expected = new List[] { 
                Arrays.asList(2),
                Arrays.asList(2)
        };    

        // batch with two commands
		ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
    	values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "b", Boolean.TRUE, new Integer(1) })));  //$NON-NLS-1$
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'b', e3 = TRUE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
    	
    	// Create the plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false, RealMetadataFactory.example1VDB());

    	// Repeat with different number of commands in batch
        expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(2) })
        };    

        // batch with one command
		values = new ArrayList<ArrayList<Object>>(1);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "c",  Boolean.FALSE, new Integer(1) })));  //$NON-NLS-1$
    	
    	// Add our expected queries to the final query list
   		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'c', e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
    	
    	// Use the cached plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true, RealMetadataFactory.example1VDB());

    	// Repeat with different number of commands in batch
        expected = new List[] { 
                Arrays.asList(2),
                Arrays.asList(2),
                Arrays.asList(1),
                Arrays.asList(1)
        };    

        // batch with four commands
		values = new ArrayList<ArrayList<Object>>(4);
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "d",  Boolean.FALSE, new Integer(1)} )));  //$NON-NLS-1$
    	values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "e", Boolean.FALSE, new Integer(0)} )));  //$NON-NLS-1$ 
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "f",  Boolean.TRUE, new Integer(2)} )));  //$NON-NLS-1$
		values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "g",  Boolean.TRUE, new Integer(3)} )));  //$NON-NLS-1$

    	// Add our expected queries to the final query list
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'd', e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'e', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'f', e3 = TRUE WHERE pm1.g1.e2 = 2")); //$NON-NLS-1$
		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'g', e3 = TRUE WHERE pm1.g1.e2 = 3")); //$NON-NLS-1$
    	
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true,RealMetadataFactory.example1VDB());

    	// Verify all the queries that were run
    	assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }

}
