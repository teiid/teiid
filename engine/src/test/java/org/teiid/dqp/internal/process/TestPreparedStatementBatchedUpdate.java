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
import org.teiid.dqp.internal.process.TestPreparedStatement.TestablePreparedPlanCache;

import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.processor.FakeDataManager;
import com.metamatrix.query.processor.HardcodedDataManager;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.unittest.FakeMetadataFactory;

/**
 * JUnit TestCase to test planning and caching of <code>PreparedStatement</code>
 * plans that contain batched updates.
 *
 */
public class TestPreparedStatementBatchedUpdate {

    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with varying number of commands in the batch.
     * <p>
     * The test verifies that no errors occur when planning the same batched 
     * command SQL with varying number of batched command parameter value sets.
     * For example, if the first executeBatch() call were to occur with three 
     * batched commands a repeated call with only two batched commands should 
     * not result in an error.
     * <p>
     * The test also verifies that a cached version of the PreparedStatement plan 
     * is used on each subsequent execution of the same SQL command even though 
     * the number of batched commands may vary. 
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
     * The result should be that only one command is in the plan cache and 
     * no plan creation or validation errors will occur.
     * 
     * @throws Exception
     */
    @Test public void testPlanCache_VarNumCmds() throws Exception {
        // Create query 
		String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        
		// Create PrepareedPlanCache
		PreparedPlanCache prepPlanCache = new PreparedPlanCache();
		
        // batch with two commands
		ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
		values.add( new ArrayList<Object>( Arrays.asList( new Object[] { "a",  Boolean.FALSE, new Integer(0) } ) ) );  //$NON-NLS-1$
    	values.add( new ArrayList<Object>( Arrays.asList( new Object[] { null, Boolean.FALSE, new Integer(1) } ) ) );
    	
        //Create plan
    	TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, prepPlanCache);

        // batch with one command
		values = new ArrayList<ArrayList<Object>>(1);
		values.add( new ArrayList<Object>( Arrays.asList( new Object[] { "a",  Boolean.FALSE, new Integer(0) } ) ) );  //$NON-NLS-1$
    	
        //Create plan
		TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, prepPlanCache);

        // batch with four commands
		values = new ArrayList<ArrayList<Object>>(4);
		values.add( new ArrayList<Object>( Arrays.asList( new Object[] { "a",  Boolean.FALSE, new Integer(0) } ) ) );  //$NON-NLS-1$
    	values.add( new ArrayList<Object>( Arrays.asList( new Object[] { null, Boolean.FALSE, new Integer(1) } ) ) );
		values.add( new ArrayList<Object>( Arrays.asList( new Object[] { "c",  Boolean.TRUE, new Integer(4) } ) ) );  //$NON-NLS-1$
		values.add( new ArrayList<Object>( Arrays.asList( new Object[] { "b",  Boolean.TRUE, new Integer(5) } ) ) );  //$NON-NLS-1$
    	
        //Create plan
		TestPreparedStatement.helpGetProcessorPlan(preparedSql, values, prepPlanCache);
		assertEquals("PreparedPlanCache size is invalid - ", 1, prepPlanCache.getSpaceUsed() ); //$NON-NLS-1$
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
     * The test also verifies that a cached version of the PreparedStatement plan 
     * is used on each subsequent execution of the same SQL command even though 
     * the number of batched commands may vary. 
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager 
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective 
     * batch.
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
     * The result should be that only one command is in the plan cache and 
     * no plan creation, validation, or execution errors will occur and 
     * a predetermined set of queries were executed in the data manager.
     * 
     * @throws Exception
     */
    @Test public void testProcessor_VarNumCmds() throws Exception {
        // Create query 
		String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        int executionsPerTest = 2;
		// Create a testable prepared plan cache
		TestablePreparedPlanCache prepPlanCache = new TestablePreparedPlanCache();
		
		// Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);    
		
		// Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
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
    	for ( int i = 0; i < executionsPerTest; i++ ) {
    		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
    		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
    	}
    	
    	// Create the plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, FakeMetadataFactory.example1Cached(), prepPlanCache, false, false, false);

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
    	for (int i = 0; i < executionsPerTest; i++) {
    		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
    	}
    	
    	// Use the cached plan and process the query
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, FakeMetadataFactory.example1Cached(), prepPlanCache, false, false, true);

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
    	for (int i = 0; i < executionsPerTest; i++) {
    		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
    		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
    		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'c', e3 = TRUE WHERE pm1.g1.e2 = 4")); //$NON-NLS-1$
    		finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'b', e3 = TRUE WHERE pm1.g1.e2 = 5")); //$NON-NLS-1$
    	}
    	
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, FakeMetadataFactory.example1Cached(), prepPlanCache, false, false, true);

    	// Verify all the queries that were run
    	assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }
    
    @Test public void testBatchedUpdatePushdown() throws Exception {
        // Create query 
		String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        
		// Create a testable prepared plan cache
		TestablePreparedPlanCache prepPlanCache = new TestablePreparedPlanCache();
		
		// Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("UPDATE pm1.g1 SET e1 = ?, e3 = ? WHERE pm1.g1.e2 = ?", new List[] {Arrays.asList(4)}); //$NON-NLS-1$
		// Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
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
    	TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, FakeMetadataFactory.example1Cached(), prepPlanCache, false, false, false);
    	Update update = (Update)dataManager.getCommandHistory().iterator().next();
    	assertTrue(((Constant)update.getChangeList().getClauses().get(0).getValue()).isMultiValued());
    }
    
}
