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

import java.util.Arrays;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;

import junit.framework.TestCase;


/**
 * <p><code>TestCase</code> to cover processing of JOINs which use a scalar 
 * function as a symbol or as part of the JOIN criteria.</p>
 * 
 * <p>All tests should verify and validate that the scalar function's result 
 * is being used appropriately from the pre-JOIN and post-JOIN aspect.  Most 
 * specifically, the results returned from the JOIN should match the expected 
 * results defined in each test method.</p>
 * @since 6.0
 */
public class TestJoinWithFunction extends TestCase {

	/**
	 * <p>Test the use of a non-deterministic function on a user command that
	 * performs a JOIN of two sources.</p>
	 * 
	 * <p>The function should be executed on the result returned from the JOIN and
	 * is expected to be executed for each row of the final result set.</p>
	 * @throws TeiidComponentException 
	 * @throws QueryMetadataException 
	 */
	public void testNonDeterministicPostJoin() throws Exception {
		// source query for one side of a JOIN
		String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 " //$NON-NLS-1$
				+ "FROM pm1.g1"; //$NON-NLS-1$
		// source query for other side of a JOIN
		String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
				+ "FROM pm2.g2"; //$NON-NLS-1$

		// User Command
		/*
		 * Return everything from the JOIN. RandomTop is the use of RAND() on
		 * the user command and should result in unique random numbers for each 
		 * row in the JOINed output.
		 */
		String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, RAND() AS RandomTop " + //$NON-NLS-1$
				"FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
				"("	+ rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
				"WHERE l.ID = r.ID"; //$NON-NLS-1$

		/*
		 * Populate a List with our expected results. We can predict the return value
		 * for RAND() because the TestProcessor.helpProcess() method seeds the random 
		 * number generator. 
		 */
		List<?>[] expected = new List[] {
				Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(0.24053641567148587) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(0.0), new Double(0.6374174253501083) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(0.5504370051176339) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
						new Boolean(false), null, new Double(0.5975452777972018) }), };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
	}

	/**
	 * <p>Test the use of a non-deterministic function on a source command of a JOIN
	 * defined by a user command that performs a JOIN of two sources.</p>
	 * 
	 * <p>The function should be executed on the result that will be used for one side
	 * of the JOIN.  The function should only be executed for each row of the the result
	 * set returned from the left-side of the JOIN which should result in the same return 
	 * value for multiple rows of the final result set after the JOIN is completed.  For 
	 * example, if the left-side query is expected to return one row and the right-side 
	 * query will return three rows which match the JOIN criteria for the one row on the 
	 * left-side then the expected result should be that the function be executed once to 
	 * represent the one row from the left-side and the function's return value will be 
	 * repeated for each of the three post-JOINed results.</p>
	 * @throws TeiidComponentException 
	 * @throws QueryMetadataException 
	 */
	public void testNonDeterministicPreJoin() throws Exception {
		// source query for one side of a JOIN
		String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, RAND() AS RandomLeft " //$NON-NLS-1$
				+ "FROM pm1.g1"; //$NON-NLS-1$
		// source query for other side of a JOIN
		String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
				+ "FROM pm2.g2"; //$NON-NLS-1$

		// User Command
		/*
		 * Return everything from the JOIN. TopRandom is the use of RAND() on
		 * the user command while RandomLeft is the use of RAND() within a
		 * source node.
		 */
		String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.RandomLeft " + //$NON-NLS-1$
				"FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
				"(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
				"WHERE l.ID = r.ID"; //$NON-NLS-1$

		/*
		 * Populate a List with our expected results. We can predict the return value
		 * for RAND() because the TestProcessor.helpProcess() method seeds the random 
		 * number generator. 
		 */
		List<?>[] expected = new List[] {
				Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(0.24053641567148587) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(0.0), new Double(0.6374174253501083) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(0.6374174253501083) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
						new Boolean(false), null, new Double(0.6374174253501083) }), 
		};

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
	}

	/**
	 * <p>Test the use of a non-deterministic function on the sub-command of a user
	 * command and the user command itself, which performs a JOIN of two sources.</p>
	 * 
	 * <p>This test combines the PostJoin and PreJoin test cases.</p>
	 * @see #testNonDeterministicPostJoin
	 * @see #testNonDeterministicPreJoin
	 * @throws TeiidComponentException 
	 * @throws QueryMetadataException 
	 */
	public void testNonDeterministicPrePostJoin() throws Exception {
		// source query for one side of a JOIN
		String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, RAND() AS RandomLeft " //$NON-NLS-1$
				+ "FROM pm1.g1"; //$NON-NLS-1$
		// source query for other side of a JOIN
		String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
				+ "FROM pm2.g2"; //$NON-NLS-1$

		// User Command
		/*
		 * Return everything from the JOIN. TopRandom is the use of RAND() on
		 * the user command while RandomLeft is the use of RAND() within a
		 * source node.
		 */
		String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.RandomLeft, RAND() AS RandomTop " + //$NON-NLS-1$
				"FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
				"(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
				"WHERE l.ID = r.ID"; //$NON-NLS-1$
		
		/*
		 * Populate a List with our expected results. We can predict the return value
		 * for RAND() because the TestProcessor.helpProcess() method seeds the random 
		 * number generator. 
		 */
		List<?>[] expected = new List[] {
			Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
					new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
					new Boolean(true), new Double(2.0), new Double(0.24053641567148587), new Double(0.5975452777972018) }),
			Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
					new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
					new Boolean(false), new Double(0.0), new Double(0.6374174253501083), new Double(0.3332183994766498) }),
			Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
					new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
					new Boolean(true), new Double(2.0), new Double(0.6374174253501083), new Double(0.3851891847407185) }),
			Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
					new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
					new Boolean(false), null, new Double(0.6374174253501083), new Double(0.984841540199809) })
		};

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
	}

	/**
	 * <p>Test the use of a deterministic function on the user command which
	 * performs a JOIN of two sources.</p>
	 * 
	 * <p>The function should be executed prior to the JOIN being executed and should 
	 * result in the projected symbol becoming a constant.</p>
	 * @throws TeiidComponentException
	 * @throws QueryMetadataException
	 */
	public void testDeterministicPostJoin() throws Exception {
		// source query for one side of a JOIN
		String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 " //$NON-NLS-1$
				+ "FROM pm1.g1"; //$NON-NLS-1$
		// source query for other side of a JOIN
		String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
				+ "FROM pm2.g2"; //$NON-NLS-1$

		// User Command
		/*
		 * Return everything from the JOIN. SqrtTop is the use of SQRT(100) on
		 * the user command and should result in 10 for each row in the JOINed 
		 * output.
		 */
		String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, SQRT(100) AS SqrtTop " + //$NON-NLS-1$
				"FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
				"(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
				"WHERE l.ID = r.ID"; //$NON-NLS-1$

		/*
		 * Populate a List with our expected results.  
		 */
		List<?>[] expected = new List[] {
				Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(10) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(0.0), new Double(10) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(10) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
						new Boolean(false), null, new Double(10) }), 
		};

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
	}

	/**
	 * <p>The function should be executed prior to the JOIN being executed and should 
	 * result in the projected symbol becoming a constant.</p>

	 * <p>Test the use of a deterministic function on the source command of a JOIN
	 * defined by a user command which performs a JOIN of two sources.</p>
	 * 
	 * <p>The function should be executed prior to the commands from either side of the
	 * JOIN being executed and merged into user command prior to the JOIN actually being 
	 * executed.</p>
	 * @throws TeiidComponentException 
	 * @throws QueryMetadataException 
	 */
	public void testDeterministicPreJoin() throws Exception {
		// source query for one side of a JOIN
		String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, SQRT(100) AS SqrtLeft " //$NON-NLS-1$
				+ "FROM pm1.g1"; //$NON-NLS-1$
		// source query for other side of a JOIN
		String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
				+ "FROM pm2.g2"; //$NON-NLS-1$

		// User Command
		/*
		 * Return everything from the JOIN. SqrtLeft is the use of SQRT() 
		 * within a source node.
		 */
		String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.SqrtLeft " + //$NON-NLS-1$
				"FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
				"(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
				"WHERE l.ID = r.ID"; //$NON-NLS-1$

		/*
		 * Populate a List with our expected results. 
		 */
		List<?>[] expected = new List[] {
				Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(10) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(0.0), new Double(10) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(10) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
						new Boolean(false), null, new Double(10) }), 
		};

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
	}

	/**
	 * <p>Test the use of a deterministic function on the sub-command of a user
	 * command and the user command itself, which performs a JOIN of two sources.</p>
	 * 
	 * <p>This test combines the PostJoin and PreJoin test cases.</p>
	 * @throws TeiidComponentException 
	 * @throws QueryMetadataException 
	 * @see #testDeterministicPostJoin
	 * @see #testDeterministicPreJoin
	 */
	public void testDeterministicPrePostJoin() throws Exception {
		// sub-query for one side of a JOIN
		String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, SQRT(100) AS SqrtLeft " //$NON-NLS-1$
				+ "FROM pm1.g1"; //$NON-NLS-1$
		// sub-query for other side of a JOIN
		String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
				+ "FROM pm2.g2"; //$NON-NLS-1$

		// User Command
		/*
		 * Return everything from the JOIN. SqrtTop is the use of SQRT(100) on
		 * the user command while SqrtLeft is the use of SQRT() within a
		 * source node.
		 */
		String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.SqrtLeft, SQRT(100) AS SqrtTop " + //$NON-NLS-1$
				"FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
				"(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
				"WHERE l.ID = r.ID"; //$NON-NLS-1$

		/*
		 * Populate a List with our expected results. 
		 */
		List<?>[] expected = new List[] {
				Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(10.0), new Double(10.0) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
						new Boolean(false), new Double(0.0), new Double(10.0), new Double(10.0) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
						new Boolean(true), new Double(2.0), new Double(10.0), new Double(10.0) }),
				Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
						new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
						new Boolean(false), null, new Double(10.0), new Double(10.0) }), 
		};

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
	}
}
