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
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestCallableStatement {
	
	@Test public void testMissingInput() throws Exception {
		String sql = "{? = call pm4.spTest9()}"; //$NON-NLS-1$

		try {
			TestPreparedStatement.helpTestProcessing(sql, Collections.EMPTY_LIST, null, new HardcodedDataManager(), RealMetadataFactory.exampleBQTCached(), true, RealMetadataFactory.exampleBQTVDB());
			fail();
		} catch (QueryResolverException e) {
			assertEquals("TEIID30089 Required parameter 'pm4.spTest9.inkey' has no value was set or is an invalid parameter.", e.getMessage()); //$NON-NLS-1$
		}
	}
	
	@Test public void testProcedurePlanCaching() throws Exception {
		String sql = "{? = call BQT_V.v_spTest9(?)}"; //$NON-NLS-1$

		List values = new ArrayList();
		values.add(1);
		
		List[] expected = new List[1];
		expected[0] = Arrays.asList(1);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("ret = EXEC pm4.spTest9(1)", expected);
		
		TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, RealMetadataFactory.exampleBQTCached(), true, RealMetadataFactory.exampleBQTVDB());
	}
	
	@Test public void testReturnParameter() throws Exception {
		String sql = "{? = call pm4.spTest9(?)}"; //$NON-NLS-1$

		List values = new ArrayList();
		values.add(1);
		
		List[] expected = new List[1];
		expected[0] = Arrays.asList(1);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("? = EXEC pm4.spTest9(1)", expected);
		
        helpProcess(sql, values, expected, dataManager);
	}

	/**
	 * help process a physical callable statement
	 */
	private void helpProcess(String sql, List values, List[] expected,
			HardcodedDataManager dataManager) throws TeiidComponentException,
			TeiidProcessingException, Exception {
		SessionAwareCache<PreparedPlan> planCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0); //$NON-NLS-1$
		PreparedStatementRequest plan = TestPreparedStatement.helpGetProcessorPlan(sql, values, new DefaultCapabilitiesFinder(), RealMetadataFactory.exampleBQTCached(), planCache, 1, true, false, RealMetadataFactory.exampleBQTVDB());
        TestProcessor.doProcess(plan.processPlan, dataManager, expected, plan.context);
        
        TestPreparedStatement.helpGetProcessorPlan(sql, values, new DefaultCapabilitiesFinder(), RealMetadataFactory.exampleBQTCached(), planCache, 1, true, false, RealMetadataFactory.exampleBQTVDB());
        assertEquals(0, planCache.getCacheHitCount());
	}
	
	@Test public void testNoReturnParameter() throws Exception {
		String sql = "{call pm4.spTest9(?)}"; //$NON-NLS-1$

		List values = new ArrayList();
		values.add(1);
		
		List[] expected = new List[1];
		expected[0] = Arrays.asList(1);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm4.spTest9(1)", expected);
		
		helpProcess(sql, values, expected, dataManager);
	}
		
	@Test public void testOutParameter() throws Exception {
		String sql = "{call pm2.spTest8(?, ?)}"; //$NON-NLS-1$

		List values = new ArrayList();
		values.add(2);
		
		List[] expected = new List[1];
		expected[0] = Arrays.asList(null, null, 1);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm2.spTest8(2)", expected);
		
		helpProcess(sql, values, expected, dataManager);
	}
	
	@Test(expected=QueryResolverException.class) public void testInvalidReturn() throws Exception {
		String sql = "{? = call pm2.spTest8(?, ?)}"; //$NON-NLS-1$

		List values = Arrays.asList(2);
		
		List[] expected = new List[0];
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, RealMetadataFactory.exampleBQTCached(), true, RealMetadataFactory.exampleBQTVDB());
	}
	
	@Test public void testInputExpression() throws Exception {
		String sql = "{call pm2.spTest8(1, ?)}"; //$NON-NLS-1$

		List[] expected = new List[1];
		expected[0] = Arrays.asList(null, null, 0);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm2.spTest8(1)", expected);
		
		helpProcess(sql, null, expected, dataManager);
	}

}
