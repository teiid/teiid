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
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.unittest.FakeMetadataFactory;

@SuppressWarnings("nls")
public class TestCallableStatement {
	
	@Test public void testMissingInput() throws Exception {
		String sql = "{? = call pm4.spTest9()}"; //$NON-NLS-1$

		try {
			TestPreparedStatement.helpTestProcessing(sql, Collections.EMPTY_LIST, null, new HardcodedDataManager(), FakeMetadataFactory.exampleBQTCached(), true, FakeMetadataFactory.exampleBQTVDB());
			fail();
		} catch (QueryResolverException e) {
			assertEquals("Required parameter 'pm4.spTest9.inkey' has no value was set or is an invalid parameter.", e.getMessage()); //$NON-NLS-1$
		}
	}
	
	@Test public void testReturnParameter() throws Exception {
		String sql = "{? = call pm4.spTest9(?)}"; //$NON-NLS-1$

		List values = new ArrayList();
		values.add(1);
		
		List[] expected = new List[1];
		expected[0] = Arrays.asList(1);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("? = EXEC pm4.spTest9(1)", expected);
		
		TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, FakeMetadataFactory.exampleBQTCached(), true, FakeMetadataFactory.exampleBQTVDB());
	}
	
	/**
	 * same result as above, but the return parameter is not specified
	 * TODO: it would be best if the return parameter were not actually returned here, since it wasn't specified in the initial sql
	 */
	@Test public void testNoReturnParameter() throws Exception {
		String sql = "{call pm4.spTest9(?)}"; //$NON-NLS-1$

		List values = new ArrayList();
		values.add(1);
		
		List[] expected = new List[1];
		expected[0] = Arrays.asList(1);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm4.spTest9(1)", expected);
		
		TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, FakeMetadataFactory.exampleBQTCached(), true, FakeMetadataFactory.exampleBQTVDB());
	}
		
	@Test public void testOutParameter() throws Exception {
		String sql = "{call pm2.spTest8(?, ?)}"; //$NON-NLS-1$

		List values = new ArrayList();
		values.add(2);
		
		List[] expected = new List[1];
		expected[0] = Arrays.asList(null, null, 1);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm2.spTest8(2)", expected);
		
		TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, FakeMetadataFactory.exampleBQTCached(), true, FakeMetadataFactory.exampleBQTVDB());
	}
	
	@Test(expected=QueryResolverException.class) public void testInvalidReturn() throws Exception {
		String sql = "{? = call pm2.spTest8(?, ?)}"; //$NON-NLS-1$

		List values = Arrays.asList(2);
		
		List[] expected = new List[0];
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, FakeMetadataFactory.exampleBQTCached(), true, FakeMetadataFactory.exampleBQTVDB());
	}
	
	@Test public void testInputExpression() throws Exception {
		String sql = "{call pm2.spTest8(1, ?)}"; //$NON-NLS-1$

		List[] expected = new List[1];
		expected[0] = Arrays.asList(null, null, 0);
		
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm2.spTest8(1)", expected);
		
		TestPreparedStatement.helpTestProcessing(sql, null, expected, dataManager, FakeMetadataFactory.exampleBQTCached(), true, FakeMetadataFactory.exampleBQTVDB());
	}

}
