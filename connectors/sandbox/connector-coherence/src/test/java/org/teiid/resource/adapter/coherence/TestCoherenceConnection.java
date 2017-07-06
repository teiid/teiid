/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.resource.adapter.coherence;
        

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.teiid.language.Comparison;
import org.teiid.resource.adapter.coherence.CoherenceConnection;
import org.teiid.resource.adapter.coherence.CoherenceManagedConnectionFactory;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import junit.framework.TestCase;

public class TestCoherenceConnection extends TestCase  {
	
	public static final String CACHE_NAME = "Trades";
	
	public static final String OBJECT_TRANSLATOR = "org.teiid.resource.adapter.coherence.TestObjectTranslator";
	
	public static final int NUMLEGS = 10;
	public static final int NUMTRADES = 3;

	static {
		try {
			loadCoherence();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Load the cache with 3 trades and 10 legs for each trade.
	 * 
	 * @throws Exception
	 */
	private static void loadCoherence() throws Exception {
		NamedCache tradesCache = CacheFactory.getCache(CACHE_NAME);

		// populate the cache
		Map legsMap = new HashMap();
		Trade trade = new Trade();

		for (int i = 1; i <= NUMTRADES; i++) {

			for (int j = 1; j <= NUMLEGS; j++) {
				Leg leg = new Leg();
				leg.setId(j);
				leg.setNotional(i + j);
				legsMap.put(j, leg);
			}
			trade.setId(i);
			trade.setName("NameIs " + i);
			trade.setLegs(legsMap);
			tradesCache.put(i, trade);
		}

		System.out.println("Loaded Coherence");

	}

	/**
	 * This will instantiate the {@link CoherenceManagedConnectionFactory} and
	 * obtain a connection to
	 * 
	 * @throws Exception
	 */
	@Test
	public void testGet1Trade() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		f.setCacheName(CACHE_NAME);
		f.setCacheTranslatorClassName(OBJECT_TRANSLATOR);
		CoherenceConnection conn = (CoherenceConnection) f.createConnectionFactory().getConnection();

		List<Object> ids = new ArrayList<Object>();
		ids.add(new Long(1));
		
		Filter criteria = CoherenceFilterUtil.createInFilter("id", ids, Long.class);

		List<?> trades = conn.get(criteria);
		assertNotNull(trades);
		assertEquals("Did not get expected number of trades", 1, trades.size());

		// grab the first trade to confirm trade 1 was found in the cache.
		Trade t = (Trade) trades.get(0);
		Map legs = t.getLegs();
		assertEquals("Did not get expected number of legs", NUMLEGS, legs.size());

	}

	@Test
	public void testGetAllTrades() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		f.setCacheName(CACHE_NAME);
		f.setCacheTranslatorClassName(OBJECT_TRANSLATOR);

		CoherenceConnection conn = (CoherenceConnection) f
				.createConnectionFactory().getConnection();

		List<Object> trades = conn.get(null);
		assertNotNull(trades);
		assertEquals("Did not get expected number of trades", NUMTRADES, trades.size());


		Trade t = (Trade) trades.get(0);
		Map legs = t.getLegs();
		assertEquals("Did not get expected number of legs", NUMLEGS, legs.size());

	}
	
	@Test
	public void testLike() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		f.setCacheName(CACHE_NAME);
		f.setCacheTranslatorClassName(OBJECT_TRANSLATOR);

		CoherenceConnection conn = (CoherenceConnection) f.createConnectionFactory().getConnection();
		
		Filter criteria = CoherenceFilterUtil.createFilter("Name like 'Name%'");

		List<?> trades = conn.get(criteria);
		assertNotNull(trades);
		assertEquals("Did not get expected number of trades", 3, trades.size());


	}
	
	@Test
	public void testIn() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		f.setCacheName(CACHE_NAME);
		f.setCacheTranslatorClassName(OBJECT_TRANSLATOR);

		CoherenceConnection conn = (CoherenceConnection) f.createConnectionFactory().getConnection();
		
		// NOTE:  Coherence, because the datatype of ID is long, wants the "l" appended to the value
		Filter criteria = CoherenceFilterUtil.createFilter("Id In (1l)");

		List<?> trades = conn.get(criteria);
		assertNotNull(trades);
		assertEquals("Did not get expected number of trades", 1, trades.size());


	}	
	
	
	@Test
	public void testEqualOnTrade() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		f.setCacheName(CACHE_NAME);
		f.setCacheTranslatorClassName(OBJECT_TRANSLATOR);

		CoherenceConnection conn = (CoherenceConnection) f.createConnectionFactory().getConnection();
		
		// NOTE:  Coherence, because the datatype of ID is long, wants the "l" appended to the value
		Filter criteria = CoherenceFilterUtil.createFilter("Id = 1l");

		List<?> trades = conn.get(criteria);
		assertNotNull(trades);
		assertEquals("Did not get expected number of trades", 1, trades.size());
				
		long l = 1;
		criteria = CoherenceFilterUtil.createCompareFilter("Id",  l, Comparison.Operator.EQ, Long.class);

		trades = conn.get(criteria);
		assertNotNull(trades);
		assertEquals("Did not get expected number of trades", 1, trades.size());



	}	
	
	/**
	 * this test will not work out-of-the-box.  Coherence, from what I've found, doen'st support this, but can be developed.
	 * @throws Exception
	 */
	@Test
	public void xtestEqualOnContainerObject() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		f.setCacheName(CACHE_NAME);
		f.setCacheTranslatorClassName(OBJECT_TRANSLATOR);

		CoherenceConnection conn = (CoherenceConnection) f.createConnectionFactory().getConnection();
		long l = 1;
		// NOTE:  Coherence, because the datatype of ID is long, wants the "l" appended to the value
//		Filter criteria = CoherenceFilterUtil.createFilter("Id = 1l");
		Filter criteria = CoherenceFilterUtil.createCompareFilter("getLegs.getLegId",  l, Comparison.Operator.EQ, Long.class);


		List<?> trades = conn.get(criteria);
		assertNotNull(trades);
		assertEquals("Did not get expected number of trades", 1, trades.size());


	}
	
	@Test
	public void testAddAndRemoveTrade() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		f.setCacheName(CACHE_NAME);
		f.setCacheTranslatorClassName(OBJECT_TRANSLATOR);

		CoherenceConnection conn = (CoherenceConnection) f.createConnectionFactory().getConnection();

		Trade trade = new Trade();
		trade.setId(999);
		trade.setName("NameIs " + 999);
		trade.setLegs(null);
		conn.add(999l, trade);

		
		// NOTE:  Coherence, because the datatype of ID is long, wants the "l" appended to the value
		Filter criteria = CoherenceFilterUtil.createFilter("Id = 999l");

		List<?> trades = conn.get(criteria);
		assertNotNull(trades);
		assertEquals("Did not get expected 999 trade", 1, trades.size());
				
		long l = 999l;
		criteria = CoherenceFilterUtil.createCompareFilter("Id",  l, Comparison.Operator.EQ, Long.class);
		
		trades = conn.get(CoherenceFilterUtil.createFilter("Id = 999l"));
		assertNotNull(trades);
		assertEquals("Did not get expected 999 trade", 1, trades.size());


		conn.remove(l);
		
		trades = conn.get(CoherenceFilterUtil.createFilter("Id = 999l"));
		assertNotNull(trades);
		assertEquals("Expected no trade", 0, trades.size());


	}	
}


