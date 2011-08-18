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

package org.teiid.coherence.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.coherence.translator.Leg;
import org.teiid.coherence.translator.Trade;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

public class TestCoherenceConnection {

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
		NamedCache tradesCache = CacheFactory.getCache("Trades");

		// populate the cache
		Map legsMap = new HashMap();
		Trade trade = new Trade();

		for (int i = 1; i <= 3; i++) {

			for (int j = 1; j <= 10; j++) {
				Leg leg = new Leg();
				leg.setId(j);
				leg.setNotional(i + j);
				legsMap.put(j, leg);
			}
			trade.setId(i);
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
		CoherenceConnection conn = (CoherenceConnection) f.createConnectionFactory().getConnection();

		List<Long> ids = new ArrayList<Long>();
		ids.add(new Long(1));
		// List<Trade> trades = conn.getTrades(ids);

		List<?> trades = conn.get("Id = " + 1 + "l");
		if (trades == null || trades.size() == 0) {
			throw new Exception("get1Trade: No trade found for 1");
		} else {
			System.out
					.println("get1Trade: # of Trades found: " + trades.size());
		}

		// grab the first trade to confirm trade 1 was found in the cache.
		Trade t = (Trade) trades.get(0);
		Map legs = t.getLegs();
		System.out.println("Num of legs are: " + legs.size());

	}

	@Test
	public void testGetAllTrades() throws Exception {

		CoherenceManagedConnectionFactory f = new CoherenceManagedConnectionFactory();
		CoherenceConnection conn = (CoherenceConnection) f
				.createConnectionFactory().getConnection();

		List<Object> trades = conn.get(null);
		if (trades == null || trades.size() == 0) {
			throw new Exception("getAllTrades: No trades found for 1");
		} else {
			System.out.println("getAllTrades: # of Trades found: "
					+ trades.size());
		}

		Trade t = (Trade) trades.get(0);
		Map legs = t.getLegs();
		System.out.println("Num of legs are: " + legs.size());

	}
}
