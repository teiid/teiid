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
package org.teiid.resource.adapter.infinispan.util;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.*;
import org.teiid.translator.object.testdata.Leg;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.testdata.Transaction;


/**
 * Sample cache of objects
 * 
 * @author vhalbert
 *
 */
@SuppressWarnings("nls")
public class TradesCacheSource {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -727553658250070494L;
	
	public static final String TRADES_CACHE_NAME = "Trades";
	public static final String TRADES_CACHE_KEYS = "TradeKeys";
	
	public static final String TRADE_CLASS_NAME = Trade.class.getName();
	public static final String LEG_CLASS_NAME = Leg.class.getName();
	public static final String TRANSACTION_CLASS_NAME = Transaction.class.getName();
	
	private static Map<String, Class<?>> mapOfCaches = new HashMap<String, Class<?>>(1);
	
	public static final int NUMLEGS = 10;
	public static final int NUMTRADES = 3;
	public static final int NUMTRANSACTIONS = 5;
	
	static {
		mapOfCaches.put(TradesCacheSource.TRADES_CACHE_NAME, Trade.class);
	}
	
	
	public static void loadCache( Cache<Object, Object> cache) {

			for (int i = 1; i <= NUMTRADES; i++) {
				
				List<Leg> legs = new ArrayList<Leg>();
				double d = 0;
				for (int j = 1; j <= NUMLEGS; j++) {
					
					Leg leg = new Leg(j, "LegName " + j, d * j * 3.14, new Date());
					
					List<Transaction> trans = new ArrayList<Transaction>(NUMTRANSACTIONS);
					for (int x = 1; x <= NUMTRANSACTIONS; x++) {
						Transaction t = new Transaction();
						t.setLineItem("Leg " + j + ", transaction line item " + x);
						trans.add(t);
					}
					
					leg.setTransations(trans);
					leg.setNotational(i * 7 / 3.14);
					
					legs.add(leg);
				}
				
				Trade trade = new Trade(i, "TradeName " + i, legs, new Date());
				
				// even trades set settled to true
				if ( i % 2 == 0) {
					trade.setSettled(true);
				}
				
				cache.put(String.valueOf(trade.getTradeId()), trade);

			}			
	}
}
