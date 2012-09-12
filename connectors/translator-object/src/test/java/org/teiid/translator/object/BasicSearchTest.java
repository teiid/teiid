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
package org.teiid.translator.object;

import java.util.List;

import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;

/**
 * BasicSearchTest represent a common core set of test that are run for all the configuration/search combinations.  
 * @author vhalbert
 *
 */

@SuppressWarnings("nls")
public abstract class BasicSearchTest extends BaseObjectTest {
	
	@Test public void testQueryGetAllTrades() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade as T"); //$NON-NLS-1$		
		
		List<Object> rows = performTest(command, TradesCacheSource.NUMTRADES);
		
		compareResultSet(rows);
		
	}		
	
	@Test public void testQueryGetAllTradesAndLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$		
		
		List<Object> rows = performTest(command, TradesCacheSource.NUMTRADES);
		
		compareResultSet(rows);
		
	}		
	
	@Test public void testQueryGetAllTradesLegsAndTransactions() throws Exception {		

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId "); //$NON-NLS-1$
		
		List<Object> rows = performTest(command, TradesCacheSource.NUMTRADES);
		
		compareResultSet(rows);
	
	}		

	
	@Test public void testQueryGetOneTrade() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName From Trade_Object.Trade as T WHERE T.TradeId = '1'"); //$NON-NLS-1$
					
		List<Object> rows = performTest(command, 1);
		
		compareResultSet(rows);
		
	}	
	
	@Test public void testQueryGetOneTradeAndLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId and T.TradeId = '1'"); //$NON-NLS-1$		
		
		List<Object> rows = performTest(command, 1);
		
		compareResultSet(rows);
		
	}	
	
	
	@Test public void testQueryInCriteria() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName From Trade_Object.Trade as T WHERE T.TradeId in ('1', '3')"); //$NON-NLS-1$
					
		List<Object> rows = performTest(command, 2);
		
		compareResultSet(rows);
		
	}	
	
	protected abstract List<Object> performTest(Select command, int rowcnt) throws Exception;

  
}
