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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectTranslator {
	
	private static TradesCacheSource source;
	
	@BeforeClass
    public static void beforeEach() throws Exception {        
		source = TradesCacheSource.loadCache();
    }	
	
	@Test public void testQueryGetAllTrades() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade"); //$NON-NLS-1$
		
		runTest(command, 3, 4);
	}
	
	@Test public void testQueryGetAllLegs() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$
		runTest(command, 30, 3);
	}	
	
	@Test public void testQueryGetAllTransactions() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId"); //$NON-NLS-1$
		runTest(command, 150, 4);

	
	}	
	
	
	private void runTest(Select command, int rows, int columns) throws Exception {
		ObjectProjections op = new ObjectProjections(command);
		
		ObjectMethodManager omm = ObjectMethodManager.initialize(true, this.getClass().getClassLoader());

		
		List<List<Object>> results = ObjectTranslator.translateObjects(source.getAll(), op, omm);
		
		assertEquals(rows, results.size());
		
		// check the number of columns
		List<?> row1 = results.get(0);
		assertEquals(columns, row1.size());
	
	}	


  
}
