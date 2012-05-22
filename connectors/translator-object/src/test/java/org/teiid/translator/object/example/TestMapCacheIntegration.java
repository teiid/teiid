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
package org.teiid.translator.object.example;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.object.BaseObjectTest;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;


@SuppressWarnings("nls")
public class TestMapCacheIntegration extends BaseObjectTest {

	
	protected static boolean print = false;
	
	@Test public void testQueryIncludeLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$

		ExecutionContext context = Mockito.mock(ExecutionContext.class);

		MapCacheExecutionFactory factory = new MapCacheExecutionFactory();	
		factory.setCacheLoaderClassName("org.teiid.translator.object.util.TradesCacheSource");
		factory.start();
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, null);
		
		exec.execute();
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
		
		while (row != null) {
			rows.add(row);
			++cnt;
			row = exec.next();
			printRow(cnt, row);
		}	

		assertEquals("Did not get expected number of rows", TradesCacheSource.NUMTRADES * TradesCacheSource.NUMLEGS, cnt); //$NON-NLS-1$
		
		this.compareResultSet(rows);

		exec.close();
		
	}	
	
	@Test public void testQueryGetTrades() throws Exception {		

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade"); //$NON-NLS-1$

		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		
		MapCacheExecutionFactory factory = new MapCacheExecutionFactory();	
		factory.setCacheLoaderClassName("org.teiid.translator.object.util.TradesCacheSource");
		factory.start();
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, null);
		
		exec.execute();
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
		
		while (row != null) {
			rows.add(row);
			++cnt;
			row = exec.next();
			printRow(cnt, row);
		}
		
		assertEquals("Did not get expected number of rows", TradesCacheSource.NUMTRADES, cnt); //$NON-NLS-1$
		
		this.compareResultSet(rows);
		     
		exec.close();
		
	}		
	
	@Test public void testQueryGetTransaction() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId"); //$NON-NLS-1$

		ExecutionContext context = Mockito.mock(ExecutionContext.class);

		MapCacheExecutionFactory factory = new MapCacheExecutionFactory();	
		factory.setCacheLoaderClassName("org.teiid.translator.object.util.TradesCacheSource");
		factory.start();
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, null);
		
		exec.execute();
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
		
		while (row != null) {
			rows.add(row);
			++cnt;
			row = exec.next();
			printRow(cnt, row);
		}
		
		int total = TradesCacheSource.NUMTRADES * TradesCacheSource.NUMLEGS * TradesCacheSource.NUMTRANSACTIONS;
		assertEquals("Did not get expected number of rows", total, cnt); //$NON-NLS-1$
		
		this.compareResultSet(rows);
		     
		exec.close();
		
	}		
	
  
}
