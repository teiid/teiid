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
package org.teiid.translator.object.infinispan;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.BaseObjectTest;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.SearchCriterion;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;


@SuppressWarnings("nls")
public class TestInfinispanIntegration extends BaseObjectTest {
	
	private static ExecutionContext context;
	
	
	@BeforeClass
    public static void beforeEach() throws Exception {  
		//print = true;
		context = Mockito.mock(ExecutionContext.class);
    }
	
	@Test public void testQueryIncludeLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$
		
		InfinispanCacheConnection connection = new InfinispanCacheConnection() {

			@Override
			public List<Object> get(SearchCriterion criterion, String cacheName, Class<?> rootNodeType) 
					throws TranslatorException {

				return TradesCacheSource.loadCache().getAll();
			}
			
		};

		InfinispanRemoteExecutionFactory factory = new InfinispanRemoteExecutionFactory();	
		factory.setCacheName("Trades");
		factory.setClassNamesOfCachedObjects(TradesCacheSource.TRADE_CLASS_NAME);
		factory.start();
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, connection);
		
		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
		
		
		while (row != null) {
			rows.add(row);
			++cnt;
			row = exec.next();
			
		}
		

		assertEquals("Did not get expected number of rows", 30, cnt); //$NON-NLS-1$
		
		this.compareResultSet(rows);
		     
		exec.close();
		
	}	
	
	@Test public void testQueryIncludeLegsWithFilter() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId and L.Name = 'LegName 1'"); //$NON-NLS-1$
				
		InfinispanCacheConnection connection = new InfinispanCacheConnection() {

			@Override
			public List<Object> get(SearchCriterion criterion, String cacheName, Class<?> rootNodeType) 
					throws TranslatorException {

				return TradesCacheSource.loadCache().getAll();
			}
			
		};
		InfinispanRemoteExecutionFactory factory = new InfinispanRemoteExecutionFactory();	
		factory.setCacheName("Trades");
		factory.setClassNamesOfCachedObjects(TradesCacheSource.TRADE_CLASS_NAME);
		factory.setSupportFilters(true);
		factory.start();
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, connection);
		
		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
		
		
		while (row != null) {
			rows.add(row);
			++cnt;
			row = exec.next();
		}
		

		assertEquals("Did not get expected number of rows", 3, cnt); //$NON-NLS-1$
		
		this.compareResultSet(rows);
		     
		exec.close();
		
	}		
	
	@Test public void testQueryIncludeTradeAndLegsWithFilter() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId and T.TradeId = '1' and  L.Name = 'LegName 1'"); //$NON-NLS-1$
				
		InfinispanCacheConnection connection = new InfinispanCacheConnection() {

			@Override
			public List<Object> get(SearchCriterion criterion, String cacheName, Class<?> rootNodeType) 
					throws TranslatorException {

				return TradesCacheSource.loadCache().get(1);
			}
			
		};
		InfinispanRemoteExecutionFactory factory = new InfinispanRemoteExecutionFactory();	
		factory.setCacheName("Trades");
		factory.setClassNamesOfCachedObjects(TradesCacheSource.TRADE_CLASS_NAME);
		factory.setSupportFilters(true);
		factory.start();
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, connection);
		
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
		

		assertEquals("Did not get expected number of rows", 1, cnt); //$NON-NLS-1$
		
		this.compareResultSet(rows);
		     
		exec.close();
		
	}		
	
	// when using key, no filter is applied
	@Test public void testQueryIncludeTradeID() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName From Trade_Object.Trade as T WHERE T.TradeId = 1"); //$NON-NLS-1$
				
		InfinispanCacheConnection connection = new InfinispanCacheConnection() {

			@Override
			public List<Object> get(SearchCriterion criterion, String cacheName, Class<?> rootNodeType) 
					throws TranslatorException {

				return TradesCacheSource.loadCache().getAll();
			}
			
		};
		InfinispanRemoteExecutionFactory factory = new InfinispanRemoteExecutionFactory();	
		factory.setCacheName("Trades");
		factory.setClassNamesOfCachedObjects(TradesCacheSource.TRADE_CLASS_NAME);
		factory.setSupportFilters(true);
		factory.start();
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, connection);
		
		exec.execute();
		
		List<Object> rows = new ArrayList<Object>();
		
		int cnt = 0;
		List<Object> row = exec.next();
		
		
		while (row != null) {
			rows.add(row);
			++cnt;
			row = exec.next();
		}
		

		assertEquals("Did not get expected number of rows", 3, cnt); //$NON-NLS-1$
		
		this.compareResultSet(rows);
		     
		exec.close();
		
	}			
  
}
