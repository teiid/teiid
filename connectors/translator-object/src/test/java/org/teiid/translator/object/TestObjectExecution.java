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
import org.mockito.Mockito;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectExecution {
	
	private static TradesCacheSource source;
	private static ObjectExecutionFactory factory;
	private static ExecutionContext context;

	
	@BeforeClass
    public static void beforeEach() throws Exception {        
		source = TradesCacheSource.loadCache();
		
		context = Mockito.mock(ExecutionContext.class);

		factory = new ObjectExecutionFactory() {

			@Override
			protected ObjectSourceProxy createProxy(ObjectCacheConnection connection)
					throws TranslatorException {

				return new ObjectSourceProxy() {

					@Override
					public List<Object> get(Command command, String cacheName, String className) throws TranslatorException {
						return source.getAll();
					}

					@Override
					public void close() {
						
					}
					
				};
			}
			
		};
		
		factory.start();
		

    }
	

	@Test public void testQueryRootObject() throws Exception {
		execute( createExecution("select * From Trade_Object.Trade"), 3, 4);
	}
	
	@Test public void testQueryIncludeLegs() throws Exception {		
		execute( createExecution("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"), 
				30, 3);

	}	
	
	@Test public void testQueryGetAllTransactions() throws Exception {
		execute( createExecution("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId"),150, 4);
	}		
	

	@Test	public void testAtomicSelects() throws Exception {

		Thread[] threads = new Thread[20];
		for (int i = 0; i < threads.length; i++) {
		    threads[i] = new Thread() {
				public void run() {
					for (int i=0; i<1000; i++) {
						test();
					}
				}
				public void test() {
					ObjectExecution exec = null;
					try {
						exec =  createExecution("select * From Trade_Object.Trade");
						execute(exec, 3, 4);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (exec != null) exec.close();
					}
				}
		    };
  	
		    threads[i].start();
		}
		for (int i = 0; i < threads.length; i++) {
		    try {
		       threads[i].join();
		    } catch (InterruptedException ignore) {}
		}
	}
	
	private ObjectExecution createExecution(String sql) throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand(sql); //$NON-NLS-1$
				
		ObjectExecution exec = (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, null);
		
		return exec;

	}
	
	private void execute(ObjectExecution exec, int expected, int columns) throws Exception {
		
		exec.execute();
		
		int cnt = 0;
		List<?> row = exec.next();
		
		// check the number of columns
		assertEquals("Number of columns is incorrect", columns, row.size());

		
		while (row != null) {
			++cnt;
			row = exec.next();
		}
		assertEquals("Did not get expected number of rows", expected, cnt); //$NON-NLS-1$
		
		     
		exec.close();
	}
	
}
