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
package org.teiid.translator.infinispan.cache;

import static org.mockito.Mockito.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.trades.TradesCacheSource;
import org.teiid.translator.object.testdata.trades.VDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanJndiDSLSearch extends BasicAnnotatedSearchTest {
	
	private static int SELECT_STAR_COL_COUNT = TradesCacheSource.NUM_OF_ALL_COLUMNS;
	
	private static ObjectConnection CONNECTION;
	private static ExecutionContext context;
    private static InfinispanCacheExecutionFactory factory;
	
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
	    
		context = mock(ExecutionContext.class);
		
		
		CONNECTION = TestInfinispanConnection.createConnection(false);

		factory = new InfinispanCacheExecutionFactory();
		factory.setSupportsDSLSearching(true);

		factory.start();

	}	
	
	@AfterClass
    public static void closeConnection() throws Exception {

	    CONNECTION.cleanUp();

    }
    
	@Override
	protected ObjectExecution createExecution(Select command) throws Exception {
		return (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, CONNECTION);
	}	
	
	@Test public void testQueryLikeCriteria1() throws Exception {	
	Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeName like 'TradeName%'"); //$NON-NLS-1$
					
		performTest(command, 200, SELECT_STAR_COL_COUNT);
	}	
	
	@Test public void testQueryLikeCriteria2() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeName like 'TradeName 2%'"); //$NON-NLS-1$
					
		performTest(command, 12, SELECT_STAR_COL_COUNT);
	}	

	@Test public void testQueryNotLikeCriteria() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeName not like 'TradeName 2%'"); //$NON-NLS-1$
					
		performTest(command, 188, SELECT_STAR_COL_COUNT);
	}	
	
	@Test public void testQueryGetLTE() throws Exception {						
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select tradeId, tradeName From Trade_Object.Trade as T where tradeId <= 99"); //$NON-NLS-1$
		
	
		performTest(command, 99, 2);
	}	

}
