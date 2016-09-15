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
package org.teiid.resource.adapter.infinispan;

import java.util.Map;

import static org.mockito.Mockito.mock;

import org.infinispan.manager.DefaultCacheManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.resource.adapter.infinispan.InfinispanManagedConnectionFactory;
import org.teiid.resource.adapter.infinispan.InfinispanTestHelper;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.infinispan.libmode.BasicAnnotatedSearchTest;
import org.teiid.translator.infinispan.libmode.InfinispanCacheExecutionFactory;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.annotated.Trade;
import org.teiid.translator.object.testdata.annotated.TradesAnnotatedCacheSource;
import org.teiid.translator.object.testdata.trades.VDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanJndiDSLSearch extends BasicAnnotatedSearchTest {	
	
	private static int SELECT_STAR_COL_COUNT = 5;
	
    private static InfinispanManagedConnectionFactory factory = null;
	private static ExecutionContext context;
	private static ObjectConnection CONNECTION;
	
	private static InfinispanCacheExecutionFactory TRANS_FACTORY = null;
	
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
	    
		context = mock(ExecutionContext.class);
		
		final DefaultCacheManager container = new DefaultCacheManager("./src/test/resources/infinispan_persistent_config.xml", true);
		TradesAnnotatedCacheSource.loadCache(  container.getCache(InfinispanTestHelper.TRADE_CACHE_NAME ));

		factory = new InfinispanManagedConnectionFactory() {
			
			/**
			 */
			private static final long serialVersionUID = 6241061876834919893L;

			@Override
			protected Object performJNDICacheLookup(String jnidName) throws Exception {
				return container;
			}

		};

		factory.setCacheJndiName("TradeJNDI");
		factory.setCacheTypeMap(InfinispanTestHelper.TRADE_CACHE_NAME + ":" + Trade.class.getName() + ";longValue:long");
//		factory.setCacheTypeMap(InfinispanTestHelper.TRADE_CACHE_NAME + ":" + "org.teiid.translator.object.testdata.trades.Trade;longValue:long");

		CONNECTION = factory.createConnectionFactory().getConnection();
		

		TRANS_FACTORY = new InfinispanCacheExecutionFactory();
		TRANS_FACTORY.start();
		

	}	
	
	@AfterClass
    public static void closeConnection() throws Exception {

	    CONNECTION.cleanUp();
	    factory.shutDownCache();
	}
    
	@Override
	protected ObjectExecution createExecution(Select command) throws Exception {
		return (ObjectExecution) TRANS_FACTORY.createExecution(command, context, VDBUtility.RUNTIME_METADATA, CONNECTION);
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
