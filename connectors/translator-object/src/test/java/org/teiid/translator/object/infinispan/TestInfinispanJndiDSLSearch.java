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

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Properties;

import org.infinispan.manager.DefaultCacheManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.Select;
import org.teiid.resource.adapter.infinispan.base.AbstractInfinispanManagedConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.object.BasicSearchTest;
import org.teiid.translator.object.CacheContainerWrapper;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.testdata.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanJndiDSLSearch extends BasicSearchTest {
	protected static final String JNDI_NAME = "java/MyCacheManager";


	private static AbstractInfinispanManagedConnectionFactory afactory;
	private static ExecutionContext context;
    private static InfinispanExecutionFactory factory;
	
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
	    
		context = mock(ExecutionContext.class);
        
		final DefaultCacheManager container = TestInfinispanConnection.createContainer();
				//new DefaultCacheManager("./src/test/resources/infinispan_persistent_config.xml", true);
        
        TradesCacheSource.loadCache(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));
  
        afactory = new AbstractInfinispanManagedConnectionFactory() {
			/**
			 */
			private static final long serialVersionUID = 1L;

			@Override
			protected Object performJNDICacheLookup(String jndiName) throws Exception {
				return container;
			}

			@Override
			protected CacheContainerWrapper createRemoteCache(Properties props,
					ClassLoader classLoader) {
				return null;
			}
		};
		
		afactory.setCacheJndiName(JNDI_NAME);
		afactory.setCacheTypeMap(TradesCacheSource.TRADES_CACHE_NAME + ":" + Trade.class.getName());

		factory = new InfinispanExecutionFactory();
		factory.setSupportsDSLSearching(true);

		factory.start();

	}	
	
    @SuppressWarnings("unchecked")
	@AfterClass
    public static void closeConnection() throws Exception {
   	    CacheContainerWrapper ccw = afactory.getCacheContainer();
	    
	    ReflectionHelper h = new ReflectionHelper(ccw.getClass());
	    
	    (h.findBestMethodWithSignature("cleanUp", Collections.EMPTY_LIST)).invoke(ccw);

      	afactory.cleanUp();

    }
    
	@Override
	protected ObjectExecution createExecution(Select command) throws Exception {
		return (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, afactory.createConnectionFactory().getConnection());
	}	
	
	@Test public void testQueryLikeCriteria1() throws Exception {	
	Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeName like 'TradeName%'"); //$NON-NLS-1$
					
		performTest(command, 3, 4);
	}	
	
	@Test public void testQueryLikeCriteria2() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeName like 'TradeName 2%'"); //$NON-NLS-1$
					
		performTest(command, 1, 4);
	}	
	
	@Test public void testQueryCompareEQBoolean() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  Settled = 'false'"); //$NON-NLS-1$
					
		performTest(command, 2, 4);
	}	
	
	@Test public void testQueryCompareNEBoolean() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  Settled <> 'false'"); //$NON-NLS-1$
					
		performTest(command, 1, 4);
	}		
	
	@Test public void testQueryRangeBetween() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select tradeName, tradeId From Trade_Object.Trade  where  TradeId > '1' and TradeId < '3'"); //$NON-NLS-1$
					
		performTest(command, 1, 2);
	}

	@Test public void testQueryRangeAbove() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeId > '1'"); //$NON-NLS-1$
					
		performTest(command, 2, 4);
	}
	
	@Test public void testQueryRangeAbove2() throws Exception {     
	    Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeId > 1"); //$NON-NLS-1$
	                                       
	    performTest(command, 2, 4);
	}

	@Test public void testQueryRangeBelow() throws Exception {     
	    Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeId < 3"); //$NON-NLS-1$
	                                       
	    performTest(command, 2, 4);
	}
	
	@Test public void testQueryAnd() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade  where  TradeId > '1' and Settled = 'false'"); //$NON-NLS-1$
					
		performTest(command, 1, 4);
	}	
}
