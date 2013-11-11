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

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import javax.naming.Context;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.infinispan.manager.DefaultCacheManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.BasicSearchTest;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.infinispan.InfinispanExecutionFactory;
import org.teiid.translator.object.util.VDBUtility;

import org.teiid.translator.object.testdata.Trade;
import org.teiid.resource.adapter.infinispan.util.TradesCacheSource;

@SuppressWarnings("nls")
public class TestInfinispanJndiNameLuceneSearch extends BasicSearchTest {
	protected static final String JNDI_NAME = "java/MyCacheManager";

	private static ExecutionContext context;
    
    private static InfinispanManagedConnectionFactory factory = null;
    
    private static InfinispanExecutionFactory tfactory;

	
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
	    
		context = mock(ExecutionContext.class);
        
		final DefaultCacheManager container = new DefaultCacheManager("./src/test/resources/infinispan_persistent_indexing_config.xml", true);
        
        TradesCacheSource.loadCache(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));
  
		factory = new InfinispanManagedConnectionFactory() {
			@Override
			Object performJNDICacheLookup(String jndiName) throws Exception {				
				return container;
			}
		};
		
		factory.setCacheJndiName(JNDI_NAME);
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.TRADE_CACHE_NAME + ":" + Trade.class.getName());
				
		tfactory = new InfinispanExecutionFactory();
		tfactory.setSupportsLuceneSearching(true);

		tfactory.start();

	}
	
    @AfterClass
    public static void closeConnection() throws Exception {
    	factory.cleanUp();
    	
        RemoteInfinispanTestHelper.releaseServer();
    }

	@Override
	protected ObjectExecution createExecution(Select command) throws Exception {
		return (ObjectExecution) tfactory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, factory.createConnectionFactory().getConnection());
	}
	
	@Test public void testQueryLikeCriteria1() throws Exception {
		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.TradeName like 'TradeName%'"); //$NON-NLS-1$
					
		performTest(command, 3, 1);
	}	
	
	@Test public void testQueryLikeCriteria2() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.TradeName like 'TradeName 2%'"); //$NON-NLS-1$

		performTest(command, 1, 1);
	}	
	
	@Test public void testQueryCompareEQBoolean() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.Settled = 'false'"); //$NON-NLS-1$
					
		performTest(command, 2, 1);
	}	
	
	@Test public void testQueryCompareNEBoolean() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.Settled <> 'false'"); //$NON-NLS-1$

		performTest(command, 1, 1);
	}		
	
	@Test public void testQueryRangeBetween() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.TradeId > '1' and T.TradeId < '3'"); //$NON-NLS-1$
					
		performTest(command, 1, 1);
	}

	@Test public void testQueryRangeAbove() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.TradeId > '1'"); //$NON-NLS-1$
					
		performTest(command, 2, 1);
	}
	
	@Test public void testQueryRangeBelow() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.TradeId < '2'"); //$NON-NLS-1$
					
		performTest(command, 1, 1);
	}	
	
	@Test public void testQueryAnd() throws Exception {	
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trades_Cache as T WHERE T.TradeId > '1' and T.Settled = 'false' "); //$NON-NLS-1$
					
		performTest(command, 1, 1);
	}	
}
