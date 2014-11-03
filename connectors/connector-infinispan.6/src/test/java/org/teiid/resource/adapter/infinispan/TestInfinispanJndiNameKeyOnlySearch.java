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

import static org.mockito.Mockito.*;

import java.util.Collections;

import org.infinispan.manager.DefaultCacheManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.object.BasicSearchTest;
import org.teiid.translator.object.CacheContainerWrapper;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.infinispan.InfinispanExecutionFactory;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.testdata.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanJndiNameKeyOnlySearch extends BasicSearchTest {
	protected static final String JNDI_NAME = "java/MyCacheManager";

	
 	private static ExecutionContext context;
    
    private static InfinispanManagedConnectionFactory factory = null;
    
    private static InfinispanExecutionFactory tfactory;
	
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
	    
		context = mock(ExecutionContext.class);
        
		final DefaultCacheManager container = new DefaultCacheManager("./src/test/resources/infinispan_persistent_config.xml", true);
        
        TradesCacheSource.loadCache(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));
  
		factory = new InfinispanManagedConnectionFactory() {
			@Override
			protected Object performJNDICacheLookup(String jndiName) throws Exception {
				return container;
			}
		};
		
		factory.setCacheJndiName(JNDI_NAME);
		factory.setCacheTypeMap(RemoteInfinispanTestHelper.TRADE_CACHE_NAME + ":" + Trade.class.getName());

		tfactory = new InfinispanExecutionFactory();

		tfactory.start();

	}
	
    @AfterClass
    public static void closeConnection() throws Exception {
   	    CacheContainerWrapper ccw = factory.getCacheContainer();
	    
	    ReflectionHelper h = new ReflectionHelper(ccw.getClass());
	    
	    (h.findBestMethodWithSignature("cleanUp", Collections.EMPTY_LIST)).invoke(ccw);

      	factory.cleanUp();
    	
        RemoteInfinispanTestHelper.releaseServer();
    }
    
	@Override
	protected ObjectExecution createExecution(Select command) throws Exception {
		return (ObjectExecution) tfactory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, factory.createConnectionFactory().getConnection());
	}

}
