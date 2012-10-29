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

import static org.mockito.Mockito.*;

import java.util.Map;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.BeforeClass;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.BasicSearchTest;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanConfigFileKeySearch extends BasicSearchTest {
    
	static final class InfinispanConnection implements ObjectConnection {
		private final CacheContainer container;

		InfinispanConnection(CacheContainer container) {
			this.container = container;
		}

		@Override
		public Class<?> getType(String name) throws TranslatorException {
			return Trade.class;
		}

		@Override
		public Map<?, ?> getMap(String name) throws TranslatorException {
			//the real connection should use the name in source to get the cache
			return container.getCache(TradesCacheSource.TRADES_CACHE_NAME);
		}

		@Override
		public Map<String, Class<?>> getMapOfCacheTypes() {
			return null;
		}
		
		@Override
		public String getPkField(String name) {
			return null;
		}
	}

	private static ExecutionContext context;

    
    private static InfinispanExecutionFactory factory = null;
    
    private static ObjectConnection conn;
		

	@BeforeClass
    public static void beforeEachClass() throws Exception {  
        // Set up the mock JNDI ...
        
		context = mock(ExecutionContext.class);
		
		factory = new InfinispanExecutionFactory();

		final DefaultCacheManager container = new DefaultCacheManager("./src/test/resources/infinispan_persistent_config.xml");

		factory.start();
		
		TradesCacheSource.loadCache(container.getCache(TradesCacheSource.TRADES_CACHE_NAME));

		conn = new InfinispanConnection(container);
	}
	
	@Override
	protected ObjectExecution createExecution(Select command) throws TranslatorException {
		return (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, conn);
	}
	
}
