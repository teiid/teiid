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

import org.junit.Before;
import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.simpleMap.SimpleMapCacheExecutionFactory;
import org.teiid.translator.object.testdata.trades.TradesCacheSource;
import org.teiid.translator.object.testdata.trades.VDBUtility;


@SuppressWarnings("nls")
public class TestTradesKeySearch extends BasicSearchTest {	      
	   
	private static ObjectConnection conn = TradesCacheSource.createConnection(false);
	private static ExecutionContext context;
	
	private ObjectExecutionFactory factory = null;

	protected static boolean print = false;
	
	@Before public void beforeEach() throws Exception{	
		 
		factory = new SimpleMapCacheExecutionFactory();

		factory.start();

    }
	
	@Override
	protected ObjectExecution createExecution(Select command) throws TranslatorException {
		return (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, conn);
	}
	

	@Test public void testQueryGetAllTrades() throws Exception {						
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Mat.Trade_Mat.Trade as T"); //$NON-NLS-1$
		
	
		performTest(command, 3, 5);
	}	
}
