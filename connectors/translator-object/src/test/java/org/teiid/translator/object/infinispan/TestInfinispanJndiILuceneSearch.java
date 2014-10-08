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
import org.teiid.translator.object.util.VDBUtility;

@SuppressWarnings("nls")
public class TestInfinispanJndiILuceneSearch extends BasicSearchTest {

	private static ExecutionContext context;
    private static ObjectConnection conn;
    private InfinispanExecutionFactory factory = null;
		
	@BeforeClass
    public static void beforeEachClass() throws Exception {  
		
		conn = TestInfinispanConnection.createConnection("./src/test/resources/infinispan_persistent_indexing_config.xml");
		
		context = mock(ExecutionContext.class);
	}

	@Before public void beforeEachTest() throws Exception{	
		factory = new InfinispanExecutionFactory();
		factory.setSupportsLuceneSearching(true);
		factory.start();
    }
	
	@AfterClass
	public static void afterClass() {
		((TestInfinispanConnection) conn).cleanUp();
	}

	@Override
	protected ObjectExecution createExecution(Select command) throws TranslatorException {
		return (ObjectExecution) factory.createExecution(command, context, VDBUtility.RUNTIME_METADATA, conn);
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
