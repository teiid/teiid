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
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.teiid.language.Select;
import org.teiid.translator.object.mapcache.MapCacheSearchByKey;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;


@SuppressWarnings("nls")
public class TestSelectProjections {
		
	private  ObjectExecutionFactory factory;
	
	@Before public void beforeEach() throws Exception{
		
		MockitoAnnotations.initMocks(this);
		
		factory = new ObjectExecutionFactory() {};
		
		factory.setSearchStrategyClassName(MapCacheSearchByKey.class.getName());
		factory.setRootClassName(TradesCacheSource.TRADE_CLASS_NAME);
		
		factory.start();

	}
	
	
	@Test public void testQueryRootObject() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade"); //$NON-NLS-1$
		SelectProjections visitor = createSelectProjections(command);

	    validateResults(true, visitor);

	}
	
	@Test public void testQueryWithNonSearchableColumn() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select  L.Name as LegName, L.TradeId as ID From  Trade_Object.Leg as L"); //$NON-NLS-1$
		SelectProjections visitor = createSelectProjections(command);
	    validateResults( false, visitor);
	}	
		
	
	@Test public void testQuery1LevelDownWithRootNotInSelect() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select  L.Name as LegName From  Trade_Object.Leg as L"); //$NON-NLS-1$
		SelectProjections visitor = createSelectProjections(command);
	    validateResults( false, visitor);
	}	
	
	@Test public void testQuery2LevelDownWithRootNotInSelect() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select  N.LineItem " +
				" From Trade_Object.Transaction as N "); //$NON-NLS-1$
		SelectProjections visitor = createSelectProjections(command);
	    validateResults(false, visitor);

	}
	
	@Test public void testQueryIncludeLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$
		SelectProjections visitor = createSelectProjections(command);
	    validateResults(true, visitor);
	}	
	
	@Test public void testQueryGetAllTransactions() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId"); //$NON-NLS-1$
		SelectProjections visitor = createSelectProjections(command);
	    validateResults(true, visitor);

	}	
	

	@Test public void testIN() throws Exception {
		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID IN (1,2,3)"); //$NON-NLS-1$
		SelectProjections visitor = createSelectProjections(command);

		validateResults(true, visitor);
	
	}
	
	private SelectProjections createSelectProjections(Select command) {
		SelectProjections visitor = SelectProjections.create(factory);
		visitor.parse(command);
		return visitor;
		
	}
		
	private void validateResults( boolean rootNodeInQuery, SelectProjections visitor) throws Exception {
			
	    assertEquals(rootNodeInQuery, visitor.isRootTableInFrom());
	    assertNotNull(visitor.getRootNodePrimaryKeyColumnName());
	    assertNotNull(visitor.getRootTableName());

	}
		
}
