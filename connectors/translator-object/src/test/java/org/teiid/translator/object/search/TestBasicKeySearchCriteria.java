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
package org.teiid.translator.object.search;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.util.TradesCacheSource;
import org.teiid.translator.object.util.VDBUtility;


@SuppressWarnings("nls")
public class TestBasicKeySearchCriteria {
		

	private  ObjectExecutionFactory factory;
	
	@Before public void beforeEach() throws Exception {	
			 
		factory = new ObjectExecutionFactory() {};

		factory.setRootClassName(TradesCacheSource.TRADE_CLASS_NAME);

	}
		
	private BasicKeySearchCriteria createVisitor(Select command) throws Exception {
		return BasicKeySearchCriteria.getInstance(factory, command);		
	}
	

	@Test public void testIN() throws Exception {
		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID IN ('1','2','3')"); //$NON-NLS-1$
		
		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 1, false, true);
	
	}

	@Test public void test1Equals() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID = '1'"); //$NON-NLS-1$

		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 1, false, true);
		
	}
	
	@Ignore(value="Or is unnecessary as the optimizer will use IN and it was not implemented correctly (the results were not cumulative and we have to restict pushdown support of comparisions to only the primary key as no post filtering is performed")
	@Test public void test5Equals() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.Name = 'MyName' and (Trade_Object.Trade.TradeId = '2' or  Trade_Object.Trade.Settled = 'true') or (Trade_Object.Trade.Settled = 'false' and Trade_Object.Trade.TradeId = 3) "); //$NON-NLS-1$

		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 5, false, true);
	}		

	@Test public void testQueryIncludeLegsNoCriteria() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$

		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 1, false, true);
		
		assertEquals(criteria.getCriterion().getOperator(), SearchCriterion.Operator.ALL);
	}	
	
	@Test public void testQueryIncludeLegsWithCriteria() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId and L.Name='MyLeg'"); //$NON-NLS-1$

		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 1, false, true);
	}	
	
	@Test public void testQueryGetAllTransactionsNoCriteria() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId "); //$NON-NLS-1$

		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 1, false, true);
				
		assertEquals(criteria.getCriterion().getOperator(), SearchCriterion.Operator.ALL);

	
	}	
	@Test public void testQueryGetTransactionsUseKeyCriteria() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId and T.TradeId in ('1','2','3') "); //$NON-NLS-1$

		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 1, false, true);
	
	}
	
	@Test public void testQueryLegsWithCriteria() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select L.Name as LegName From Trade_Object.Leg as L Where L.Name='MyLeg'"); //$NON-NLS-1$

		BasicKeySearchCriteria criteria = createVisitor(command);
		validateSearchCriteria(criteria.getCriterion(), 1, false, false);
	}	
	
	private void validateSearchCriteria(SearchCriterion criteria, int cnt, boolean isAnd, boolean isRootInSelect) {
		if (cnt == 0) {
			assertNull("Criteria should be null", criteria);
			return;
		} 

		
		assertNotNull(criteria);
		assertEquals( "Criteria Count", cnt, criteria.getCriteriaCount());

		if (criteria.getOperator() != SearchCriterion.Operator.ALL) {
		
			assertNotNull(criteria.getColumn());
			assertNotNull(criteria.getOperator());
			assertNotNull(criteria.getTableName());
			assertNotNull(criteria.getRuntimeType());
			assertNotNull(criteria.getValue());
			
			assertEquals("IsAnd", isAnd, criteria.isAndCondition());

		} else {
			assertEquals("Criteria Cnt must be 1 when using ALL", 1, criteria.getCriteriaCount());
		}
	}
	
}
