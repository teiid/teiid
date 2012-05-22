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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.util.VDBUtility;


@SuppressWarnings("nls")
public class TestObjectVisitor {
		
	
	@Mock
	private  ObjectExecutionFactory factory;
	
	@Before public void beforeEach() {	
		
		MockitoAnnotations.initMocks(this);
		
		when(factory.isSupportFilters()).thenReturn(true);

	}
	
	
	@Test public void testQueryRootObject() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade"); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
	    validateResults(4, 0, true, visitor);

	}
	
	@Test public void testQueryWithNonSearchableColumn() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select  L.Name as LegName, L.TradeId as ID From  Trade_Object.Leg as L"); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
	    validateResults(1, 1, false, visitor);
	}	
		
	
	@Test public void testQuery1LevelDownWithRootNotInSelect() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select  L.Name as LegName From  Trade_Object.Leg as L"); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
	    validateResults(1, 1, false, visitor);
	}	
	
	@Test public void testQuery2LevelDownWithRootNotInSelect() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select  N.LineItem " +
				" From Trade_Object.Transaction as N "); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
	    validateResults(1, 2, false, visitor);

	}
	
	@Test public void testQueryIncludeLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
	    validateResults(3, 1, true, visitor);
	}	
	
	@Test public void testQueryGetAllTransactions() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId"); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
	    validateResults(4, 2, true, visitor);

	}	
	
	private ObjectVisitor createVisitor(Select command) {
		ObjectVisitor visitor = new ObjectVisitor(factory, VDBUtility.RUNTIME_METADATA);
		visitor.visitNode(command);
		return visitor;
		
	}
		
	
	private void validateResults(int size, int depth, boolean rootNodeInQuery, ObjectVisitor visitor) throws Exception {
		visitor.throwExceptionIfFound();
		
		assertEquals(Trade.class.getName(), visitor.getRootTable().getNameInSource());
	    assertEquals(size, visitor.getColumnNamesToUse().length);
	    assertEquals(size, visitor.getColumns().length);
	    assertEquals(rootNodeInQuery, visitor.isRootTableInFrom());

	    int d = (visitor.childrenDepth > -1 ? visitor.childrenDepth : 0 );

	    assertEquals(depth, d);
	    
	  //if children, then there should always be one more child node than the depth
	    assertEquals(depth, (visitor.childrenNodes == null ? 0 : visitor.childrenNodes.size() - 1) ); 
	    assertEquals(size, visitor.nameNodes.length);
	    assertEquals(size, visitor.nodeDepth.length);

	    // confirm the arrays match
	    for (int i = 0; i < visitor.getColumns().length; i++) {
	    	assertEquals(visitor.getColumnNamesToUse()[i], visitor.getColumnNameToUse(visitor.getColumns()[i]));
	    }
	}
	

	@Test public void testIN() throws Exception {
		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID IN (1,2,3)"); //$NON-NLS-1$

		ObjectVisitor visitor = createVisitor(command);
		
		validateResults(4, 0, true, visitor);
//		validateSelectVisitorAllRootTableColumns(visitor, null, 4);

		assertNotNull("Key Column In Search", visitor.getCriterion());
		
		assertEquals("Filters", visitor.getFilters().size(), 0);
	
	}

	@Test public void test1Equals() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID = 1"); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
		
		validateSelectVisitorAllRootTableColumns(visitor,  null, 4);

		assertNotNull("Key Column In Search", visitor.getCriterion());
		
		assertEquals("Filters", visitor.getFilters().size(), 0);
		
	}
	
	@Test public void test1EqualsFilter() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.Name = 'MyName'"); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
		
		List<String> names = new ArrayList(1);
		names.add("Name");
		validateSelectVisitorAllRootTableColumns(visitor, names, 4);

		assertNotNull("Key Column In Search", visitor.getCriterion());

		assertEquals("Filters", visitor.getFilters().size(), 1);

	}		

	
	@Test public void testQueryIncludeLegsNoCriteria() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$

		ObjectVisitor visitor = createVisitor(command);
		validateSelectVisitorAllRootTableColumns(visitor, null, 3);
		
		assertEquals("Filters", visitor.getFilters().size(), 0);

	}	
	
	@Test public void testQueryIncludeLegsWithCriteria() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId and L.Name='MyLeg'"); //$NON-NLS-1$

		ObjectVisitor visitor = createVisitor(command);
		List<String> names = new ArrayList(1);
		names.add("Name");
		validateSelectVisitorAllRootTableColumns(visitor, names, 3);
		
		assertEquals("Filters", visitor.getFilters().size(), 1);

	}	
	
	@Test public void testQueryGetAllTransactionsNoCriteria() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId "); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
		
		validateSelectVisitorAllRootTableColumns(visitor, null, 4);

		assertEquals("Filters", visitor.getFilters().size(), 0);

	
	}	
	@Test public void testQueryGetTransactionsUseKeyCriteria() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId and T.TradeId in (1,2,3) "); //$NON-NLS-1$
		ObjectVisitor visitor = createVisitor(command);
		
		validateSelectVisitorAllRootTableColumns(visitor, null, 4);		

		assertNotNull("Key Column In Search", visitor.getCriterion());
		assertEquals("Filters", visitor.getFilters().size(), 0);
	
	}		
	
	private void validateSelectVisitorAllRootTableColumns(ObjectVisitor visitor, List<String> columnFilters, int numOfColumns) throws Exception {
		
		Column[] columns = visitor.getColumns();
		String[] columnNamesToUse = visitor.getColumnNamesToUse();
		
		assertNotNull("ColumnNames", columns);
		assertEquals("Column Names", numOfColumns, columns.length);
		assertEquals("Column Names", columnNamesToUse.length, columns.length);
		

		boolean hasName = false;
		boolean hasTradeId = false;
		for (int i=0; i<columns.length; i++) {
			Column col = columns[i];
			if (col.getName().equalsIgnoreCase("Name")) hasName = true;
			else if (col.getName().equalsIgnoreCase("TradeId")) hasTradeId = true;
			assertNotNull("Column Type", col.getDatatype());
			assertNotNull("Column Java Type", col.getJavaType());
			assertNotNull("Column Name to Use", columnNamesToUse[i]);
			assertNotNull("Column Native Type", col.getNativeType());
			
			assertNotNull("Column Table Name", col.getParent().getName());


		}

		assertTrue("Missing NAME column", hasName);
		assertTrue("Missing TRADEID column", hasTradeId);
	}
	
}
