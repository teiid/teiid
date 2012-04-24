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
package org.teiid.translator.object.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectProjections;
import org.teiid.translator.object.testdata.Trade;
import org.teiid.translator.object.testdata.VDBUtility;


@SuppressWarnings("nls")
public class TestMapCacheVisitor {
		
	private static int CNT = 0;
	@Before public void setup() {	
		CNT = 0;
	}

	@Test public void testIN() throws Exception {
		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID IN (1,2,3)"); //$NON-NLS-1$
		ObjectProjections op = new ObjectProjections(command);

		MapCacheObjectVisitor visitor =new MapCacheObjectVisitor() {
			@Override
			public void addInCriteria(String objectName, String attributeName,
					List<Object> parms, Class<?> type)
					throws TranslatorException {
				CNT+=parms.size();				
			}		

		};

		visitor.visit(command);
		
		validateSelectVisitorAllRootTableColumns(op);
		assertEquals("In Criteria", 3, CNT);
	
	}

	@Test public void test1Equals() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID = 1"); //$NON-NLS-1$
		ObjectProjections op = new ObjectProjections(command);

		MapCacheObjectVisitor visitor =new MapCacheObjectVisitor() {
			
			@Override
			public void addCompareCriteria(String objectName,
					String attributeName, Object value, Operator op,
					Class<?> type) throws TranslatorException {

				if (value.toString().equals("1")) {
					++CNT;						
				}
			}			

		};
			
		visitor.visit(command);
		
		validateSelectVisitorAllRootTableColumns(op);
		assertEquals("Equals Criteria", 1, CNT);
	}	
	
	@Test public void testLike() throws Exception {

		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade where Trade_Object.Trade.Name like 'Test%'"); //$NON-NLS-1$
		ObjectProjections op = new ObjectProjections(command);
		MapCacheObjectVisitor visitor =new MapCacheObjectVisitor() {
			
			@Override
			public void addLikeCriteria(String objectName,
					String attributeName, Object value)
					throws TranslatorException {

				if (value.toString().equals("Test%")) {
					++CNT;						
				}
			}			

		};
			
		visitor.visit(command);
		
		validateSelectVisitorAllRootTableColumns(op);
		assertEquals("Like Criteria", 1, CNT);
	}	
	
	private void validateSelectVisitorAllRootTableColumns(ObjectProjections visitor) throws Exception {
		
		Column[] columns = visitor.getColumns();
		String[] columnNamesToUse = visitor.getColumnNamesToUse();
		
//		String[] colnames = visitor.getColumnNames();
		
		assertNotNull("ColumnNames", columns);
		assertEquals("Column Names", Trade.NUM_ATTRIBUTES, columns.length);
		assertEquals("Column Names", columnNamesToUse.length, columns.length);
		

		boolean hasName = false;
		boolean hasTradeId = false;
		for (int i=0; i<columns.length; i++) {
			Column col = columns[i];
			if (col.getName().equalsIgnoreCase("Name")) hasName = true;
			else if (col.getName().equalsIgnoreCase("TradeId")) hasTradeId = true;
			// don't validate NIS, because it can be null
//			assertNotNull("Column NIS", visitor.getColumnNameInSource(colnames[i]));
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
