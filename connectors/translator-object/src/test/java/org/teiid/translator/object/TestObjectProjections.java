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

import org.junit.Test;
import org.teiid.language.Select;
import org.teiid.translator.object.testdata.VDBUtility;

@SuppressWarnings("nls")
public class TestObjectProjections {
	

	@Test public void testQueryRootObject() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select * From Trade_Object.Trade"); //$NON-NLS-1$

	    ObjectProjections op = new ObjectProjections(command);
	    
	    validateArrays(4, op);
	    

	}
	
	@Test public void testQueryIncludeLegs() throws Exception {		
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName From Trade_Object.Trade as T, Trade_Object.Leg as L Where T.TradeId = L.TradeId"); //$NON-NLS-1$

	    ObjectProjections op = new ObjectProjections(command);
	    validateArrays(3, op);
	}	
	
	@Test public void testQueryGetAllTransactions() throws Exception {
		Select command = (Select)VDBUtility.TRANSLATION_UTILITY.parseCommand("select T.TradeId, T.Name as TradeName, L.Name as LegName, " + 
				" N.LineItem " +
				" From Trade_Object.Trade as T, Trade_Object.Leg as L, Trade_Object.Transaction as N " + 
				" Where T.TradeId = L.TradeId and L.LegId = N.LegId"); //$NON-NLS-1$
	    ObjectProjections op = new ObjectProjections(command);
	    validateArrays(4, op);

	
	}	
	
	private void validateArrays(int size, ObjectProjections op) throws Exception {
		op.throwExceptionIfFound();
		
	    assertEquals(size, op.getColumnNamesToUse().length);
	    assertEquals(size, op.getColumns().length);

	    // confirm the arrays match
	    for (int i = 0; i < op.getColumns().length; i++) {
	    	assertEquals(op.getColumnNamesToUse()[i], op.getColumnNameToUse(op.getColumns()[i]));
	    	System.out.println("ColumnToUse: " + op.getColumnNamesToUse()[i]);
	    }
	}

}
