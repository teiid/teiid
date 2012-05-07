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
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.Transaction;

import org.junit.Test;
import org.teiid.translator.object.testdata.Leg;
import org.teiid.translator.object.testdata.Trade;

@SuppressWarnings("nls")
public class TestObjectMethodManager {

	@Test public void testInitializationNoClassesLoaded() throws Exception {

		ObjectMethodManager omm = ObjectMethodManager.initialize(true, this.getClass().getClassLoader());
		
		// should not fail
		assertEquals(omm.size(), 0);
		assertNull(omm.get("test"));
		assertNull(omm.getClassMethods("class.not.in.here"));
		
		// when trying to find a method,and the class isn't loaded yet, this should trigger
		// the loading of the class
		assertNotNull(omm.findGetterMethod(Trade.class, "getName"));
		
	}
	
	@Test public void testInitializationLoading1Class() throws Exception {

		ObjectMethodManager omm = ObjectMethodManager.initialize(Trade.class.getName(), true, this.getClass().getClassLoader());
		
		// should not fail
		assertEquals(omm.size(), 1);
		assertNotNull(omm.get(Trade.class.getName()));
		assertNotNull(omm.getClassMethods(Trade.class.getName()));
		assertNotNull(omm.findGetterMethod(Trade.class, "getName"));
		
	}	
	
	@Test public void testInitializationLoadingMulitipleClasses() throws Exception {

		ObjectMethodManager omm = ObjectMethodManager.initialize(Trade.class.getName() + ","
				+ Leg.class.getName() 
				+ ","
				+ Transaction.class.getName()
				, true, this.getClass().getClassLoader());
		
		// should not fail
		assertEquals(omm.size(), 3);
		assertNotNull(omm.getClassMethods(Trade.class.getName()));
		assertNotNull(omm.getClassMethods(Leg.class.getName()));
		assertNotNull(omm.getClassMethods(Transaction.class.getName()));
		assertNotNull(omm.findGetterMethod(Trade.class, "getName"));
		assertNotNull(omm.findGetterMethod(Leg.class, "getName"));
//		assertNotNull(omm.findGetterMethod(Transaction.class, "getLineItem"));
		
	}
	
	@Test public void testInitializationLoadingListOfClasses() throws Exception {

		List<String> classNames = new ArrayList(3);
		classNames.add(Trade.class.getName());
		classNames.add(Leg.class.getName());
		classNames.add(Transaction.class.getName());
		
		
		
		ObjectMethodManager omm = ObjectMethodManager.initialize(classNames,
				true, this.getClass().getClassLoader());
		
		// should not fail
		assertEquals(omm.size(), 3);
		assertNotNull(omm.getClassMethods(Trade.class.getName()));
		assertNotNull(omm.getClassMethods(Leg.class.getName()));
		assertNotNull(omm.getClassMethods(Transaction.class.getName()));
		assertNotNull(omm.findGetterMethod(Trade.class, "getName"));
		assertNotNull(omm.findGetterMethod(Leg.class, "getName"));
//		assertNotNull(omm.findGetterMethod(Transaction.class, "getLineItem"));
		
	}
  
}
