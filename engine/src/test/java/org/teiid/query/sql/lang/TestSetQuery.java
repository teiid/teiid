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

package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


public class TestSetQuery extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestSetQuery(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	

	/** SELECT x FROM g UNION ALL SELECT y FROM h */
	public static final SetQuery sample1() { 
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x"));        //$NON-NLS-1$
        q1.setSelect(select);        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        q1.setFrom(from);

        Query q2 = new Query();
        select = new Select();
        select.addSymbol(new ElementSymbol("y"));        //$NON-NLS-1$
        q2.setSelect(select);        
        from = new From();
        from.addGroup(new GroupSymbol("h")); //$NON-NLS-1$
        q2.setFrom(from);
        
        SetQuery sq = new SetQuery(Operation.UNION);
        sq.setLeftQuery(q1);
        sq.setRightQuery(q2);
        return sq;
	}

	/** SELECT x FROM g INTERSECT SELECT y FROM h ORDER BY x */
	public static final SetQuery sample2() { 
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x"));        //$NON-NLS-1$
        q1.setSelect(select);        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        q1.setFrom(from);

        Query q2 = new Query();
        select = new Select();
        select.addSymbol(new ElementSymbol("y"));        //$NON-NLS-1$
        q2.setSelect(select);        
        from = new From();
        from.addGroup(new GroupSymbol("h")); //$NON-NLS-1$
        q2.setFrom(from);
   
        SetQuery sq = new SetQuery(Operation.INTERSECT);
        sq.setAll(false);
        sq.setLeftQuery(q1);
        sq.setRightQuery(q2);

        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("x")); //$NON-NLS-1$
        sq.setOrderBy(orderBy);	
        return sq;    
	}
	
	/** SELECT xml FROM xmltest.doc1 */
	public static final Query sample3() { 
		Query q1 = new Query();
		Select select = new Select();
		select.addSymbol(new ElementSymbol("xml"));        //$NON-NLS-1$
		q1.setSelect(select);        
		q1.setIsXML(true);
		
		From from = new From();
		from.addGroup(new GroupSymbol("xmltest.doc1")); //$NON-NLS-1$
		q1.setFrom(from);

		return q1;    
	}
	
	// ################################## ACTUAL TESTS ################################
	
	public void test1() {    
        SetQuery sq = sample1();
        assertEquals("Union string doesn't match expected: ",  //$NON-NLS-1$
                     "SELECT x FROM g UNION ALL SELECT y FROM h",  //$NON-NLS-1$
                     sq.toString());
	}


	public void test2() {
        Query q1 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("x"));        //$NON-NLS-1$
        q1.setSelect(select);        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        q1.setFrom(from);

        Query q2 = new Query();
        select = new Select();
        select.addSymbol(new ElementSymbol("y"));        //$NON-NLS-1$
        q2.setSelect(select);        
        from = new From();
        from.addGroup(new GroupSymbol("h")); //$NON-NLS-1$
        q2.setFrom(from);
   
        SetQuery sq = new SetQuery(Operation.INTERSECT);
        sq.setAll(false);
        sq.setLeftQuery(q1);
        sq.setRightQuery(q2);
        
        assertEquals("Query combiner string doesn't match expected: ",  //$NON-NLS-1$
                     "SELECT x FROM g INTERSECT SELECT y FROM h",  //$NON-NLS-1$
                     sq.toString());
	}

	public void test3() {
        SetQuery sq = sample2();
                
        assertEquals("Query combiner string doesn't match expected: ",  //$NON-NLS-1$
                     "SELECT x FROM g INTERSECT SELECT y FROM h ORDER BY x",  //$NON-NLS-1$
                     sq.toString());
	}

	public void testSelfEquivalence(){
		Object s1 = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1);
	}

	public void testEquivalence(){
		Object s1 = sample1();
		Object s1a = sample1();
		int equals = 0;
		UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
	}
	
	public void testNonEquivalence(){
		Object s1 = sample1();
		Object s2 = sample2();
		int equals = -1;
		UnitTestUtil.helpTestEquivalence(equals, s1, s2);
	}
	
	public void testQuery() {    
		Query q = sample3();
		
		List projList = new ArrayList();
		projList.add(new ElementSymbol("xml")); //$NON-NLS-1$
		
		assertEquals("result is not as expected.", //$NON-NLS-1$
			projList, 
			q.getProjectedSymbols());
	}
}
