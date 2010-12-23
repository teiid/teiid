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

import java.util.List;

import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Into;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;



public class TestQuery extends TestCase {

    // ################################## FRAMEWORK ################################
    
    public TestQuery(String name) { 
        super(name);
    }   
    
    // ################################## TEST HELPERS ################################ 


    /** SELECT y FROM h ORDER BY x */
    public static final Query sample1() { 
        Query q2 = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y"));        //$NON-NLS-1$
        q2.setSelect(select);        
        From from = new From();
        from.addGroup(new GroupSymbol("h")); //$NON-NLS-1$
        q2.setFrom(from);
        OrderBy orderBy = new OrderBy();
        orderBy.addVariable(new ElementSymbol("x")); //$NON-NLS-1$
        q2.setOrderBy(orderBy); 
        return q2;    
    }
    
    /** SELECT xml FROM xmltest.doc1 */
    public static final Query sample2() { 
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
    
    public void testNonEquivalence2(){
        Query s1 = sample1();
        Query s2 = sample1();
        s2.setIsXML(true);
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }
    
    public void testClone() {    
        Query q = sample2();
        Query qclone = (Query)q.clone();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, q, qclone);
    }

    public void testAreResultsCachable(){
    	//SELECT y FROM h
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("y"));        //$NON-NLS-1$
        query.setSelect(select);        
        From from = new From();
        from.addGroup(new GroupSymbol("h")); //$NON-NLS-1$
        query.setFrom(from);
        assertTrue(query.areResultsCachable());
        //set y to be type of Blob or Clob
        select = new Select();
        ElementSymbol column = new ElementSymbol("y");//$NON-NLS-1$
        column.setType(BlobType.class);
        select.addSymbol(column);        
        query.setSelect(select);        
        query.setFrom(from);
        assertTrue(query.areResultsCachable());
        select = new Select();
        column = new ElementSymbol("y");//$NON-NLS-1$
        column.setType(ClobType.class);
        select.addSymbol(column);        
        query.setSelect(select);        
        query.setFrom(from);
        assertTrue(query.areResultsCachable());
        select = new Select();
        column = new ElementSymbol("y");//$NON-NLS-1$
        column.setType(XMLType.class);
        select.addSymbol(column);        
        query.setSelect(select);        
        query.setFrom(from);
        assertTrue(query.areResultsCachable());        
    }
    
    public void testClone2() {    
        Query q = sample2();
        List projectedSymbols = q.getProjectedSymbols();
        assertTrue(projectedSymbols == q.getProjectedSymbols());
        Query qclone = (Query)q.clone();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, q, qclone);
        assertEquals(projectedSymbols, qclone.getProjectedSymbols());
    }  
    
    public void testClone3() {
    	Query q = sample2();
        q.setInto(new Into(new GroupSymbol("#foo"))); //$NON-NLS-1$
        Query qclone = (Query)q.clone();
        assertNotNull(qclone.getInto());
    }
}
