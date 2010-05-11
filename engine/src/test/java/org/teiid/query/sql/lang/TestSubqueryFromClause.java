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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


public class TestSubqueryFromClause extends TestCase {

    public TestSubqueryFromClause(String arg0) {
        super(arg0);
    }

    // ################################## HELPERS ################################

    public static SubqueryFromClause example1() {
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("b"));         //$NON-NLS-1$
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        query.setFrom(from);
        CompareCriteria crit = new CompareCriteria();
        crit.setLeftExpression(new ElementSymbol("a")); //$NON-NLS-1$
        crit.setRightExpression(new Constant(new Integer(5)));
        crit.setOperator(CompareCriteria.EQ);
        query.setCriteria(crit);
        
        return new SubqueryFromClause("temp", query); //$NON-NLS-1$
    }

    public static SubqueryFromClause example2() {
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("c")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("d"));         //$NON-NLS-1$
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("m.g2")); //$NON-NLS-1$
        query.setFrom(from);
        CompareCriteria crit = new CompareCriteria();
        crit.setLeftExpression(new ElementSymbol("c")); //$NON-NLS-1$
        crit.setRightExpression(new Constant(new Integer(10)));
        crit.setOperator(CompareCriteria.EQ);
        query.setCriteria(crit);
        
        return new SubqueryFromClause("temp", query); //$NON-NLS-1$
    }

    public static SubqueryFromClause example3() {
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("a")); //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("b"));         //$NON-NLS-1$
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        query.setFrom(from);
        CompareCriteria crit = new CompareCriteria();
        crit.setLeftExpression(new ElementSymbol("a")); //$NON-NLS-1$
        crit.setRightExpression(new Constant(new Integer(5)));
        crit.setOperator(CompareCriteria.EQ);
        query.setCriteria(crit);
        
        return new SubqueryFromClause("temp2", query); //$NON-NLS-1$
    }

    // ################################## ACTUAL TESTS ################################
    
    public void testSelfEquivalence(){
        Object s1 = example1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        Object s1 = example1();
        Object s1a = example1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testEquivalenceDifferentName(){
        Object s1 = example1();
        Object s2 = example3();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }
   
    public void testCommandNonEquivalence(){
        Object s1 = example1();
        Object s2 = example2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }
    
    public void testEquivalenceDifferentOptional(){
        Object s1 = example1();
        SubqueryFromClause s2 = example1();
        s2.setOptional(true);
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }
    
    public void testClone() {
        SubqueryFromClause s1 = example1();
        Object clonedS1 = s1.clone();
        assertEquals(s1, clonedS1);
    }
}
