/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
