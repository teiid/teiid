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
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


public class TestSubquerySetCriteria extends TestCase {

    public TestSubquerySetCriteria(String name) {
        super(name);
    }

    public static SubquerySetCriteria example1() {
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

        return new SubquerySetCriteria(new ElementSymbol("temp"), query); //$NON-NLS-1$
    }

    public static SubquerySetCriteria example3() {
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

        return new SubquerySetCriteria(new ElementSymbol("temp2"), query); //$NON-NLS-1$
    }

    // ################################## ACTUAL TESTS ################################

    public void testEquals1() {
        SubquerySetCriteria c1 = example1();
        SubquerySetCriteria c2 = example1();
        assertTrue("Equivalent set criteria should have been equal.", c1.equals(c2)); //$NON-NLS-1$
    }

    public void testEquals2() {
        SubquerySetCriteria c1 = example1();
        SubquerySetCriteria c2 = c1.clone();
        assertTrue("Equivalent set criteria should have been equal.", c1.equals(c2)); //$NON-NLS-1$
    }

    public void testEquals3() {
        SubquerySetCriteria c1 = example1();
        SubquerySetCriteria c2 = c1.clone();
        c2.setNegated(true);
        assertFalse("Set criteria are not the same", c1.equals(c2)); //$NON-NLS-1$
    }

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

    public void testEquivalenceDifferent(){
        Object s1 = example1();
        Object s2 = example3();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

}
