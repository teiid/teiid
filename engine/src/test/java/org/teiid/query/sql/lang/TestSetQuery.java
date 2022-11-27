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

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


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
