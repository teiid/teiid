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

import junit.framework.TestCase;

import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;



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

    public static final Query sample2() {
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

    public void testClone3() {
        Query q = sample2();
        q.setInto(new Into(new GroupSymbol("#foo"))); //$NON-NLS-1$
        Query qclone = (Query)q.clone();
        assertNotNull(qclone.getInto());
    }
}
