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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;


public class TestSelect extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestSelect(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    public static final Select sample1() {
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a")); //$NON-NLS-1$
        symbols.add(new ElementSymbol("b")); //$NON-NLS-1$

        Select select = new Select();
        MultipleElementSymbol all = new MultipleElementSymbol();
        all.setElementSymbols(symbols);
        select.addSymbol(all);
        return select;
    }

    public static final Select sample2() {
        Select select = new Select();
        select.addSymbol(new ElementSymbol("a"));     //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("b"));     //$NON-NLS-1$
        select.addSymbol(new ElementSymbol("c")); //$NON-NLS-1$
        select.addSymbol(new AliasSymbol("Z", new ElementSymbol("ZZ 9 Plural Z Alpha"))); //$NON-NLS-1$ //$NON-NLS-2$
        return select;
    }

    // ################################## ACTUAL TESTS ################################

    public void testGetProjectedNoElements() {
        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());

        List projectedSymbols = select.getProjectedSymbols();
        assertEquals("Did not get empty list for select * with no elements: ", new ArrayList(), projectedSymbols); //$NON-NLS-1$
    }

    public void testGetProjectedWithStar() {
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a")); //$NON-NLS-1$
        symbols.add(new ElementSymbol("b")); //$NON-NLS-1$

        Select select = new Select();
        MultipleElementSymbol all = new MultipleElementSymbol();
        all.setElementSymbols(symbols);
        select.addSymbol(all);

        List projectedSymbols = select.getProjectedSymbols();
        assertEquals("Did not get correct list for select *: ", symbols, projectedSymbols); //$NON-NLS-1$
    }

    public void testSelfEquivalence(){
        Select s1 = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1);
    }

    public void testEquivalence(){
        Select s1 = sample1();
        Select s1a = sample1();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, s1, s1a);
    }

    public void testNonEquivalence(){
        Select s1 = sample1();
        Select s2 = sample2();
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, s1, s2);
    }

}
