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

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;

import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;


/**
 */
public class TestCriteria extends TestCase {

    /**
     * Constructor for TestCriteria.
     * @param arg0
     */
    public TestCriteria(String arg0) {
        super(arg0);
    }

    public CompareCriteria exampleCompareCrit(int num) {
        return new CompareCriteria(
            new ElementSymbol("" + num),  //$NON-NLS-1$
            CompareCriteria.EQ,
            new Constant("" + num)); //$NON-NLS-1$
    }

    public void helpTestSeparateCriteria(Criteria originalCrit, Criteria[] partsArray) {
        Collection<Criteria> expectedParts = Arrays.asList(partsArray);
        Collection<Criteria> actualParts = Criteria.separateCriteriaByAnd(originalCrit);

        assertEquals("Didn't get the same parts ", expectedParts, actualParts); //$NON-NLS-1$
    }

    public void testSeparateCriteriaByAnd1() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        helpTestSeparateCriteria(crit1, new Criteria[] { crit1 });
    }

    public void testSeparateCriteriaByAnd2() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        CompareCriteria crit2 = exampleCompareCrit(2);
        CompoundCriteria compCrit = new CompoundCriteria();
        compCrit.setOperator(CompoundCriteria.AND);
        compCrit.addCriteria(crit1);
        compCrit.addCriteria(crit2);
        helpTestSeparateCriteria(compCrit, new Criteria[] { crit1, crit2 });
    }

    public void testSeparateCriteriaByAnd3() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        CompareCriteria crit2 = exampleCompareCrit(2);
        CompareCriteria crit3 = exampleCompareCrit(3);
        CompoundCriteria compCrit1 = new CompoundCriteria();
        compCrit1.setOperator(CompoundCriteria.AND);
        compCrit1.addCriteria(crit2);
        compCrit1.addCriteria(crit3);
        CompoundCriteria compCrit2 = new CompoundCriteria();
        compCrit2.setOperator(CompoundCriteria.AND);
        compCrit2.addCriteria(crit1);
        compCrit2.addCriteria(compCrit1);
        helpTestSeparateCriteria(compCrit2, new Criteria[] { crit1, crit2, crit3 });
    }

    public void testSeparateCriteriaByAnd4() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        CompareCriteria crit2 = exampleCompareCrit(2);
        CompareCriteria crit3 = exampleCompareCrit(3);
        CompoundCriteria compCrit1 = new CompoundCriteria();
        compCrit1.setOperator(CompoundCriteria.OR);
        compCrit1.addCriteria(crit2);
        compCrit1.addCriteria(crit3);
        CompoundCriteria compCrit2 = new CompoundCriteria();
        compCrit2.setOperator(CompoundCriteria.AND);
        compCrit2.addCriteria(crit1);
        compCrit2.addCriteria(compCrit1);
        helpTestSeparateCriteria(compCrit2, new Criteria[] { crit1, compCrit1 });
    }

    public void testSeparateCriteriaByAnd5() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        CompareCriteria crit2 = exampleCompareCrit(2);
        CompareCriteria crit3 = exampleCompareCrit(3);
        CompoundCriteria compCrit1 = new CompoundCriteria();
        compCrit1.setOperator(CompoundCriteria.AND);
        compCrit1.addCriteria(crit2);
        compCrit1.addCriteria(crit3);
        NotCriteria notCrit = new NotCriteria(compCrit1);
        CompoundCriteria compCrit2 = new CompoundCriteria();
        compCrit2.setOperator(CompoundCriteria.AND);
        compCrit2.addCriteria(crit1);
        compCrit2.addCriteria(notCrit);
        helpTestSeparateCriteria(compCrit2, new Criteria[] { crit1, notCrit });
    }

    public void helpTestCombineCriteria(Criteria crit1, Criteria crit2, Criteria expected) {
        Criteria actual = Criteria.combineCriteria(crit1, crit2);
        assertEquals("Didn't combine the criteria correctly ", expected, actual); //$NON-NLS-1$
    }

    public void testCombineCriteria1() {
        helpTestCombineCriteria(null, null, null);
    }

    public void testCombineCriteria2() {
        helpTestCombineCriteria(exampleCompareCrit(1), null, exampleCompareCrit(1));
    }

    public void testCombineCriteria3() {
        helpTestCombineCriteria(null, exampleCompareCrit(1), exampleCompareCrit(1));
    }

    public void testCombineCriteria4() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        CompareCriteria crit2 = exampleCompareCrit(2);
        CompoundCriteria compCrit = new CompoundCriteria();
        compCrit.setOperator(CompoundCriteria.AND);
        compCrit.addCriteria(crit1);
        compCrit.addCriteria(crit2);
        helpTestCombineCriteria(crit1, crit2, compCrit);
    }

    public void testCombineCriteria5() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        CompareCriteria crit2 = exampleCompareCrit(2);
        CompareCriteria crit3 = exampleCompareCrit(3);
        CompoundCriteria compCrit = new CompoundCriteria();
        compCrit.setOperator(CompoundCriteria.AND);
        compCrit.addCriteria(crit1);
        compCrit.addCriteria(crit2);

        CompoundCriteria compCrit2 = new CompoundCriteria();
        compCrit2.setOperator(CompoundCriteria.AND);
        compCrit2.addCriteria(crit1);
        compCrit2.addCriteria(crit2);
        compCrit2.addCriteria(crit3);

        helpTestCombineCriteria(compCrit, crit3, compCrit2);
    }

    public void testCombineCriteria6() {
        CompareCriteria crit1 = exampleCompareCrit(1);
        CompareCriteria crit2 = exampleCompareCrit(2);
        CompareCriteria crit3 = exampleCompareCrit(3);
        CompoundCriteria compCrit = new CompoundCriteria();
        compCrit.setOperator(CompoundCriteria.AND);
        compCrit.addCriteria(crit1);
        compCrit.addCriteria(crit2);

        CompoundCriteria compCrit2 = new CompoundCriteria();
        compCrit2.setOperator(CompoundCriteria.AND);
        compCrit2.addCriteria(crit3);
        compCrit2.addCriteria(crit1);
        compCrit2.addCriteria(crit2);
        helpTestCombineCriteria(crit3, compCrit, compCrit2);
    }

}
