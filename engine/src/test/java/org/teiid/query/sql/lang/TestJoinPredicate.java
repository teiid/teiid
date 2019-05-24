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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.*;

import junit.framework.*;

/**
 * Test the <code>JoinPredicate</code> implementation to verify it is producing
 * expected <code>JoinPredicate</code> objects.
 */
public class TestJoinPredicate extends TestCase {

    // ################################## FRAMEWORK ################################

    public TestJoinPredicate(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    /**
     * Constructs an example <code>JoinPredicate</code> object that can be used
     * as join predicate in a query.
     *
     * @param joinType the type of join to be constructed
     * @param joinOnElement the element name to be used in the left and right
     *                         side criteria of the ON expression of the join
     * @return a join predicate object
     */
    public static JoinPredicate example(JoinType joinType, String joinOnElement) {
        JoinPredicate jp = new JoinPredicate();

        GroupSymbol g1 = new GroupSymbol("m.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("m.g2"); //$NON-NLS-1$
        FromClause lc = new UnaryFromClause(g1);
        FromClause rc = new UnaryFromClause(g2);

        Expression le = new ElementSymbol("m.g1." + joinOnElement); //$NON-NLS-1$
        Expression re = new ElementSymbol("m.g2." + joinOnElement); //$NON-NLS-1$
        Criteria c1 = new CompareCriteria(le, CompareCriteria.EQ, re);

        jp.setLeftClause(lc);
        jp.setRightClause(rc);
        jp.setJoinType(joinType != null ? joinType : JoinType.JOIN_LEFT_OUTER);
        jp.setJoinCriteria( Arrays.asList(new Object[]{c1}));

        return jp;
    }

    /**
     * Constructs an example <code>JoinPredicate</code> object that contains
     * compound criteria in the join's ON expression.  The resulting object
     * could be used as join predicate in a query.
     * <p>
     * This method calls <code>example(joinType, joinOnElement)</code> to
     * construct the initial join predicate object.  The join criteria of
     * the initial join predicate object is then modified to add the use
     * of compound criteria which include the original criteria from the
     * initial join predicate object and the newly constructed criteria
     * specified from the <code>andJoinOnElement</code> string.
     *
     * @param joinType the type of join to be constructed
     * @param joinOnElement the element name to be used in the left and right
     *                         side criteria of the ON expression of the join
     * @param andJoinOnElement the element name to be used in the left and right
     *                         side criteria of the right hand expression of AND
     *                         criteria of the ON expression of the join
     * @return a join predicate object
     */
    public static JoinPredicate example(JoinType joinType, String joinOnElement, String andJoinOnElement) {
        JoinPredicate jp = example(joinType, joinOnElement);
        List<Criteria> joinCrits = jp.getJoinCriteria();
        List<Object> newJoinCrits = new ArrayList<Object>(1);

        Expression le = new ElementSymbol("m.g1." + andJoinOnElement); //$NON-NLS-1$
        Expression re = new ElementSymbol("m.g2." + andJoinOnElement); //$NON-NLS-1$
        Criteria c1 = new CompareCriteria(le, CompareCriteria.EQ, re);

        Iterator<Criteria> ci = joinCrits.iterator();

        if (!ci.hasNext()) {
            newJoinCrits.add(c1);
        }

        while (ci.hasNext()) {
            Criteria crit = ci.next();
            if ( ci.hasNext() ) newJoinCrits.add(crit);
            else {
                Criteria compundCrit = new CompoundCriteria(CompoundCriteria.AND, crit, c1);
                newJoinCrits.add(compundCrit);
            }
        }

        jp.setJoinCriteria(newJoinCrits);

        return jp;
    }

    // ################################## ACTUAL TESTS ################################

    /**
     * Test <code>equals()</code> method of <code>JoinPredicate</code> to
     * verify it properly evaluates the equality of two <code>JoinPredicate</code>
     * objects.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * that were constructed with the same join type and with the same criteria
     * evaluate as equal.
     * <p>
     * For example:
     * ... m.g1 LEFT OUTER JOIN m.g2 ON m.g1.e1 = m.g2.e1
     */
    public void testEquals1() {
        JoinPredicate jp1 = example(JoinType.JOIN_LEFT_OUTER, "e1"); //$NON-NLS-1$
        JoinPredicate jp2 = example(JoinType.JOIN_LEFT_OUTER, "e1"); //$NON-NLS-1$
        assertTrue("Equivalent join predicate don't compare as equal: " + jp1 + ", " + jp2, jp1.equals(jp2)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test <code>equals()</code> method of <code>JoinPredicate</code> to
     * verify it properly evaluates the equality of two
     * <code>JoinPredicate</code> objects.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * that were constructed with the same join type and with the same
     * compound criteria evaluate as equal.
     * <p>
     * For example:
     * ... m.g1 LEFT OUTER JOIN m.g2 ON ((m.g1.e1 = m.g2.e1) AND (m.g1.e2 = m.g2.e2))
     */
    public void testEquals2() {
        JoinPredicate jp1 = example(JoinType.JOIN_LEFT_OUTER, "e1", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp2 = example(JoinType.JOIN_LEFT_OUTER, "e1", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Equivalent join predicate don't compare as equal: " + jp1 + ", " + jp2, jp1.equals(jp2)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test <code>equals()</code> method of <code>JoinPredicate</code> to
     * verify it properly evaluates the equality of two <code>JoinPredicate</code>
     * objects.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * that were constructed with different join types but with the same
     * compound criteria evaluate as not equal.
     * <p>
     * For example:
     * ... m.g1 LEFT OUTER JOIN m.g2 ON ((m.g1.e1 = m.g2.e1) AND (m.g1.e2 = m.g2.e2))
     * ... m.g1 RIGHT OUTER JOIN m.g2 ON ((m.g1.e1 = m.g2.e1) AND (m.g1.e2 = m.g2.e2))
     */
    public void testEquals3() {
        JoinPredicate jp1 = example(JoinType.JOIN_LEFT_OUTER, "e1", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp2 = example(JoinType.JOIN_RIGHT_OUTER, "e1", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Different join predicate compare as equal: " + jp1 + ", " + jp2, !jp1.equals(jp2)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test <code>equals()</code> method of <code>JoinPredicate</code> to
     * verify it properly evaluates the equality of two <code>JoinPredicate</code>
     * objects.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * that were constructed with the same join type but with different
     * criteria evaluate as not equal.
     * <p>
     * For example:
     * ... m.g1 INNER JOIN m.g2 ON m.g1.e1 = m.g2.e1
     * ... m.g1 INNER JOIN m.g2 ON m.g1.e2 = m.g2.e2
     */
    public void testEquals4() {
        JoinPredicate jp1 = example(JoinType.JOIN_INNER, "e1"); //$NON-NLS-1$
        JoinPredicate jp2 = example(JoinType.JOIN_INNER, "e2"); //$NON-NLS-1$
        assertTrue("Different join predicate compare as equal: " + jp1 + ", " + jp2, !jp1.equals(jp2)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test <code>equals()</code> method of <code>JoinPredicate</code> to
     * verify it properly evaluates the equality of two <code>JoinPredicate</code>
     * objects.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * that were constructed with the same join type but with different
     * compound criteria evaluate as not equal.
     * <p>
     * For example:
     * ... m.g1 CROSS JOIN m.g2 ON ((m.g1.e1 = m.g2.e1) AND (m.g1.e2 = m.g2.e2))
     * ... m.g1 CROSS JOIN m.g2 ON ((m.g1.e2 = m.g2.e2) AND (m.g1.e2 = m.g2.e2))
     */
    public void testEquals5() {
        JoinPredicate jp1 = example(JoinType.JOIN_CROSS, "e1", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp2 = example(JoinType.JOIN_CROSS, "e2", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Different join predicate compare as equal: " + jp1 + ", " + jp2, !jp1.equals(jp2)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test a <code>JoinPredicate</code> object using <code>UnitTestUtil.helpTestEquivalence</code>.
     * <p>
     * This test ensures that the same <code>JoinPredicate</code> object
     * evaluates as equal when it is compared to itself.
     * <p>
     * For example:
     * ... m.g1 FULL OUTER JOIN m.g2 ON m.g1.e1 = m.g2.e1
     */
    public void testSelfEquivalence(){
        JoinPredicate jp1 = example(JoinType.JOIN_FULL_OUTER, "e1"); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, jp1, jp1);
    }

    /**
     * Test a <code>JoinPredicate</code> object using <code>UnitTestUtil.helpTestEquivalence</code>.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * constructed with the same join type and the same criteria evaluates as
     * equal.
     * <p>
     * For example:
     * ... m.g1 FULL OUTER JOIN m.g2 ON m.g1.e1 = m.g2.e1
     * ... m.g1 FULL OUTER JOIN m.g2 ON m.g1.e1 = m.g2.e1
     */
    public void testEquivalence(){
        JoinPredicate jp1 = example(JoinType.JOIN_FULL_OUTER, "e1"); //$NON-NLS-1$
        JoinPredicate jp2 = example(JoinType.JOIN_FULL_OUTER, "e1"); //$NON-NLS-1$
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, jp1, jp2);
    }

    /**
     * Test a <code>JoinPredicate</code> object using <code>UnitTestUtil.helpTestEquivalence</code>.
     * <p>
     * This test ensures that a <code>JoinPredicate</code> object's clone
     * evaluate as equal when compared to the original object.
     * <p>
     * For example:
     * ... m.g1 UNION JOIN m.g2 ON m.g1.e1 = m.g2.e1
     */
    public void testCloneEquivalence(){
        JoinPredicate jp1 = example(JoinType.JOIN_UNION, "e1"); //$NON-NLS-1$
        JoinPredicate jp2 = (JoinPredicate)jp1.clone();
        int equals = 0;
        UnitTestUtil.helpTestEquivalence(equals, jp1, jp2);
    }

    /**
     * Test a <code>JoinPredicate</code> object using <code>UnitTestUtil.helpTestEquivalence</code>.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * constructed with the same join type but with different criteria evaluate
     * as not equal.
     * <p>
     * For example:
     * ... m.g1 FULL OUTER JOIN m.g2 ON ((m.g1.e1 = m.g2.e1) AND (m.g1.e2 = m.g2.e2))
     * ... m.g1 FULL OUTER JOIN m.g2 ON m.g1.e400 = m.g2.e400
     */
    public void testNonEquivalence1(){
        JoinPredicate jp1 = example(JoinType.JOIN_FULL_OUTER, "e1", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp2 = example(JoinType.JOIN_FULL_OUTER, "e400"); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, jp1, jp2);
    }

    /**
     * Test a <code>JoinPredicate</code> object using <code>UnitTestUtil.helpTestEquivalence</code>.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * constructed with the same join type but with different criteria evaluate
     * as not equal.
     * <p>
     * For example:
     * ... m.g1 FULL OUTER JOIN m.g2 ON ((m.g1.e1 = m.g2.e1) AND (m.g1.e2 = m.g2.e2))
     * ... m.g1 FULL OUTER JOIN m.g2 ON ((m.g1.e2 = m.g2.e2) AND (m.g1.e1 = m.g2.e1))
     */
    public void testNonEquivalence2(){
        JoinPredicate jp1 = example(JoinType.JOIN_LEFT_OUTER, "e1", "e2"); //$NON-NLS-1$ //$NON-NLS-2$
        JoinPredicate jp2 = example(JoinType.JOIN_LEFT_OUTER, "e2", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, jp1, jp2);
    }

    /**
     * Test a <code>JoinPredicate</code> object using <code>UnitTestUtil.helpTestEquivalence</code>.
     * <p>
     * This test ensures that two different <code>JoinPredicate</code> objects
     * constructed with different join types but with the same criteria evaluate
     * as not equal.
     * <p>
     * For example:
     * ... m.g1 LEFT OUTER JOIN m.g2 ON m.g1.e1 = m.g2.e1
     * ... m.g1 RIGHT OUTER JOIN m.g2 ON m.g1.e2 = m.g2.e2
     */
    public void testNonEquivalence3(){
        JoinPredicate jp1 = example(JoinType.JOIN_LEFT_OUTER, "e1"); //$NON-NLS-1$
        JoinPredicate jp2 = example(JoinType.JOIN_RIGHT_OUTER, "e1"); //$NON-NLS-1$
        int equals = -1;
        UnitTestUtil.helpTestEquivalence(equals, jp1, jp2);
    }

}
