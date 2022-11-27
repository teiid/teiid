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

package org.teiid.dqp.internal.datamgr;

import junit.framework.TestCase;

import org.teiid.language.AndOr;
import org.teiid.language.Comparison;
import org.teiid.language.AndOr.Operator;
import org.teiid.query.sql.lang.CompareCriteria;


public class TestCompoundCriteriaImpl extends TestCase {

    /**
     * Constructor for TestCompoundCriteriaImpl.
     * @param name
     */
    public TestCompoundCriteriaImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.CompoundCriteria helpExample(int operator) {
        CompareCriteria c1 = TestCompareCriteriaImpl.helpExample(CompareCriteria.GE, 100, 200);
        CompareCriteria c2 = TestCompareCriteriaImpl.helpExample(CompareCriteria.LT, 500, 600);
        return new org.teiid.query.sql.lang.CompoundCriteria(operator, c1, c2);
    }

    public static AndOr example(int operator) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(operator));
    }

    public void testGetOperator() throws Exception {
        assertEquals(Operator.AND, example(org.teiid.query.sql.lang.CompoundCriteria.AND).getOperator());
        assertEquals(Operator.OR, example(org.teiid.query.sql.lang.CompoundCriteria.OR).getOperator());
    }

    public void testGetCriteria() throws Exception {
        AndOr cc = example(org.teiid.query.sql.lang.CompoundCriteria.AND);
        assertTrue(cc.getLeftCondition() instanceof Comparison);
        assertTrue(cc.getRightCondition() instanceof Comparison);
    }

}
