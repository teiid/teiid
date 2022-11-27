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


import org.teiid.language.IsNull;
import org.teiid.query.sql.lang.IsNullCriteria;


import junit.framework.TestCase;

public class TestIsNullCriteriaImpl extends TestCase {

    /**
     * Constructor for TestIsNullCriteriaImpl.
     * @param name
     */
    public TestIsNullCriteriaImpl(String name) {
        super(name);
    }

    public static IsNullCriteria helpExample(boolean negated) {
        IsNullCriteria crit = new IsNullCriteria(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        crit.setNegated(negated);
        return crit;
    }

    public static IsNull example(boolean negated) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(negated));
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example(false).getExpression());
    }

    public void testIsNegated() throws Exception {
        assertTrue(example(true).isNegated());
        assertFalse(example(false).isNegated());
    }

}
