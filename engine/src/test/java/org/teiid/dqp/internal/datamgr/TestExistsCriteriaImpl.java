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

import org.teiid.language.Exists;
import org.teiid.language.Not;
import org.teiid.query.sql.lang.ExistsCriteria;

/**
 */
public class TestExistsCriteriaImpl extends TestCase {

    /**
     * Constructor for TestExistsCriteriaImpl.
     * @param name
     */
    public TestExistsCriteriaImpl(String name) {
        super(name);
    }

    public static ExistsCriteria helpExample(boolean negated) {
        ExistsCriteria crit = new ExistsCriteria(TestQueryImpl.helpExample(true));
        crit.setNegated(negated);
        return crit;
    }

    public static Exists example() throws Exception {
        return (Exists)TstLanguageBridgeFactory.factory.translate(helpExample(false));
    }

    public void testGetQuery() throws Exception {
        assertNotNull(example().getSubquery());
    }

    public void testNegated() throws Exception {
        assertTrue(TstLanguageBridgeFactory.factory.translate(helpExample(true)) instanceof Not);
    }

}
