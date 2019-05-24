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


import org.teiid.language.Not;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.NotCriteria;


import junit.framework.TestCase;

public class TestNotCriteriaImpl extends TestCase {

    /**
     * Constructor for TestNotCriteriaImpl.
     * @param name
     */
    public TestNotCriteriaImpl(String name) {
        super(name);
    }

    public static NotCriteria helpExample() {
        return new NotCriteria(TestCompareCriteriaImpl.helpExample(CompareCriteria.GE, 100, 200));
    }

    public static Not example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetCriteria() throws Exception {
        assertNotNull(example().getCriteria());
    }

}
