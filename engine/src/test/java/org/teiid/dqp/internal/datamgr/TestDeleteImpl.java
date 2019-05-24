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

import org.teiid.language.Delete;
import org.teiid.query.sql.lang.CompoundCriteria;


public class TestDeleteImpl extends TestCase {

    /**
     * Constructor for TestDeleteImpl.
     * @param name
     */
    public TestDeleteImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.Delete helpExample() {
        return new org.teiid.query.sql.lang.Delete(TestGroupImpl.helpExample("vm1.g1"), //$NON-NLS-1$
                          TestCompoundCriteriaImpl.helpExample(CompoundCriteria.AND));
    }

    public static Delete example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetGroup() throws Exception {
        assertNotNull(example().getTable());
    }

    public void testGetCriteria() throws Exception {
        assertNotNull(example().getWhere());
    }

}
