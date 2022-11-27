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

import java.util.List;

import junit.framework.TestCase;

import org.teiid.language.Update;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;

@SuppressWarnings("nls")
public class TestUpdateImpl extends TestCase {

    /**
     * Constructor for TestUpdateImpl.
     * @param name
     */
    public TestUpdateImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.Update helpExample() {
        GroupSymbol group = TestGroupImpl.helpExample("vm1.g1"); //$NON-NLS-1$
        org.teiid.query.sql.lang.Update result = new org.teiid.query.sql.lang.Update();
        result.setGroup(group);
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e1"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e2"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e3"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e4"), new Constant(new Integer(1)));
        result.setCriteria(new CompareCriteria(new Constant(new Integer(1)), CompareCriteria.EQ, new Constant(new Integer(1))));
        return result;
    }

    public static Update example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetGroup() throws Exception {
        assertNotNull(example().getTable());
    }

    public void testGetChanges() throws Exception {
        List changes = example().getChanges();
        assertNotNull(changes);
        assertEquals(4, changes.size());
    }

    public void testGetCriteria() throws Exception {
        assertNotNull(example().getWhere());
    }

}
