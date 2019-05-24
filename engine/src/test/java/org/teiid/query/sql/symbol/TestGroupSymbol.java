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

package org.teiid.query.sql.symbol;


import junit.framework.TestCase;

import org.teiid.query.sql.lang.UnaryFromClause;


/**
 * @since 4.2
 */
public class TestGroupSymbol extends TestCase {

    /**
     * Constructor for TestGroupSymbol.
     * @param name
     */
    public TestGroupSymbol(String name) {
        super(name);
    }

    public void testIsTempGroupSymbol() {
        GroupSymbol group = new GroupSymbol("g1"); //$NON-NLS-1$
        assertFalse(group.isTempGroupSymbol());
        group = new GroupSymbol("#temp"); //$NON-NLS-1$
        assertTrue(group.isTempGroupSymbol());
    }

    public void testIsNotTempGroupSymbol() {
        GroupSymbol group = new GroupSymbol("g1"); //$NON-NLS-1$
        assertFalse(group.isTempGroupSymbol());
        group = new GroupSymbol("temp"); //$NON-NLS-1$
        assertFalse(group.isTempGroupSymbol());
    }

    public void testEquality() {
        GroupSymbol group = new GroupSymbol("g1", "a"); //$NON-NLS-1$ //$NON-NLS-2$
        GroupSymbol group1 = new GroupSymbol("g1", "b"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(group, group1);
    }

    public void testInequality1() {
        GroupSymbol group = new GroupSymbol("g1", "a"); //$NON-NLS-1$ //$NON-NLS-2$
        GroupSymbol group1 = new GroupSymbol("g1"); //$NON-NLS-1$
        assertFalse(new UnaryFromClause(group).equals(new UnaryFromClause(group1)));
    }

}
