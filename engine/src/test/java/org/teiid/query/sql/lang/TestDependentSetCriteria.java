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

import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;



/**
 */
public class TestDependentSetCriteria extends TestCase {

    /**
     *
     */
    public TestDependentSetCriteria() {
        super();
    }

    /**
     * @param name
     */
    public TestDependentSetCriteria(String name) {
        super(name);
    }

    private DependentSetCriteria example() {
        ElementSymbol e1 = new ElementSymbol("pm1.g1.e1"); //$NON-NLS-1$
        DependentSetCriteria dsc = new DependentSetCriteria(e1, ""); //$NON-NLS-1$

        final ElementSymbol e2 = new ElementSymbol("pm2.g1.e2"); //$NON-NLS-1$
        dsc.setValueExpression(e2);

        return dsc;
    }

    public void testEquivalence() {
        DependentSetCriteria dsc = example();

        UnitTestUtil.helpTestEquivalence(0, dsc, dsc);
        UnitTestUtil.helpTestEquivalence(0, dsc, dsc.clone());
    }

    public void testEquivalence1() {
        DependentSetCriteria dsc = example();
        DependentSetCriteria dsc1 = example();

        dsc1.setValueExpression(new ElementSymbol("foo")); //$NON-NLS-1$

        assertNotSame(dsc, dsc1);
    }

}
