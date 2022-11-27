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
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;

import junit.framework.*;


/**
 * @author amiller
 *
 * To change this generated comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class TestCompoundCriteria extends TestCase {

    /**
     * Constructor for TestCompoundCriteria.
     * @param name
     */
    public TestCompoundCriteria(String name) {
        super(name);
    }

    public void testClone1() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        CompareCriteria ccrit1 = new CompareCriteria(e1, CompareCriteria.EQ, new Constant("abc")); //$NON-NLS-1$
        ElementSymbol e2 = new ElementSymbol("e2"); //$NON-NLS-1$
        CompareCriteria ccrit2 = new CompareCriteria(e2, CompareCriteria.EQ, new Constant("xyz")); //$NON-NLS-1$
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.AND, ccrit1, ccrit2);

        UnitTestUtil.helpTestEquivalence(0, comp, comp.clone());
    }

    public void testClone2() {
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        CompareCriteria ccrit1 = new CompareCriteria(e1, CompareCriteria.EQ, new Constant("abc")); //$NON-NLS-1$
        CompoundCriteria comp = new CompoundCriteria(CompoundCriteria.AND, ccrit1, null);

        UnitTestUtil.helpTestEquivalence(0, comp, comp.clone());
    }

}
