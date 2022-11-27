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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.language.ColumnReference;
import org.teiid.language.GroupBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;


public class TestGroupByImpl extends TestCase {

    /**
     * Constructor for TestGroupByImpl.
     * @param name
     */
    public TestGroupByImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.lang.GroupBy helpExample() {
        List<ElementSymbol> symbols = new ArrayList<ElementSymbol>();
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e1")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e2")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e3")); //$NON-NLS-1$ //$NON-NLS-2$
        symbols.add(TestElementImpl.helpExample("vm1.g1", "e4")); //$NON-NLS-1$ //$NON-NLS-2$
        return new org.teiid.query.sql.lang.GroupBy(symbols);
    }

    public static org.teiid.query.sql.lang.GroupBy helpExampleWithFunctions() {
        List<Expression> symbols = new ArrayList<Expression>();

        ElementSymbol e1 = TestElementImpl.helpExample("vm1.g1", "e1");//$NON-NLS-1$ //$NON-NLS-2$
        Function f = new Function("length", new Expression[] { e1 } );//$NON-NLS-1$

        symbols.add(e1);
        symbols.add(f);
        return new org.teiid.query.sql.lang.GroupBy(symbols);
    }


    public static GroupBy example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetElements() throws Exception {
        GroupBy gb = example();
        assertNotNull(gb.getElements());
        assertEquals(4, gb.getElements().size());
        for (Iterator i = gb.getElements().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof ColumnReference);
        }
    }

    public void testTranslateWithFunction() throws Exception {
        TstLanguageBridgeFactory.factory.translate(helpExampleWithFunctions());
    }

}
