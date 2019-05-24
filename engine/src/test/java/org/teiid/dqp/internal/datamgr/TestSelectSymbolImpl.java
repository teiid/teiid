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
import java.util.List;

import junit.framework.TestCase;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Literal;
import org.teiid.language.Select;


public class TestSelectSymbolImpl extends TestCase {

    /**
     * Constructor for TestSelectSymbolImpl.
     * @param name
     */
    public TestSelectSymbolImpl(String name) {
        super(name);
    }

    public static DerivedColumn example(String symbolName, String alias) throws Exception {
        DerivedColumn selectSymbol = new DerivedColumn(alias, new ColumnReference(null, symbolName, null, DataTypeManager.DefaultDataClasses.INTEGER));
        return selectSymbol;
    }

    public void testHasAlias() throws Exception {
        assertNotNull(example("testName", "testAlias").getAlias()); //$NON-NLS-1$ //$NON-NLS-2$
        assertNull(example("testName", null).getAlias()); //$NON-NLS-1$
    }

    public void testGetOutputName() throws Exception {
        assertEquals("testAlias", example("testName", "testAlias").getAlias()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testGetExpression() throws Exception {
        assertNotNull(example("testName", null).getExpression()); //$NON-NLS-1$
    }

    public void testGetColumnDataTypes(){
        Class<?>[] expectedResults = new Class[2];
        List<DerivedColumn> symbols = new ArrayList<DerivedColumn>();
        symbols.add(new DerivedColumn("c1", new Literal("3", DataTypeManager.DefaultDataClasses.STRING)));  //$NON-NLS-1$//$NON-NLS-2$
        expectedResults[0] = DataTypeManager.DefaultDataClasses.STRING;
        symbols.add(new DerivedColumn("c2", new Literal(new Integer(5), DataTypeManager.DefaultDataClasses.INTEGER)));  //$NON-NLS-1$
        expectedResults[1] = DataTypeManager.DefaultDataClasses.INTEGER;
        Select query = new Select(symbols, false, null, null, null, null, null);
        Class<?>[] results = query.getColumnTypes();
        assertEquals( results[0], expectedResults[0]);
        assertEquals( results[1], expectedResults[1]);
    }

}
