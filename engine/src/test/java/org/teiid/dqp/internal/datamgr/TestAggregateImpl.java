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

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.AggregateFunction;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;

import junit.framework.TestCase;


public class TestAggregateImpl extends TestCase {

    /**
     * Constructor for TestAggregateImpl.
     * @param name
     */
    public TestAggregateImpl(String name) {
        super(name);
    }

    public static AggregateFunction example(String functionName, boolean distinct, int value) throws Exception {
        AggregateSymbol symbol = new AggregateSymbol(functionName,
                                                     distinct,
                                                     new Constant(new Integer(value)));
        return TstLanguageBridgeFactory.factory.translate(symbol);

    }

    public void testGetName() throws Exception {
        assertEquals(AggregateFunction.COUNT, example(NonReserved.COUNT, true, 42).getName());
    }

    public void testUserDefinedName() throws Exception {
        assertEquals("FIRST_VALUE", example("FIRST_VALUE", true, 42).getName()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIsDistinct() throws Exception {
        assertTrue(example(NonReserved.COUNT, true, 42).isDistinct());
        assertFalse(example(NonReserved.COUNT, false, 42).isDistinct());
    }

    public void testGetExpression() throws Exception {
        AggregateFunction agg = example(NonReserved.COUNT, true, 42);
        assertNotNull(agg.getExpression());
        assertTrue(agg.getExpression() instanceof Literal);
        assertEquals(new Integer(42), ((Literal)agg.getExpression()).getValue());
    }

    public void testGetType() throws Exception {
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, example(NonReserved.COUNT, true, 42).getType());
    }

}
