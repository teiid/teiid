/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.connector.language.IAggregate;
import org.teiid.dqp.internal.datamgr.language.AggregateImpl;
import org.teiid.dqp.internal.datamgr.language.LiteralImpl;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.Constant;

import junit.framework.TestCase;

public class TestAggregateImpl extends TestCase {

    /**
     * Constructor for TestAggregateImpl.
     * @param name
     */
    public TestAggregateImpl(String name) {
        super(name);
    }

    public static AggregateImpl example(String name, String functionName, boolean distinct, int value) throws Exception {
        AggregateSymbol symbol = new AggregateSymbol(name,
                                                     functionName,
                                                     distinct,
                                                      new Constant(new Integer(value)));
        return (AggregateImpl)TstLanguageBridgeFactory.factory.translate(symbol);
        
    }

    public void testGetName() throws Exception {
        assertEquals(IAggregate.COUNT, example("testName", ReservedWords.COUNT, true, 42).getName()); //$NON-NLS-1$ 
    }

    public void testIsDistinct() throws Exception {
        assertTrue(example("testName", ReservedWords.COUNT, true, 42).isDistinct()); //$NON-NLS-1$
        assertFalse(example("testName", ReservedWords.COUNT, false, 42).isDistinct()); //$NON-NLS-1$
    }

    public void testGetExpression() throws Exception {
        AggregateImpl agg = example("testName", ReservedWords.COUNT, true, 42); //$NON-NLS-1$
        assertNotNull(agg.getExpression());
        assertTrue(agg.getExpression() instanceof LiteralImpl);
        assertEquals(new Integer(42), ((LiteralImpl)agg.getExpression()).getValue());
    }

    public void testGetType() throws Exception {
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, example("x", ReservedWords.COUNT, true, 42).getType()); //$NON-NLS-1$
    }

}
