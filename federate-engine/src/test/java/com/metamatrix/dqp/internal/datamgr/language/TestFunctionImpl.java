/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr.language;

import com.metamatrix.connector.language.IExpression;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;

import junit.framework.TestCase;

public class TestFunctionImpl extends TestCase {

    /**
     * Constructor for TestFunctionImpl.
     * @param name
     */
    public TestFunctionImpl(String name) {
        super(name);
    }

    public static Function helpExample(String name) {
        Constant c1 = new Constant(new Integer(100));
        Constant c2 = new Constant(new Integer(200));
        Function f = new Function(name, new Expression [] {c1, c2});
        f.setType(Integer.class);
        return f;
    }
    
    public static FunctionImpl example(String name) throws Exception {
        return (FunctionImpl)TstLanguageBridgeFactory.factory.translate(helpExample(name));
    }

    public void testGetName() throws Exception {
        assertEquals("testName", example("testName").getName()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testGetParameters() throws Exception {
        IExpression[] params = example("testFunction").getParameters(); //$NON-NLS-1$
        assertNotNull(params);
        assertEquals(2, params.length);
        for (int i = 0; i < params.length; i++) {
            assertNotNull(params[i]);
        }
    }

    public void testGetType() throws Exception {
        assertEquals(Integer.class, example("test").getType()); //$NON-NLS-1$
    }

}
