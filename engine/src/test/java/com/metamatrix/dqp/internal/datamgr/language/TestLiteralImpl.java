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

import com.metamatrix.query.sql.symbol.Constant;

import junit.framework.TestCase;

public class TestLiteralImpl extends TestCase {

    /**
     * Constructor for TestLiteralImpl.
     * @param name
     */
    public TestLiteralImpl(String name) {
        super(name);
    }

    public static Constant helpExample(int val) {
        return new Constant(new Integer(val));
    }
    
    public static Constant helpExample(Object val) {
        return new Constant(val);
    }
    
    public static LiteralImpl example(int val) {
        Constant c = helpExample(val);
        return new LiteralImpl(c.getValue(), c.getType());
    }

    public static LiteralImpl example(Object val) {
        Constant c = helpExample(val);
        return new LiteralImpl(c.getValue(), c.getType());
    }

    public void testGetValue() {
        assertEquals(new Integer(100), example(100).getValue());
    }

}
