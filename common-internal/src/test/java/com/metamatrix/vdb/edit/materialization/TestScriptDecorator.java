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

package com.metamatrix.vdb.edit.materialization;

import com.metamatrix.vdb.materialization.DatabaseDialect;
import com.metamatrix.vdb.materialization.ScriptDecorator;

import junit.framework.TestCase;


public class TestScriptDecorator extends TestCase {

    public void test1() {
        ScriptDecorator decorator = new ScriptDecorator();
        String output = new String(decorator.modifyDDL(DatabaseDialect.DB2.getType(), "(a)%\n(b)%".getBytes())); //$NON-NLS-1$
        assertEquals("(a) NOT LOGGED INITIALLY%\n(b) NOT LOGGED INITIALLY%", output); //$NON-NLS-1$
    }
}
