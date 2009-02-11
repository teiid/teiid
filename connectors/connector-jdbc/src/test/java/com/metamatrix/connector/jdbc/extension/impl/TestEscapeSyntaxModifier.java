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

package com.metamatrix.connector.jdbc.extension.impl;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILiteral;

/**
 */
public class TestEscapeSyntaxModifier extends TestCase {

    /**
     * Constructor for TestDropFunctionModifier.
     * @param name
     */
    public TestEscapeSyntaxModifier(String name) {
        super(name);
    }

    public void testEscape() {
        EscapeSyntaxModifier mod = new EscapeSyntaxModifier();
    
        ILiteral arg1 = CommandBuilder.getLanuageFactory().createLiteral("arg1", String.class); //$NON-NLS-1$
        ILiteral arg2 = CommandBuilder.getLanuageFactory().createLiteral("arg2", String.class);//$NON-NLS-1$
        IFunction func = CommandBuilder.getLanuageFactory().createFunction("concat", new IExpression[] { arg1, arg2}, Integer.class); //$NON-NLS-1$
                
        func = (IFunction) mod.modify(func);
        List parts = mod.translate(func);
        
        List expected = Arrays.asList(new Object[] { "{fn ", "concat", "(", arg1, ", ", arg2, ")", "}"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        assertEquals(expected, parts);
    }
    
}
