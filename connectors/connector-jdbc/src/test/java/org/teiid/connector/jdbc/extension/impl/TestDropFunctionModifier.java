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

package org.teiid.connector.jdbc.extension.impl;

import java.util.Arrays;

import org.teiid.connector.jdbc.translator.DropFunctionModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILiteral;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;

/**
 */
public class TestDropFunctionModifier extends TestCase {

    /**
     * Constructor for TestDropFunctionModifier.
     * @param name
     */
    public TestDropFunctionModifier(String name) {
        super(name);
    }

    public void testDrop() {
        DropFunctionModifier mod = new DropFunctionModifier();
    
        ILiteral arg1 = CommandBuilder.getLanuageFactory().createLiteral(new Integer(5), Integer.class);
        ILiteral arg2 = CommandBuilder.getLanuageFactory().createLiteral("string", String.class);//$NON-NLS-1$
        IFunction func = CommandBuilder.getLanuageFactory().createFunction("convert", Arrays.asList(arg1, arg2), Integer.class); //$NON-NLS-1$
                
        IExpression output = mod.modify(func);
        assertEquals("Did not get expected function after using drop modifier", arg1, output); //$NON-NLS-1$
    }
    
    /**
     * In SQL Server convert(), the type arg is the first arg, and the column name
     * is the second arg.  DropFunctionModifier needs to be able to handle the column name
     * arg being in different indices.
     */
    public void testDrop2() {
        DropFunctionModifier mod = new DropFunctionModifier();
        mod.setReplaceIndex(1);
    
        ILiteral arg1 = CommandBuilder.getLanuageFactory().createLiteral("string", String.class);//$NON-NLS-1$
        ILiteral arg2 = CommandBuilder.getLanuageFactory().createLiteral(new Integer(5), Integer.class);
        IFunction func = CommandBuilder.getLanuageFactory().createFunction("convert", Arrays.asList(arg1, arg2), Integer.class); //$NON-NLS-1$
                
        IExpression output = mod.modify(func);
        assertEquals("Did not get expected function after using drop modifier", arg2, output); //$NON-NLS-1$
    }
    
}
