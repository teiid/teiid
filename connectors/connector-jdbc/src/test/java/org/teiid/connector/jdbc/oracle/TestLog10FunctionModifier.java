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

package org.teiid.connector.jdbc.oracle;

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.jdbc.oracle.Log10FunctionModifier;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.visitor.util.SQLStringVisitor;

/**
 */
public class TestLog10FunctionModifier extends TestCase {

    /**
     * Constructor for TestLog10FunctionModifier.
     * @param name
     */
    public TestLog10FunctionModifier(String name) {
        super(name);
    }

    public void testModifier() {
        ILiteral arg = CommandBuilder.getLanuageFactory().createLiteral(new Double(5.2), Double.class);
        IFunction func = CommandBuilder.getLanuageFactory().createFunction("log10", Arrays.asList(arg), Double.class); //$NON-NLS-1$
        
        Log10FunctionModifier modifier = new Log10FunctionModifier(CommandBuilder.getLanuageFactory());
        IExpression outExpr = modifier.modify(func);
        
        assertTrue(outExpr instanceof IFunction);
        IFunction outFunc = (IFunction) outExpr;
        
        assertEquals("log", outFunc.getName()); //$NON-NLS-1$
        assertEquals(func.getType(), outFunc.getType());
        
        List<IExpression> outArgs = func.getParameters();
        assertEquals(2, outArgs.size());
        assertEquals(arg, outArgs.get(1));
        
        assertTrue(outArgs.get(1) instanceof ILiteral);
        ILiteral newArg = (ILiteral) outArgs.get(0);
        assertEquals(Integer.class, newArg.getType());
        assertEquals(new Integer(10), newArg.getValue());
        
        assertEquals("log(10, 5.2)", SQLStringVisitor.getSQLString(outFunc));              //$NON-NLS-1$
    }
}
