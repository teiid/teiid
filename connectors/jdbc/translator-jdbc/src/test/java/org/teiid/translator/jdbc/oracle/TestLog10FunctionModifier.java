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

package org.teiid.translator.jdbc.oracle;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.translator.jdbc.oracle.Log10FunctionModifier;

/**
 */
public class TestLog10FunctionModifier extends TestCase {
	private static final LanguageFactory LANG_FACTORY = new LanguageFactory();
	
    /**
     * Constructor for TestLog10FunctionModifier.
     * @param name
     */
    public TestLog10FunctionModifier(String name) {
        super(name);
    }

    public void testModifier() {
        Literal arg = LANG_FACTORY.createLiteral(new Double(5.2), Double.class);
        Function func = LANG_FACTORY.createFunction("log10", Arrays.asList(arg), Double.class); //$NON-NLS-1$
        
        Log10FunctionModifier modifier = new Log10FunctionModifier(LANG_FACTORY);
        modifier.translate(func);
        
        assertEquals("log", func.getName()); //$NON-NLS-1$
        assertEquals(Double.class, func.getType());
        
        List<Expression> outArgs = func.getParameters();
        assertEquals(2, outArgs.size());
        assertEquals(arg, outArgs.get(1));
        
        assertTrue(outArgs.get(1) instanceof Literal);
        Literal newArg = (Literal) outArgs.get(0);
        assertEquals(Integer.class, newArg.getType());
        assertEquals(new Integer(10), newArg.getValue());
        
        assertEquals("log(10, 5.2)", SQLStringVisitor.getSQLString(func));              //$NON-NLS-1$
    }
}
