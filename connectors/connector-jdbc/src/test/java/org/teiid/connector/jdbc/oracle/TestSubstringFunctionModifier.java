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
import java.util.Properties;

import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.oracle.OracleSQLTranslator;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.jdbc.translator.SQLConversionVisitor;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;

/**
 */
public class TestSubstringFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();


    /**
     * Constructor for TestSubstringFunctionModifier.
     * @param name
     */
    public TestSubstringFunctionModifier(String name) {
        super(name);
    }

    public IExpression helpTestMod(IExpression[] args, String expectedStr) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("substring",  //$NON-NLS-1$
            Arrays.asList(args), TypeFacility.RUNTIME_TYPES.STRING);
        
        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));

        IExpression expr = ((FunctionModifier)trans.getFunctionModifiers().get("substring")).modify(func); //$NON-NLS-1$
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        
        assertEquals(expectedStr, sqlVisitor.toString());
        
        return expr;
    }

    public void testTwoArgs() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral(new Integer(1), Integer.class)           
        }; 
        helpTestMod(args, "substr('a.b.c', 1)"); //$NON-NLS-1$
    }

    public void testThreeArgsWithConstant() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral(new Integer(3), Integer.class),
            LANG_FACTORY.createLiteral(new Integer(1), Integer.class) 
        }; 
        helpTestMod(args, "substr('a.b.c', 3, 1)"); //$NON-NLS-1$
    }

    public void testThreeArgsWithElement() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), //$NON-NLS-1$
            LANG_FACTORY.createElement("e1", null, null, Integer.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral(new Integer(1), Integer.class) 
        }; 
        helpTestMod(args, "substr('a.b.c', e1, 1)"); //$NON-NLS-1$
    }

    public void testThreeArgsWithNull() throws Exception {
        IExpression[] args = new IExpression[] {
            LANG_FACTORY.createLiteral("a.b.c", String.class), //$NON-NLS-1$
            LANG_FACTORY.createLiteral(null, Integer.class),
            LANG_FACTORY.createLiteral(new Integer(5), Integer.class) 
        }; 
        helpTestMod(args, "substr('a.b.c', NULL, 5)"); //$NON-NLS-1$
    }

}
