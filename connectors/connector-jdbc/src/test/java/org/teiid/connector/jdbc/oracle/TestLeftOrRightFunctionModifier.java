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

import junit.framework.TestCase;

import org.teiid.connector.jdbc.translator.SQLConversionVisitor;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;

/**
 */
public class TestLeftOrRightFunctionModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestHourFunctionModifier.
     * @param name
     */
    public TestLeftOrRightFunctionModifier(String name) {
        super(name);
    }

    public IExpression helpTestMod(ILiteral c, ILiteral d, String target, String expectedStr) throws Exception {
        IFunction func = LANG_FACTORY.createFunction(target,
            Arrays.asList( c, d ),
            String.class);
        
        LeftOrRightFunctionModifier mod = new LeftOrRightFunctionModifier (LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        //System.out.println(" expected: " + expectedStr + " \t actual: " + sqlVisitor.toString());
        assertEquals(expectedStr, sqlVisitor.toString());
        
        return expr;
    }

    public void test1() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral("1234214", String.class); //$NON-NLS-1$
        ILiteral count = LANG_FACTORY.createLiteral(new Integer(11), Integer.class);
        helpTestMod(arg1, count, "left", //$NON-NLS-1$
            "SUBSTR('1234214', 1, 11)"); //$NON-NLS-1$
    }
    
    public void test2() throws Exception {
        ILiteral arg1 = LANG_FACTORY.createLiteral("1234214", String.class); //$NON-NLS-1$
        ILiteral count = LANG_FACTORY.createLiteral(new Integer(2), Integer.class);
        helpTestMod(arg1, count, "right", //$NON-NLS-1$
            "SUBSTR('1234214', (-1 * 2))"); //$NON-NLS-1$
    }
}