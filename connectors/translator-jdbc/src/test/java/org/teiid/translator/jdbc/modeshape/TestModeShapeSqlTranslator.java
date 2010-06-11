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

package org.teiid.translator.jdbc.modeshape;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;

import com.metamatrix.cdk.api.TranslationUtility;

/**
 */
public class TestModeShapeSqlTranslator {

    private static ModeShapeExecutionFactory TRANSLATOR; 
    
    private static String MODESHAPE_VDB = (UnitTestUtil.getTestDataPath() != null ? UnitTestUtil.getTestDataPath() : "src/test/resources") + "/ModeShape.vdb";
 
    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new ModeShapeExecutionFactory();        
        TRANSLATOR.start();
        
    }
    
    
    public void helpTestVisitor(TranslationUtility util, String input, String expectedOutput) throws TranslatorException {
        // Convert from sql to objects
        Command obj = util.parseCommand(input);
        
        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), TRANSLATOR);
        tc.translateCommand(obj);
        
        System.out.println("Input: "+ tc.getSql() + "  Expected: " + expectedOutput);
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    @Test
    public void testSimpleSelect() throws Exception {        
       String input = "select Model from Car"; //$NON-NLS-1$
        String output = "SELECT [car:Model] FROM [car:Car]";  //$NON-NLS-1$
	        
	        // FakeTranslationFactory.getInstance().getExampleTranslationUtility(),
        helpTestVisitor(new TranslationUtility(MODESHAPE_VDB),
                input, 
                output);

    }
    
    @Test
    public void testWhereClause() throws Exception {

	String input = "select Model from Car WHERE Make = 'Honda'"; //$NON-NLS-1$
	String output = "SELECT [car:Model] FROM [car:Car] WHERE [car:Make] = 'Honda'"; //$NON-NLS-1$

	// FakeTranslationFactory.getInstance().getExampleTranslationUtility(),
	helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

    }

    @Test
    public void testOrderBy() throws Exception {

	String input = "select Model from Car ORDER BY Make"; //$NON-NLS-1$
	String output = "SELECT [car:Model] FROM [car:Car] ORDER BY [car:Make]"; //$NON-NLS-1$

	// FakeTranslationFactory.getInstance().getExampleTranslationUtility(),
	helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

    }

    @Ignore
    @Test
    public void testUsingAlias() throws Exception {

	String input = "select c.Model from Car As c"; //$NON-NLS-1$
	String output = "SELECT c.[car:Model] FROM [car:Car] As c"; //$NON-NLS-1$

	// FakeTranslationFactory.getInstance().getExampleTranslationUtility(),
	helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);

    }
    
    @Ignore
    @Test
    public void testUsingNameFunction() throws Exception {

	String input = "select Model from Car as car WHERE PATH('car') LIKE '%/Hybrid/%'"; //$NON-NLS-1$
	String output = "SELECT [car:Model] FROM [car:Car] WHERE PATH(car:Car) LIKE '%/Hybrid/%'"; //$NON-NLS-1$

	// FakeTranslationFactory.getInstance().getExampleTranslationUtility(),
	helpTestVisitor(new TranslationUtility(MODESHAPE_VDB), input, output);
	

    }
    


}
