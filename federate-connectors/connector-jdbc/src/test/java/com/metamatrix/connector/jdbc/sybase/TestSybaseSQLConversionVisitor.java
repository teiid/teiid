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

package com.metamatrix.connector.jdbc.sybase;

import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.MetadataFactory;
import com.metamatrix.connector.jdbc.extension.TranslatedCommand;
import com.metamatrix.connector.jdbc.util.FunctionReplacementVisitor;
import com.metamatrix.connector.language.ICommand;

/**
 */
public class TestSybaseSQLConversionVisitor extends TestCase {

    private static Map MODIFIERS;
    
    static {
        SybaseSQLTranslator trans = new SybaseSQLTranslator();
        
        try {
            trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        } catch(ConnectorException e) {
            e.printStackTrace();
        }
        
        MODIFIERS = trans.getFunctionModifiers();
    }
    /**
     * Constructor for TestSqlServerConversionVisitor.
     * @param name
     */
    public TestSybaseSQLConversionVisitor(String name) {
        super(name);
    }

    public String getTestVDB() {
        return MetadataFactory.PARTS_VDB;
    }

    public String getBQTVDB() {
        return MetadataFactory.BQT_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, Map modifiers, int expectedType, String expectedOutput) {
        // Convert from sql to objects
        ICommand obj = MetadataFactory.helpTranslate(vdb, input);
        
        // Apply function replacement
        FunctionReplacementVisitor funcVisitor = new FunctionReplacementVisitor(modifiers);
        
        // Convert back to SQL
        SybaseSQLConversionVisitor sqlVisitor = new SybaseSQLConversionVisitor();      
        sqlVisitor.setFunctionModifiers(modifiers);  
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), new SybaseSQLTranslator(), sqlVisitor, funcVisitor);
		try {
			tc.translateCommand(obj);
		} catch (ConnectorException e) {
			throw new RuntimeException(e);
		}
        
        // Check stuff
//        System.out.println("in: " + input); //$NON-NLS-1$
        //System.out.println("out: " + tc.getSql()); //$NON-NLS-1$
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
        assertEquals("Did not get expected command type", expectedType, tc.getExecutionType());         //$NON-NLS-1$
    }

    public void testModFunction() {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        //String output = "SELECT (PARTS.PART_ID % 13) FROM PARTS";  //$NON-NLS-1$
        String output = "SELECT (convert(int, PARTS.PART_ID) % 13) FROM PARTS";  //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    } 

    public void testConcatFunction() {
        String input = "SELECT concat(part_name, 'b') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }    

    public void testConcatOperatorFunction() {
        String input = "SELECT PART_NAME || 'b' FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);    
    }    

    public void testChrFunction() {
        String input = "SELECT chr(CONVERT(PART_ID, INTEGER)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT char(convert(int, PARTS.PART_ID)) FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);    
    }    

    public void testLcaseFunction() {
        String input = "SELECT lcase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT lower(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testUcaseFunction() {
        String input = "SELECT ucase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT upper(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
     
    public void testLengthFunction() {
        String input = "SELECT length(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT char_length(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }

    public void testSubstring2ArgFunction() {
        String input = "SELECT substring(PART_NAME, 3) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, char_length(PARTS.PART_NAME)) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }

    public void testSubstring3ArgFunction() {
        String input = "SELECT substring(PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testConvertFunctionInteger() {
        String input = "SELECT convert(PARTS.PART_ID, integer) FROM PARTS"; //$NON-NLS-1$
        //String output = "SELECT PARTS.PART_ID FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(int, PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testConvertFunctionChar() {
        String input = "SELECT convert(PART_NAME, char) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(char, PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testConvertFunctionBoolean() {
        String input = "SELECT convert(PART_ID, boolean) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(bit, PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }

    public void testIfNullFunction() {
        String input = "SELECT ifnull(PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT isnull(PARTS.PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }    

    public void testNvlFunction() {
        String input = "SELECT nvl(PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT isnull(PARTS.PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }    

    public void testDateLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {d'2002-12-31'} FROM parts", //$NON-NLS-1$
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            "SELECT {d'2002-12-31'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testTimeLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {t'13:59:59'} FROM parts", //$NON-NLS-1$
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            "SELECT {t'13:59:59'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testTimestampLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {ts'2002-12-31 13:59:59'} FROM parts", //$NON-NLS-1$
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            "SELECT {ts'2002-12-31 13:59:59.0'} FROM PARTS"); //$NON-NLS-1$
    }
    
    public void testDefect12120() {
        helpTestVisitor(getBQTVDB(),
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE BQT1.SmallA.BooleanValue IN ({b'false'}, {b'true'}) ORDER BY IntKey", //$NON-NLS-1$
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            "SELECT SmallA.IntKey FROM SmallA WHERE SmallA.BooleanValue IN (0, 1) ORDER BY IntKey"); //$NON-NLS-1$
                
    }

	public void helpTestVisitor(String vdb, String input, String expectedOutput) throws ConnectorException {
		helpTestVisitor(vdb, input, MODIFIERS, TranslatedCommand.EXEC_TYPE_QUERY, expectedOutput);
		
	}
}
