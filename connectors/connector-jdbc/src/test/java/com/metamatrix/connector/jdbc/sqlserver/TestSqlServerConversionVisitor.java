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

package com.metamatrix.connector.jdbc.sqlserver;

import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.MetadataFactory;
import com.metamatrix.connector.jdbc.extension.TranslatedCommand;
import com.metamatrix.connector.jdbc.util.FunctionReplacementVisitor;
import com.metamatrix.connector.language.ICommand;

/**
 */
public class TestSqlServerConversionVisitor extends TestCase {

    private static final Properties EMPTY_PROPERTIES = new Properties();
    private static Map MODIFIERS;
    
    static {
        SqlServerSQLTranslator trans = new SqlServerSQLTranslator();
        
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
    public TestSqlServerConversionVisitor(String name) {
        super(name);
    }

    public String getTestVDB() {
        return MetadataFactory.PARTS_VDB;
    }

    public String getBQTVDB() {
        return MetadataFactory.BQT_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, Map modifiers, int expectedType, String expectedOutput) throws ConnectorException {
		helpTestVisitor(vdb, input, modifiers, expectedType, new String[] {expectedOutput}, EMPTY_PROPERTIES);
    }
    
    public void helpTestVisitor(String vdb, String input, Map modifiers, int expectedType, String[] expectedOutputs, Properties props) throws ConnectorException {
        // Convert from sql to objects
        ICommand obj = MetadataFactory.helpTranslate(vdb, input);
        
        // Apply function replacement
        FunctionReplacementVisitor funcVisitor = new FunctionReplacementVisitor(modifiers);
        
        // Convert back to SQL
        SqlServerSQLConversionVisitor sqlVisitor = new SqlServerSQLConversionVisitor();
        sqlVisitor.setProperties(props);
        sqlVisitor.setLanguageFactory(CommandBuilder.getLanuageFactory());
        sqlVisitor.setFunctionModifiers(modifiers);  
        
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), new SqlServerSQLTranslator(), sqlVisitor, funcVisitor); //$NON-NLS-1$
        tc.translateCommand(obj);
        
        // Check stuff

        assertEquals("Did not get correct sql", expectedOutputs[0], tc.getSql());             //$NON-NLS-1$
        assertEquals("Did not get expected command type", expectedType, tc.getExecutionType());         //$NON-NLS-1$
    }

    public void testModFunction() throws Exception {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        String output = "SELECT (convert(int, PARTS.PART_ID) % 13) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    } 

    public void testConcatFunction() throws Exception {
        String input = "SELECT concat(part_name, 'b') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }    

    public void testConcatOperatorFunction() throws Exception {
        String input = "SELECT PART_NAME || 'b' FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);    
    }    

    public void testChrFunction() throws Exception {
        String input = "SELECT chr(CONVERT(PART_ID, INTEGER)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT char(convert(int, PARTS.PART_ID)) FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);    
    }    

    public void testLcaseFunction() throws Exception {
        String input = "SELECT lcase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT lower(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testUcaseFunction() throws Exception {
        String input = "SELECT ucase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT upper(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
     
    public void testLengthFunction() throws Exception {
        String input = "SELECT length(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT len(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }

    public void testDayOfMonthFunction() throws Exception {
        String input = "SELECT dayofmonth(convert(PARTS.PART_ID, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT day(convert(datetime, PARTS.PART_ID)) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }

    public void testSubstring2ArgFunction() throws Exception {
        String input = "SELECT substring(PART_NAME, 3) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, len(PARTS.PART_NAME)) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }

    public void testSubstring3ArgFunction() throws Exception {
        String input = "SELECT substring(PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testConvertFunctionString() throws Exception {
        String input = "SELECT convert(PARTS.PART_ID, integer) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(int, PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testConvertFunctionChar() throws Exception {
        String input = "SELECT convert(PART_NAME, char) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(char, PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }
    
    public void testConvertFunctionBoolean() throws Exception {
        String input = "SELECT convert(PART_ID, boolean) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(bit, PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            output);
    }

    public void testDateLiteral() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select {d'2002-12-31'} FROM parts", //$NON-NLS-1$
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            "SELECT {d'2002-12-31'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testTimeLiteral() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select {t'13:59:59'} FROM parts", //$NON-NLS-1$
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            "SELECT {ts'1900-01-01 13:59:59'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testTimestampLiteral() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select {ts'2002-12-31 13:59:59'} FROM parts", //$NON-NLS-1$
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY,
            "SELECT {ts'2002-12-31 13:59:59.0'} FROM PARTS"); //$NON-NLS-1$
    }

	public void helpTestVisitor(String vdb, String input, String expectedOutput) throws ConnectorException {
		helpTestVisitor(vdb, input, MODIFIERS, TranslatedCommand.EXEC_TYPE_QUERY, expectedOutput);
	}
    
    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 SmallA.IntKey FROM SmallA "; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY, output);        
    }
    
    public void testNonIntMod() throws Exception {
    	String input = "select mod(intkey/1.5, 3) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT ((convert(float, SmallA.IntKey) / 1.5) - (floor(((convert(float, SmallA.IntKey) / 1.5) / 3.0)) * 3.0)) FROM SmallA"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            MODIFIERS,
            TranslatedCommand.EXEC_TYPE_QUERY, output);
    }
}
