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

package org.teiid.connector.jdbc.sybase;

import java.util.Properties;

import org.teiid.connector.jdbc.MetadataFactory;
import org.teiid.connector.jdbc.sybase.SybaseSQLTranslator;
import org.teiid.connector.jdbc.translator.FunctionReplacementVisitor;
import org.teiid.connector.jdbc.translator.SQLConversionVisitor;
import org.teiid.connector.jdbc.translator.TranslatedCommand;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.language.ICommand;

/**
 */
public class TestSybaseSQLConversionVisitor extends TestCase {

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
    
    public void helpTestVisitor(String vdb, String input, String expectedOutput) {
        // Convert from sql to objects
        ICommand obj = MetadataFactory.helpTranslate(vdb, input);
        
        // Apply function replacement
        SybaseSQLTranslator trans = new SybaseSQLTranslator();
        try {
			trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
		} catch (ConnectorException e1) {
			throw new RuntimeException(e1);
		}
        FunctionReplacementVisitor funcVisitor = new FunctionReplacementVisitor(trans.getFunctionModifiers());

        // Convert back to SQL
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor();     
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), trans, sqlVisitor, funcVisitor);
		try {
			tc.translateCommand(obj);
		} catch (ConnectorException e) {
			throw new RuntimeException(e);
		}
        
        // Check stuff
//        System.out.println("in: " + input); //$NON-NLS-1$
        //System.out.println("out: " + tc.getSql()); //$NON-NLS-1$
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    public void testModFunction() {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        //String output = "SELECT (PARTS.PART_ID % 13) FROM PARTS";  //$NON-NLS-1$
        String output = "SELECT (convert(int, PARTS.PART_ID) % 13) FROM PARTS";  //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    } 

    public void testConcatFunction() {
        String input = "SELECT concat(part_name, 'b') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }    

    public void testLcaseFunction() {
        String input = "SELECT lcase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT lower(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    public void testUcaseFunction() {
        String input = "SELECT ucase(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT upper(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
     
    public void testLengthFunction() {
        String input = "SELECT length(PART_NAME) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT char_length(PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }

    public void testSubstring2ArgFunction() {
        String input = "SELECT substring(PART_NAME, 3) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, char_length(PARTS.PART_NAME)) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }

    public void testSubstring3ArgFunction() {
        String input = "SELECT substring(PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_NAME, 3, 5) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    public void testConvertFunctionInteger() {
        String input = "SELECT convert(PARTS.PART_ID, integer) FROM PARTS"; //$NON-NLS-1$
        //String output = "SELECT PARTS.PART_ID FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(int, PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    public void testConvertFunctionChar() {
        String input = "SELECT convert(PART_NAME, char) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(char, PARTS.PART_NAME) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
    
    public void testConvertFunctionBoolean() {
        String input = "SELECT convert(PART_ID, boolean) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(bit, PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }

    public void testIfNullFunction() {
        String input = "SELECT ifnull(PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT isnull(PARTS.PART_NAME, 'abc') FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }    

    public void testDateLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {d'2002-12-31'} FROM parts", //$NON-NLS-1$
            "SELECT {d'2002-12-31'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testTimeLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {t'13:59:59'} FROM parts", //$NON-NLS-1$
            "SELECT {ts'1970-01-01 13:59:59'} FROM PARTS"); //$NON-NLS-1$
    }

    public void testTimestampLiteral() {
        helpTestVisitor(getTestVDB(),
            "select {ts'2002-12-31 13:59:59'} FROM parts", //$NON-NLS-1$
            "SELECT {ts'2002-12-31 13:59:59.0'} FROM PARTS"); //$NON-NLS-1$
    }
    
    public void testDefect12120() {
        helpTestVisitor(getBQTVDB(),
            "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA WHERE BQT1.SmallA.BooleanValue IN ({b'false'}, {b'true'}) ORDER BY IntKey", //$NON-NLS-1$
            "SELECT SmallA.IntKey FROM SmallA WHERE SmallA.BooleanValue IN (0, 1) ORDER BY IntKey"); //$NON-NLS-1$
                
    }
    
    public void testConvertFunctionString() throws Exception {
        String input = "SELECT convert(PARTS.PART_ID, integer) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(int, PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            output);
    }
        
    public void testNonIntMod() throws Exception {
    	String input = "select mod(intkey/1.5, 3) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT ((convert(float, SmallA.IntKey) / 1.5) - (floor(((convert(float, SmallA.IntKey) / 1.5) / 3.0)) * 3.0)) FROM SmallA"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            output);
    }

}
