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

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.jdbc.MetadataFactory;
import com.metamatrix.connector.jdbc.translator.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.translator.TranslatedCommand;
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
            trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
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
    
    public void helpTestVisitor(String vdb, String input, Map modifiers, String expectedOutput) throws ConnectorException {
		helpTestVisitor(vdb, input, modifiers, new String[] {expectedOutput}, EMPTY_PROPERTIES);
    }
    
    public void helpTestVisitor(String vdb, String input, Map modifiers, String[] expectedOutputs, Properties props) throws ConnectorException {
        // Convert from sql to objects
        ICommand obj = MetadataFactory.helpTranslate(vdb, input);
        
        // Apply function replacement
        FunctionReplacementVisitor funcVisitor = new FunctionReplacementVisitor(modifiers);
        
        SqlServerSQLTranslator trans = new SqlServerSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(props, false));
        // Convert back to SQL
        SQLConversionVisitor sqlVisitor = new SQLConversionVisitor(trans);
        
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), trans, sqlVisitor, funcVisitor); //$NON-NLS-1$
        tc.translateCommand(obj);
        
        // Check stuff

        assertEquals("Did not get correct sql", expectedOutputs[0], tc.getSql());             //$NON-NLS-1$
    }

    public void testModFunction() throws Exception {
        String input = "SELECT mod(CONVERT(PART_ID, INTEGER), 13) FROM parts"; //$NON-NLS-1$
        String output = "SELECT (convert(int, PARTS.PART_ID) % 13) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    } 

    public void testConcatFunction() throws Exception {
        String input = "SELECT concat(part_name, 'b') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT (PARTS.PART_NAME + 'b') FROM PARTS"; //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }    

    public void testDayOfMonthFunction() throws Exception {
        String input = "SELECT dayofmonth(convert(PARTS.PART_ID, date)) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT day(convert(datetime, PARTS.PART_ID)) FROM PARTS"; //$NON-NLS-1$
    
        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }

    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 * FROM (SELECT SmallA.IntKey FROM SmallA) AS X"; //$NON-NLS-1$
               
        helpTestVisitor(getBQTVDB(),
            input, 
            MODIFIERS,
            output);        
    }
    
}
