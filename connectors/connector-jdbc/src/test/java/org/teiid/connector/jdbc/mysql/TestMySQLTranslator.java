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

package org.teiid.connector.jdbc.mysql;

import java.util.Map;
import java.util.Properties;

import org.teiid.connector.jdbc.MetadataFactory;
import org.teiid.connector.jdbc.mysql.MySQLTranslator;
import org.teiid.connector.jdbc.translator.TranslatedCommand;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.language.ICommand;

/**
 */
public class TestMySQLTranslator extends TestCase {

    private static Map MODIFIERS;
    private static MySQLTranslator TRANSLATOR; 
    
    static {
        try {
            TRANSLATOR = new MySQLTranslator();        
            TRANSLATOR.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
            MODIFIERS = TRANSLATOR.getFunctionModifiers();
        } catch(ConnectorException e) {
            e.printStackTrace();    
        }
    }

    public TestMySQLTranslator(String name) {
        super(name);
    }

    private String getTestVDB() {
        return MetadataFactory.PARTS_VDB;
    }
    
    private String getTestBQTVDB() {
        return MetadataFactory.BQT_VDB; 
    }
    
    public void helpTestVisitor(String vdb, String input, Map modifiers, String expectedOutput) throws ConnectorException {
        // Convert from sql to objects
        ICommand obj = MetadataFactory.helpTranslate(vdb, input);
        
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), TRANSLATOR);
        tc.translateCommand(obj);
        
        
        // Check stuff
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    public void testRewriteConversion1() throws Exception {
        String input = "SELECT char(convert(PART_WEIGHT, integer) + 100) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT char((convert(PARTS.PART_WEIGHT, SIGNED INTEGER) + 100)) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
          
    public void testRewriteConversion2() throws Exception {
        String input = "SELECT convert(PART_WEIGHT, long) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(PARTS.PART_WEIGHT, SIGNED) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
          
    public void testRewriteConversion3() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, long), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(convert(PARTS.PART_WEIGHT, SIGNED), CHAR) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
          
    public void testRewriteConversion4() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, date), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT date_format(DATE(PARTS.PART_WEIGHT), '%Y-%m-%d') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteConversion5() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, time), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT date_format(TIME(PARTS.PART_WEIGHT), '%H:%i:%S') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteConversion6() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, timestamp), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT date_format(TIMESTAMP(PARTS.PART_WEIGHT), '%Y-%m-%d %H:%i:%S.%f') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteConversion8() throws Exception {
        String input = "SELECT ifnull(PART_WEIGHT, 'otherString') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT ifnull(PARTS.PART_WEIGHT, 'otherString') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteConversion7() throws Exception {
        String input = "SELECT convert(convert(PART_WEIGHT, integer), string) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT convert(convert(PARTS.PART_WEIGHT, SIGNED INTEGER), CHAR) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteInsert() throws Exception {
        String input = "SELECT insert(PART_WEIGHT, 1, 5, 'chimp') FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT insert(PARTS.PART_WEIGHT, 1, 5, 'chimp') FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteLocate() throws Exception {
        String input = "SELECT locate(PART_WEIGHT, 'chimp', 1) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT locate(PARTS.PART_WEIGHT, 'chimp', 1) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteSubstring1() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_WEIGHT, 1) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteSubstring2() throws Exception {
        String input = "SELECT substring(PART_WEIGHT, 1, 5) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT substring(PARTS.PART_WEIGHT, 1, 5) FROM PARTS";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    public void testRewriteUnionWithOrderBy() throws Exception {
        String input = "SELECT PART_ID FROM PARTS UNION SELECT PART_ID FROM PARTS ORDER BY PART_ID"; //$NON-NLS-1$
        String output = "(SELECT PARTS.PART_ID FROM PARTS) UNION (SELECT PARTS.PART_ID FROM PARTS) ORDER BY PART_ID";  //$NON-NLS-1$

        helpTestVisitor(getTestVDB(),
            input, 
            MODIFIERS,
            output);
    }
    
    public void testRowLimit2() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 100"; //$NON-NLS-1$
               
        helpTestVisitor(getTestBQTVDB(),
            input, 
            MODIFIERS,
            output);        
    }
    public void testRowLimit3() throws Exception {
        String input = "select intkey from bqt1.smalla limit 50, 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 50, 100"; //$NON-NLS-1$
               
        helpTestVisitor(getTestBQTVDB(),
            input, 
            MODIFIERS,
            output);        
    }
          
}
