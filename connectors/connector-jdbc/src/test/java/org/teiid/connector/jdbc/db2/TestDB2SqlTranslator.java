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

package org.teiid.connector.jdbc.db2;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.language.ICommand;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;
import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestDB2SqlTranslator {

    private static DB2SQLTranslator TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws ConnectorException {
        TRANSLATOR = new DB2SQLTranslator();        
        TRANSLATOR.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
    }
    
    public String getTestVDB() {
        return UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb"; //$NON-NLS-1$
    }
    
    public void helpTestVisitor(TranslationUtility util, String input, String expectedOutput) throws ConnectorException {
        // Convert from sql to objects
        ICommand obj = util.parseCommand(input);
        
        ExecutionContext context = EnvironmentUtility.createSecurityContext("user"); //$NON-NLS-1$
                
        TranslatedCommand tc = new TranslatedCommand(context, TRANSLATOR);
        tc.translateCommand(obj);
        
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    @Test
    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA FETCH FIRST 100 ROWS ONLY";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
            input, 
            output);
    }
    
    @Test
    public void testCrossJoin() throws Exception{
        String input = "SELECT bqt1.smalla.stringkey FROM bqt1.smalla cross join bqt1.smallb"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA INNER JOIN SmallB ON 1 = 1";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
            input, 
            output);
    }
    
    @Test
    public void testConcat2_useLiteral() throws Exception {
        String input = "select concat2(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT concat(coalesce(SmallA.StringNum, ''), '_xx') FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input, 
                output);

    }

    @Test
    public void testConcat2() throws Exception {
        String input = "select concat2(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT CASE WHEN SmallA.StringNum IS NULL THEN NULL ELSE concat(coalesce(SmallA.StringNum, ''), coalesce(SmallA.StringNum, '')) END FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input, 
                output);
    }    
    
    @Test
    public void testSelectNullLiteral() throws Exception {
        String input = "select null + 1 as x, null || 'a' from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT CAST(NULL AS INTEGER) AS x, CAST(NULL AS CHAR) FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input, 
                output);
    }    


}
