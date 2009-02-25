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

package org.teiid.connector.jdbc.access;

import java.util.Properties;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.access.AccessSQLTranslator;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;


/** 
 * @since 4.3
 */
public class TestAccessSQLTranslator extends TestCase {

    private static Translator TRANSLATOR; 

    static {
        try {
            TRANSLATOR = new AccessSQLTranslator();        
            TRANSLATOR.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        } catch(ConnectorException e) {
            e.printStackTrace();    
        }
    }
    
    public void helpTestVisitor(String input, String expectedOutput) throws ConnectorException {
        // Convert from sql to objects
        ICommand obj = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(input);
        
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), TRANSLATOR);
        tc.translateCommand(obj);
        
        
        // Check stuff
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 * FROM (SELECT SmallA.IntKey FROM SmallA) AS X";  //$NON-NLS-1$

        helpTestVisitor(
            input, 
            output);

    }
    
    public void testRowLimit1() throws Exception {
        String input = "select distinct intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 * FROM (SELECT DISTINCT SmallA.IntKey FROM SmallA) AS X";  //$NON-NLS-1$

        helpTestVisitor(
            input, 
            output);

    }
}
