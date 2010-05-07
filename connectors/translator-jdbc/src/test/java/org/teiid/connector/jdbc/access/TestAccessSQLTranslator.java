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

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.language.Command;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.adapter.jdbc.JDBCExecutionFactory;
import org.teiid.resource.cci.ExecutionContext;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.teiid.translator.jdbc.Translator;
import org.teiid.translator.jdbc.access.AccessSQLTranslator;

import com.metamatrix.cdk.unittest.FakeTranslationFactory;


/** 
 * @since 4.3
 */
public class TestAccessSQLTranslator extends TestCase {

    private static Translator TRANSLATOR; 

    static {
        try {
            TRANSLATOR = new AccessSQLTranslator();        
            TRANSLATOR.initialize(Mockito.mock(JDBCExecutionFactory.class));
        } catch(ConnectorException e) {
            e.printStackTrace();    
        }
    }
    
    public void helpTestVisitor(String input, String expectedOutput) throws ConnectorException {
        // Convert from sql to objects
        Command obj = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(input);
        
        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), TRANSLATOR);
        tc.translateCommand(obj);
        
        
        // Check stuff
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT TOP 100 SmallA.IntKey FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(
            input, 
            output);

    }
    
    public void testRowLimit1() throws Exception {
        String input = "select distinct intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT DISTINCT TOP 100 SmallA.IntKey FROM SmallA";  //$NON-NLS-1$

        helpTestVisitor(
            input, 
            output);

    }
}
