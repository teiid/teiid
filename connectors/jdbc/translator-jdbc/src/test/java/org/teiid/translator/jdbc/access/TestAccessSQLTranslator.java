/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.jdbc.access;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.TranslatedCommand;



/**
 * @since 4.3
 */
public class TestAccessSQLTranslator extends TestCase {

    private static JDBCExecutionFactory TRANSLATOR;

    static {
        try {
            TRANSLATOR = new AccessExecutionFactory();
            TRANSLATOR.start();
        } catch(TranslatorException e) {
            e.printStackTrace();
        }
    }

    public void helpTestVisitor(String input, String expectedOutput) throws TranslatorException {
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
