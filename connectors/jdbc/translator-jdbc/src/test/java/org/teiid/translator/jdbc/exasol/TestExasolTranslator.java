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
package org.teiid.translator.jdbc.exasol;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestExasolTranslator {

    private static ExasolExecutionFactory translator;

    @BeforeClass
    public static void setupOnce() throws Exception {
        translator = new ExasolExecutionFactory();
        translator.start();
    }

    public void helpTestVisitor(String input, String expectedOutput) throws TranslatorException {
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, expectedOutput, translator);
    }

    @Test
    public void testConvertBooleanToDouble() throws TranslatorException {
        String input = "SELECT convert(BooleanValue, double) FROM BQT1.SmallA";
        String output = "SELECT cast(SmallA.BooleanValue as double precision) FROM SmallA";
        helpTestVisitor(input, output);
    }

    @Test
    public void testConvertBooleanToString() throws TranslatorException {
        String input = "SELECT convert(BooleanValue, byte) FROM BQT1.SmallA";
        String output = "SELECT cast(SmallA.BooleanValue as decimal(3)) FROM SmallA";
        helpTestVisitor(input, output);
    }
}
