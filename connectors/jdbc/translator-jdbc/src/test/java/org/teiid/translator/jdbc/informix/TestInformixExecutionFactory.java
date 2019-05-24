package org.teiid.translator.jdbc.informix;
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

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestInformixExecutionFactory {

    private static InformixExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new InformixExecutionFactory();
        TRANSLATOR.start();
    }

    @Test public void testCast() throws Exception {
        String input = "SELECT cast(INTKEY as string), cast(stringkey as time), cast(stringkey as date), cast(stringkey as timestamp) FROM BQT1.SmallA"; //$NON-NLS-1$
        String output = "SELECT cast(SmallA.IntKey AS varchar(255)), cast(SmallA.StringKey AS datetime hour to second), cast(SmallA.StringKey AS date), cast(SmallA.StringKey AS datetime year to fraction(5)) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testTimeLiteral() throws Exception {
        String input = "SELECT {t '12:11:01'} FROM BQT1.SmallA"; //$NON-NLS-1$
        String output = "SELECT {t '12:11:01'} FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testMinMaxBoolean() throws Exception {
        String input = "SELECT min(booleanvalue), max(booleanvalue) FROM BQT1.SmallA"; //$NON-NLS-1$
        String output = "SELECT cast(MIN(cast(SmallA.BooleanValue as char)) as boolean), cast(MAX(cast(SmallA.BooleanValue as char)) as boolean) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

}
