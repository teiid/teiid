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
package org.teiid.translator.jdbc.ucanaccess;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestUCanAccessTranslator {

    private static UCanAccessExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setup() throws TranslatorException {
        TRANSLATOR = new UCanAccessExecutionFactory();
        TRANSLATOR.start();
    }

    @Test
    public void testPushDownFuctions() throws TranslatorException {

        String input = "SELECT ucanaccess.DCount('*','T20','id > 100') FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT DCount('*', 'T20', 'id > 100') FROM MediumA"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);

        input = "SELECT ucanaccess.DSum('id','T20','id > 100')"; //$NON-NLS-1$
        output = "VALUES(DSum('id', 'T20', 'id > 100'))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);

        input = "SELECT ucanaccess.DMax('id', 'T20')"; //$NON-NLS-1$
        output = "VALUES(DMax('id', 'T20'))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);

        input = "SELECT ucanaccess.DMin('id', 'T20')"; //$NON-NLS-1$
        output = "VALUES(DMin('id', 'T20'))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);

        input = "SELECT ucanaccess.DAvg('id', 'T20')"; //$NON-NLS-1$
        output = "VALUES(DAvg('id', 'T20'))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);

        input = "SELECT ucanaccess.DFirst('descr', 'T20')"; //$NON-NLS-1$
        output = "VALUES(DFirst('descr', 'T20'))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);

        input = "SELECT ucanaccess.DLast('descr', 'T20')"; //$NON-NLS-1$
        output = "VALUES(DLast('descr', 'T20'))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

}
