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

package org.teiid.translator.jdbc.ingres;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestIngresExecutionFactory {

    private static IngresExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new IngresExecutionFactory();
        TRANSLATOR.start();
    }

    @Ignore
    @Test public void testLocate() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SmallA WHERE LOCATE(1, INTKEY) = 1 ORDER BY INTKEY"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE locate(cast(SmallA.IntKey AS varchar(4000)), '1') = 1 ORDER BY SmallA.IntKey";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testLimit() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SmallA LIMIT 1"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA FETCH FIRST 1 ROWS ONLY";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

}
