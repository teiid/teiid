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
package org.teiid.translator.jdbc.redshift;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.ExecutionFactory.Format;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestRedshiftTranslator {

    private static RedshiftExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new RedshiftExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.setDatabaseVersion(RedshiftExecutionFactory.NINE_3);
        TRANSLATOR.start();
    }

    @Test public void testLocate() throws Exception {
        String input = "SELECT INTKEY, STRINGKEY FROM bqt1.SmallA WHERE LOCATE('1', STRINGKEY, 2) IN (1, 2)";
        String output = "SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA WHERE (position('1' in substring(SmallA.StringKey from 2)) + 1) IN (1, 2)";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testParseDate() throws Exception {
        String input = "SELECT INTKEY FROM bqt1.SmallA WHERE parsedate(stringkey, 'yyyy-MM dd') = {d '1999-12-01'}";
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE cast(TO_DATE(SmallA.StringKey, 'YYYY-MM DD') AS date) = DATE '1999-12-01'";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testBigDecimalCast() throws Exception {
        String input = "SELECT cast(floatnum as bigdecimal) FROM bqt1.SmallA";
        String output = "SELECT cast(SmallA.FloatNum AS decimal(38, 19)) FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testTimezoneFormat() throws Exception {
        assertFalse(TRANSLATOR.supportsFormatLiteral("hh:MM:ss Z", Format.DATE));
    }

    @Test public void testTempTable() throws Exception {
        assertEquals("create temporary  table foo (COL1 int4, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
        assertEquals("create temporary  table foo (COL1 int4, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(TRANSLATOR, false));
    }

}
