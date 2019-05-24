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

package org.teiid.translator.jdbc.derby;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestDerbySQLTranslator {

    private static DerbyExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new DerbyExecutionFactory();
        TRANSLATOR.setDatabaseVersion(Version.DEFAULT_VERSION);
        TRANSLATOR.start();
    }

    @Test
    public void testConcat_useLiteral() throws Exception {
        String input = "select concat(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT {fn concat(SmallA.StringNum, '_xx')} FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testConcat() throws Exception {
        String input = "select concat(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT {fn concat(SmallA.StringNum, SmallA.StringNum)} FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testConcat2_useLiteral() throws Exception {
        String input = "select concat2(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT {fn concat(coalesce(SmallA.StringNum, ''), '_xx')} FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testConcat2() throws Exception {
        String input = "select concat2(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN SmallA.StringNum IS NULL AND SmallA.StringNum IS NULL THEN NULL ELSE {fn concat(coalesce(SmallA.StringNum, ''), coalesce(SmallA.StringNum, ''))} END FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test
    public void testSelectWithNoFrom() throws Exception {
        String input = "select 1, 2"; //$NON-NLS-1$
        String output = "VALUES(1, 2)";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testTempTable() throws Exception {
        assertEquals("declare global temporary table foo (COL1 integer, COL2 varchar(100)) not logged", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
    }

    @Test public void testXmlSelect() throws Exception {
        String input = "SELECT col as x, col1 as y from test"; //$NON-NLS-1$
        String output = "SELECT XMLSERIALIZE(test.col AS CLOB) AS x, test.col1 AS y FROM test";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor("create foreign table test (col xml, col1 integer);",
                input, output,
                TRANSLATOR);

        input = "SELECT * from test"; //$NON-NLS-1$
        output = "SELECT XMLSERIALIZE(test.col AS CLOB), test.col1 FROM test";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor("create foreign table test (col xml, col1 integer);",
                input, output,
                TRANSLATOR);
    }

    @Test
    public void testOffset() throws Exception {
        String input = "select intkey from bqt1.smalla limit 50, 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA OFFSET 50 ROWS FETCH FIRST 100 ROWS ONLY"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

}
