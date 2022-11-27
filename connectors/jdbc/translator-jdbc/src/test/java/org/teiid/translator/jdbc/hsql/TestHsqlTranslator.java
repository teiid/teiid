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

package org.teiid.translator.jdbc.hsql;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestHsqlTranslator {

    private static HsqlExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new HsqlExecutionFactory();
        TRANSLATOR.start();
    }

    @Ignore("the hibernate dialect has the version set reflectively so we can't set version 2")
    @Test public void testTempTable() throws Exception {
        assertEquals("declare local temporary table foo (COL1 integer, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
    }

    @Test public void testVarcharCast() throws Exception {
        String input = "select cast(SmallA.IntKey as varchar) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT cast(SmallA.IntKey AS varchar(4000)) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testSelectWithoutFrom() throws Exception {
        String input = "select 1"; //$NON-NLS-1$
        String output = "VALUES(1)";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testJoinNesting() throws Exception {
        String input = "select a.intkey from (BQT1.Smalla a left outer join bqt1.smallb b on a.intkey = b.intkey) inner join (bqt1.mediuma ma inner join bqt1.mediumb mb on mb.intkey = ma.intkey) on a.intkey = mb.intkey"; //$NON-NLS-1$
        String output = "SELECT a.IntKey FROM (SmallA AS a LEFT OUTER JOIN SmallB AS b ON a.IntKey = b.IntKey) INNER JOIN (MediumA AS ma INNER JOIN MediumB AS mb ON mb.IntKey = ma.IntKey) ON a.IntKey = mb.IntKey";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

}
