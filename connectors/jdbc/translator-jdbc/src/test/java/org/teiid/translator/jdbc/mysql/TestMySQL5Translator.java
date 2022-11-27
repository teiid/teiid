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

package org.teiid.translator.jdbc.mysql;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestMySQL5Translator {

    private static MySQL5ExecutionFactory TRANSLATOR;

    @BeforeClass public static void oneTimeSetup() throws TranslatorException {
        TRANSLATOR = new MySQL5ExecutionFactory();
        TRANSLATOR.start();
        TRANSLATOR.initCapabilities(null);
    }

    @Test public void testChar() throws Exception {
        String input = "SELECT intkey, CHR(CONVERT(bigintegervalue, integer)) FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT MediumA.IntKey, char(cast(MediumA.BigIntegerValue AS signed) USING ASCII) FROM MediumA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }


    @Test public void testTimestampFunctions() throws Exception {
        String input = "SELECT mysql.timestampdiff('SQL_TSI_FRAC_SECOND', timestampvalue, {d '1970-01-01'}), mysql.timestampdiff('SQL_TSI_HOUR', timestampvalue, {d '1970-01-01'}), timestampadd(SQL_TSI_FRAC_SECOND, 2000, MediumA.TimestampValue) FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT timestampdiff(MICROSECOND, MediumA.TimestampValue, {ts '1970-01-01 00:00:00.0'}) * 1000, timestampdiff(SQL_TSI_HOUR, MediumA.TimestampValue, {ts '1970-01-01 00:00:00.0'}), timestampadd(MICROSECOND, (2000 / 1000), MediumA.TimestampValue) FROM MediumA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testTempTable() throws Exception {
        assertEquals("create temporary table if not exists foo (COL1 integer, COL2 varchar(100)) ", TranslationHelper.helpTestTempTable(TRANSLATOR, true));
    }

    @Test public void testRollup() throws Exception {
        String input = "select intkey, max(stringkey) from bqt1.smalla group by rollup(intkey)";
        String output = "SELECT SmallA.IntKey, MAX(SmallA.StringKey) FROM SmallA GROUP BY SmallA.IntKey WITH ROLLUP";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testGeometryPushdown() throws Exception {
        String input = "select mkt_id from cola_markets where ST_Contains(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))'), shape);"; //$NON-NLS-1$
        String output = "SELECT COLA_MARKETS.MKT_ID FROM COLA_MARKETS WHERE st_contains(GeomFromWKB(?, 0), COLA_MARKETS.SHAPE) = 1"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testGeometryPushdownSrid() throws Exception {
        String input = "select mkt_id from cola_markets where ST_Contains(ST_GeomFromText('POLYGON ((40 0, 50 50, 0 50, 0 0, 40 0))', 8307), shape);"; //$NON-NLS-1$
        String output = "SELECT COLA_MARKETS.MKT_ID FROM COLA_MARKETS WHERE st_contains(GeomFromWKB(?, 8307), COLA_MARKETS.SHAPE) = 1"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testTinyintBoolean() throws Exception {
        String input = "select boolcol from x where boolcol = false"; //$NON-NLS-1$
        String output = "SELECT case when x.boolcol is null then null when x.boolcol = -1 or x.boolcol > 0 then 1 else 0 end FROM x WHERE case when x.boolcol is null then null when x.boolcol = -1 or x.boolcol > 0 then 1 else 0 end = 0"; //$NON-NLS-1$
        String ddl = "create foreign table x (boolcol boolean options (native_type 'tinyint(1)'))";
        TranslationHelper.helpTestVisitor(ddl, input, output, TRANSLATOR);
    }

    @Test public void testTimestampLiteral() throws Exception {
        String input = "SELECT {ts '1970-01-01 00:00:00.123456789'}"; //$NON-NLS-1$
        String output = "SELECT {ts '1970-01-01 00:00:00.123456789'}"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }
}
