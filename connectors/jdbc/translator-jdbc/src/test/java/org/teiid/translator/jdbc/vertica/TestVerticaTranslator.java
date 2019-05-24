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
package org.teiid.translator.jdbc.vertica;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

public class TestVerticaTranslator {

    private static VerticaExecutionFactory translator;

    @BeforeClass
    public static void setupOnce() throws Exception {
        translator = new VerticaExecutionFactory();
        translator.start();
    }

    public String getBQT_VDB() {
        return TranslationHelper.BQT_VDB;
    }

    public String getPARTS_VDB() {
        return TranslationHelper.PARTS_VDB;
    }

    public void helpTestVisitor(String vdb, String input, String expectedOutput) throws TranslatorException {
        TranslationHelper.helpTestVisitor(vdb, input, expectedOutput, translator);
    }

    @Test
    public void testConversion() throws TranslatorException {

        String input = "SELECT char(convert(PART_WEIGHT, integer) + 100) FROM PARTS"; //$NON-NLS-1$
        String output = "SELECT CHR((convert(PARTS.PART_WEIGHT, integer) + 100)) FROM PARTS";  //$NON-NLS-1$
        helpTestVisitor(getPARTS_VDB(), input, output);

        input = "SELECT curdate() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT CURRENT_DATE() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT curtime() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT CURRENT_TIME() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT week(datevalue) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT WEEK_ISO(MediumA.DateValue) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);
    }

    @Test
    public void testPushDownFuctions() throws TranslatorException {

        String input = "SELECT vertica.BIT_LENGTH('abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        String output = "SELECT BIT_LENGTH('abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.BITCOUNT(127) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT BITCOUNT(127) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.BITSTRING_TO_BINARY('abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT BITSTRING_TO_BINARY('abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.BTRIM('abc', 'abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT BTRIM('abc', 'abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.GREATEST(127) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT GREATEST(127) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.GREATESTB(127) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT GREATESTB(127) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.HEX_TO_BINARY('abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT HEX_TO_BINARY('abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.HEX_TO_INTEGER('127') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT HEX_TO_INTEGER('127') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.INITCAP('abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT INITCAP('abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.INSERT('abc', 1, 2, 'd') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT INSERT('abc', 1, 2, 'd') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.ISUTF8('abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT ISUTF8('abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.MD5('abc') FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT MD5('abc') FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.SPACE(127) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT SPACE(127) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.TO_HEX(127) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT TO_HEX(127) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.CBRT(6.2) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT CBRT(6.2) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.LN(6.2) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT LN(6.2) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.PI() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT PI() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.RANDOM() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT RANDOM() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.TRUNC(6.2) FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT TRUNC(6.2) FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.GETDATE() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT GETDATE() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.GETUTCDATE() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT GETUTCDATE() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.LOCALTIME() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT LOCALTIME() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT vertica.LOCALTIMESTAMP() FROM BQT1.MediumA"; //$NON-NLS-1$
        output = "SELECT LOCALTIMESTAMP() FROM MediumA"; //$NON-NLS-1$
        helpTestVisitor(getBQT_VDB(), input, output);
    }


    @Test
    public void testMinMaxBoolean() throws TranslatorException {
        String input = "SELECT MIN(booleanvalue) FROM BQT1.SmallA";
        String output = "SELECT CAST(MIN(CAST(SmallA.BooleanValue AS tinyint)) AS boolean) FROM SmallA";

        helpTestVisitor(getBQT_VDB(), input, output);

        input = "SELECT MAX(booleanvalue) FROM BQT1.SmallA";
        output = "SELECT CAST(MAX(CAST(SmallA.BooleanValue AS tinyint)) AS boolean) FROM SmallA";

        helpTestVisitor(getBQT_VDB(), input, output);
    }

}
