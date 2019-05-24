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

package org.teiid.translator.jdbc.netezza;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestNetezzaTranslatorTypeMapping {

    private static NetezzaExecutionFactory TRANSLATOR;
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    @BeforeClass public static void oneTimeSetup() throws TranslatorException {
        TRANSLATOR = new NetezzaExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();


    }


    /////////////////UTILLITY FUNCTIONS/////////
    ////////////////////////////////////////////

    private String getTestBQTVDB() {

        return TranslationHelper.BQT_VDB;
    }


    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",
            Arrays.asList( srcExpression,LANG_FACTORY.createLiteral(tgtType, String.class)),TypeFacility.getDataTypeClass(tgtType));

        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType,
            expectedExpression, helpGetString(func));
    }
    public String helpGetString(Expression expr) throws Exception {
        SQLConversionVisitor sqlVisitor = TRANSLATOR.getSQLConversionVisitor();
        sqlVisitor.append(expr);

        return sqlVisitor.toString();
    }


    /////////TYPE MAPPING TESTS/////////
    ///////////////////////////////////

    @Test public void testCHARtoChar1() throws Exception {
        String input = "SELECT convert(StringKey, CHAR) FROM BQT1.SmallA";
        String output = "SELECT cast(SmallA.StringKey AS char(1)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testLongToBigInt() throws Exception {
        String input = "SELECT convert(convert(StringKey, long), string) FROM BQT1.SmallA";
        String output = "SELECT cast(cast(SmallA.StringKey AS bigint) AS varchar(4000)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToByte() throws Exception {
        String input = "SELECT char(convert(stringnum, byte) + 100) FROM BQT1.SMALLA";
        String output = "SELECT chr((cast(SmallA.StringNum AS byteint) + 100)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToShort() throws Exception {
        String input = "SELECT char(convert(stringnum, short) + 100) FROM BQT1.SMALLA";
        String output = "SELECT chr((cast(SmallA.StringNum AS smallint) + 100)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToLong() throws Exception {
//      String input = "SELECT char(convert(PART_WEIGHT, long) + 100) FROM PARTS";
//      String output = "SELECT chr((cast(PARTS.PART_WEIGHT AS bigint) + 100)) FROM PARTS";
      String input = "SELECT convert(stringnum, long) FROM BQT1.SMALLA";
      String output = "SELECT cast(SmallA.StringNum AS bigint) FROM SmallA";

      TranslationHelper.helpTestVisitor(getTestBQTVDB(),
          input,
          output, TRANSLATOR);
  }
    @Test public void testStringToBiginteger() throws Exception {
        String input = "SELECT convert(stringnum, BIGINTEGER) FROM BQT1.SMALLA";
        String output = "SELECT cast(SmallA.StringNum AS numeric(38)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testStringToBigdecimal() throws Exception {
        String input = "SELECT convert(stringnum, BIGDECIMAL) FROM BQT1.SMALLA";
        String output = "SELECT cast(SmallA.StringNum AS numeric(38,18)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testStringToVarchar() throws Exception {
        String input = "SELECT convert(intkey, STRING) FROM BQT1.SMALLA";
        String output = "SELECT cast(SmallA.IntKey AS varchar(4000)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }




    ////NON-MAPPED TYPES TEST/////////////
    //////////////////////////////////////

    @Test public void testStringToInteger() throws Exception {
        String input = "SELECT char(convert(stringnum, integer) + 100) FROM BQT1.SMALLA";
        String output = "SELECT chr((cast(SmallA.StringNum AS integer) + 100)) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }

    @Test public void testStringToFloat() throws Exception {
        String input = "SELECT convert(stringnum, float) FROM BQT1.SMALLA";
        String output = "SELECT cast(SmallA.StringNum AS float) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToReal() throws Exception {
        String input = "SELECT convert(stringnum, real) FROM BQT1.SMALLA";
        String output = "SELECT cast(SmallA.StringNum AS float) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToDouble() throws Exception {
        String input = "SELECT convert(stringnum, double) FROM BQT1.SMALLA";
        String output = "SELECT cast(SmallA.StringNum AS double) FROM SmallA";
       //SELECT cast(MEAS_PRD_ID AS DOUBLE) from ACTG_UNIT_BAL_FACT

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToBoolean() throws Exception {
        String input = "SELECT convert(stringnum, boolean) FROM BQT1.SMALLA";
        String output = "SELECT CASE WHEN SmallA.StringNum IN ('false', '0') THEN '0' WHEN SmallA.StringNum IS NOT NULL THEN '1' END FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToDate() throws Exception {
        String input = "SELECT convert(stringnum, date) FROM BQT1.SMALLA";
        String output = "SELECT to_date(SmallA.StringNum, 'YYYY-MM-DD') FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToTime() throws Exception {
        String input = "SELECT convert(stringnum, time) FROM BQT1.SMALLA";
        String output = "SELECT to_timestamp(SmallA.StringNum, 'HH24:MI:SS') FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }
    @Test public void testStringToTimestamp() throws Exception {
        String input = "SELECT convert(stringnum, timestamp) FROM BQT1.SMALLA";
        String output = "SELECT to_timestamp(SmallA.StringNum, 'YYYY-MM-DD HH24:MI:SS.MS') FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
            input,
            output, TRANSLATOR);
    }


    @Test public void testbooleanToIntegerConversion() throws Exception {
        String input = "SELECT BQT1.SmallA.IntNum, BQT2.SmallB.BooleanValue FROM BQT1.SmallA, BQT2.SmallB WHERE BQT1.SmallA.IntNum = convert(BQT2.SmallB.booleanvalue, integer)";
        String output = "SELECT SmallA.IntNum, SmallB.BooleanValue FROM SmallA, SmallB WHERE SmallA.IntNum = (CASE WHEN SmallB.BooleanValue IN ( '0', 'FALSE') THEN 0 WHEN SmallB.BooleanValue IS NOT NULL THEN 1 END)";
        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
                input,
                output, TRANSLATOR);


     }

    @Test public void testIntegerToBooleanconversion() throws Exception {
        String input = "SELECT BQT1.SmallA.IntNum, BQT2.SmallB.BooleanValue FROM BQT1.SmallA, BQT2.SmallB WHERE BQT1.SmallA.booleanvalue = convert(BQT2.SmallB.IntNum, boolean) AND BQT1.SmallA.IntKey >= 0 AND BQT2.SmallB.IntKey >= 0 ORDER BY BQT1.SmallA.IntNum";
        String output = "SELECT SmallA.IntNum, SmallB.BooleanValue FROM SmallA, SmallB WHERE SmallA.BooleanValue = CASE WHEN SmallB.IntNum = 0 THEN '0' WHEN SmallB.IntNum IS NOT NULL THEN '1' END AND SmallA.IntKey >= 0 AND SmallB.IntKey >= 0 ORDER BY SmallA.IntNum";
        TranslationHelper.helpTestVisitor(getTestBQTVDB(),
                input,
                output, TRANSLATOR);


     }


}
