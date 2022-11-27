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

/**
 */
@SuppressWarnings("nls")
public class TestNetezzaTranslatorDatetimeConversion {

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


     ///////////////DATE/TIME CONVERSION TESTCASES///////
     ////////////////////////////////////////////////////

    @Test public void testdayofmonth() throws Exception {
        String input = "SELECT dayofmonth(datevalue) FROM BQT1.SMALLA";
        String output = "SELECT extract(DAY from SmallA.DateValue) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output, TRANSLATOR);
    }


     ///BEGIN--FROM TIMESTAMP->DATE, TIME, STRING////////
     @Test public void testTimestampToDate() throws Exception {
         String input = "SELECT convert(convert(TIMESTAMPVALUE, date), string) FROM BQT1.SMALLA";
         String output = "SELECT cast(cast(SmallA.TimestampValue AS DATE) AS varchar(4000)) FROM SmallA";

         TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output, TRANSLATOR);
     }
     @Test public void testTimestampToTime() throws Exception {
         String input = "SELECT convert(convert(TIMESTAMPVALUE, time), string) FROM BQT1.SMALLA";
         String output = "SELECT cast(cast(SmallA.TimestampValue AS TIME) AS varchar(4000)) FROM SmallA";

         TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output, TRANSLATOR);
     }

     @Test public void testTimestampToString() throws Exception {
          String input = "SELECT convert(timestampvalue, string) FROM BQT1.SMALLA";
          String output = "SELECT to_char(SmallA.TimestampValue, 'YYYY-MM-DD HH24:MI:SS.MS') FROM SmallA";

          TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output, TRANSLATOR);
      }
     ///END--FROM TIMESTAMP->DATE, TIME, STRING////////

     ///BEGIN--FROM DATE->TIMESTAMP////////
     @Test public void testDateToTimestamp() throws Exception {
         String input = "SELECT convert(convert(datevalue, timestamp), string) FROM BQT1.SMALLA";
         String output = "SELECT to_char(cast(SmallA.DateValue AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS.MS') FROM SmallA";

         TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output, TRANSLATOR);
     }
     ///END--FROM DATE->TIMESTAMP////////

     ///BEGIN--FROM TIME->TIMESTAMP////////
     @Test public void testTimeToTimestamp() throws Exception {
         String input = "SELECT convert(convert(TIMEVALUE, timestamp), string) FROM BQT1.SMALLA";
         //String output = "SELECT to_char(cast(SmallA.TimeValue AS timestamp), 'YYYY-MM-DD HH24:MI:SS.FF') FROM SmallA";
         String output = "SELECT to_char(SmallA.TimeValue, 'YYYY-MM-DD HH24:MI:SS.MS') FROM SmallA";

         TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output,     TRANSLATOR);
     }
     ///END--FROM TIME->TIMESTAMP////////


//     @Test public void testTimestampToTime() throws Exception {
//          helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(111, 4, 5, 9, 16, 34, 220000000), Timestamp.class), "TIME", "cast(cast('2011-05-05 09:16:34.22' AS TIMESTAMP(6)) AS TIME)");
//      }




}
