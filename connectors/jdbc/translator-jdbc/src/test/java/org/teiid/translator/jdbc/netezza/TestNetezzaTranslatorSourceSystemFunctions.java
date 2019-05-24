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

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.LanguageFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslationHelper;

/**
 */
@SuppressWarnings("nls")
public class TestNetezzaTranslatorSourceSystemFunctions {

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


    /////SOURCESYSTEM FUNCTION TESTCASES//////////////
    ///////////////////////////////////////////////


    //////////////////BEGIN---STRING FUNCTIONS TESTCASES///////////////////

    @Test
    public void testLcaseUcase() throws Exception {
        String input = "select lcase(StringKey), ucase(StringKey) FROM BQT1.SmallA";
        String output = "SELECT lower(SmallA.StringKey), upper(SmallA.StringKey) FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output,TRANSLATOR);
    }
    @Test public void testPad() throws Exception {
        String input = "select lpad(smalla.stringkey, 18), rpad(smalla.stringkey, 12) from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT lpad(SmallA.StringKey, 18), rpad(SmallA.StringKey, 12) FROM SmallA"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input,
            output, TRANSLATOR);
    }

      @Test
      public void testIFNull() throws Exception {
      String input = "SELECT ifnull(StringKey, 'otherString') FROM BQT1.SmallA";
      String output = "SELECT NVL(SmallA.StringKey, 'otherString') FROM SmallA";
      //SELECT IFNULL(GL_ACTG_APPL_ID, 'otherString') FROM ACTG_UNIT_BAL_FACT
      //SELECT nvl(GL_ACTG_APPL_ID, 'otherString') FROM ACTG_UNIT_BAL_FACT

      TranslationHelper.helpTestVisitor(getTestBQTVDB(),  input, output, TRANSLATOR);
    }


      @Test public void testSubstring1() throws Exception {

          //////////BOTH SUBSTRING AND SUBSTR work in NETEZZA//
          //////////////////////////////////////////////////////
                  String input = "SELECT substring(StringKey, 1) FROM BQT1.SmallA";
                  String output = "SELECT substring(SmallA.StringKey, 1) FROM SmallA";
                //SELECT substring(FDL_PMF_ACCT, 3) FROM ACTG_UNIT_BAL_FACT
                //SELECT substr(FDL_PMF_ACCT, 3) FROM ACTG_UNIT_BAL_FACT
                  TranslationHelper.helpTestVisitor(getTestBQTVDB(), input, output, TRANSLATOR);
    }
    @Test public void testSubstring2() throws Exception {

          //////////BOTH SUBSTRING AND SUBSTR work in NETEZZA//
          //////////////////////////////////////////////////////
              String input = "SELECT substring(StringKey, 1, 5) FROM BQT1.SmallA";
              String output = "SELECT substring(SmallA.StringKey, 1, 5) FROM SmallA";
              TranslationHelper.helpTestVisitor(getTestBQTVDB(),  input, output, TRANSLATOR);
    }



    @Test public void testConcat_withLiteral() throws Exception {
//        String sql = "select stringnum || '1' from BQT1.Smalla";
//        String expected = "SELECT CASE WHEN SmallA.StringNum IS NULL THEN NULL ELSE concat(SmallA.StringNum, '1') END FROM SmallA";
//        helpTestVisitor(FakeMetadataFactory.exampleBQTCached(), sql, EMPTY_CONTEXT, null, expected);
        String input = "select stringnum || '1' from BQT1.Smalla";
        String output = "SELECT (SmallA.StringNum || '1') FROM SmallA";

        TranslationHelper.helpTestVisitor(getTestBQTVDB(),  input, output, TRANSLATOR);
    }



    ////BEGIN-LOCATE FUNCTION
    @Test public void testLocate() throws Exception {
        String input = "SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA";
        String output = "SELECT INSTR('chimp', cast(SmallA.IntNum AS varchar(4000)), 1) FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testLocate2() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA";
        String output = "SELECT INSTR('chimp', SmallA.StringNum) FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output,
                TRANSLATOR);
    }

    @Test public void testLocate3() throws Exception {
        String input = "SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26";
        String output = "SELECT INSTR('234567890', cast(SmallA.IntNum AS varchar(4000)), 1) FROM SmallA WHERE SmallA.IntKey = 26";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output,  TRANSLATOR);
    }

    @Test public void testLocate4() throws Exception {
        String input = "SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA";
        String output = "SELECT 1 FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testLocate5() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA";
        String output = "SELECT INSTR('chimp', SmallA.StringNum, 1) FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,  input, output, TRANSLATOR);
    }

     @Test public void testLocate6() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', INTNUM) FROM BQT1.SMALLA";
        String output = "SELECT INSTR('chimp', SmallA.StringNum, CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END) FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testLocate7() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', LOCATE(STRINGNUM, 'chimp') + 1) FROM BQT1.SMALLA";
        String output = "SELECT INSTR('chimp', SmallA.StringNum, CASE WHEN (INSTR('chimp', SmallA.StringNum) + 1) < 1 THEN 1 ELSE (INSTR('chimp', SmallA.StringNum) + 1) END) FROM SmallA";

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }
    ////END-LOCATE FUNCTION




    //////////////////BEGIN---NUMERIC FUNCTIONS TESTCASES///////////////////
    @Test public void testCeil() throws Exception {
        //select ceiling(sqrt(MEAS_PRD_ID)) from ACTG_UNIT_BAL_FACT
        //select ceil(sqrt(MEAS_PRD_ID)) from ACTG_UNIT_BAL_FACT
              String input = "SELECT ceiling(sqrt(INTKEY)) FROM BQT1.SMALLA";
              String output = "SELECT ceil(sqrt(SmallA.IntKey)) FROM SmallA";
              TranslationHelper.helpTestVisitor(getTestBQTVDB(),
                  input,
                  output, TRANSLATOR);
    }

    @Test public void testPower() throws Exception {

        //select power(MEAS_PRD_ID, 2) from ACTG_UNIT_BAL_FACT
        //select pow(MEAS_PRD_ID, 2) from ACTG_UNIT_BAL_FACT
              String input = "SELECT power(INTKEY, 2) FROM BQT1.SMALLA";
              String output = "SELECT pow(SmallA.IntKey, 2) FROM SmallA";
              TranslationHelper.helpTestVisitor(getTestBQTVDB(),
                  input,
                  output, TRANSLATOR);
    }
    //////////////////END---NUMERIC FUNCTIONS TESTCASES///////////////////


    //////////////////BEGIN---BIT FUNCTIONS CONVERSION TESTCASES///////////////////
    ///////////////////////////////////////////////////////////////////

   @Test public void testBitAnd() throws Exception {
       String input = "select bitand(intkey, intnum) from bqt1.smalla";
       String output = "SELECT int4and(SmallA.IntKey, SmallA.IntNum) FROM SmallA";

       TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
           input,
           output, TRANSLATOR);
   }
   @Test public void testBitNot() throws Exception {
       String input = "select bitnot(intkey) from bqt1.smalla";
       String output = "SELECT int4not(SmallA.IntKey) FROM SmallA";

       TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
           input,
           output, TRANSLATOR);
   }
   @Test public void testBitOr() throws Exception {
       String input = "select bitor(intkey, intnum) from bqt1.smalla";
       String output = "SELECT int4or(SmallA.IntKey, SmallA.IntNum) FROM SmallA";

       TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
           input,
           output, TRANSLATOR);
   }
   @Test public void testBitXor() throws Exception {
       String input = "select bitxor(intkey, intnum) from bqt1.smalla";
       String output = "SELECT int4xor(SmallA.IntKey, SmallA.IntNum) FROM SmallA";

       TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
           input,
           output, TRANSLATOR);
   }
   //////////////////END---BIT FUNCTIONS CONVERSION TESTCASES///////////////////
   /////////////////////////////////////////////////////////////////////////////


}
