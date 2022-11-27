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

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;

@SuppressWarnings("nls")
public class TestNetezzaTranslatorCapabilities {

    private static NetezzaExecutionFactory TRANSLATOR;

    @BeforeClass public static void oneTimeSetup() throws TranslatorException {
        TRANSLATOR = new NetezzaExecutionFactory();
        TRANSLATOR.start();
    }

    public void helpTestVisitor(String input, String expectedOutput) throws TranslatorException {
        // Convert from sql to objects
        Command obj = FakeTranslationFactory.getInstance().getBQTTranslationUtility().parseCommand(input);

        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), TRANSLATOR);
        tc.translateCommand(obj);


        // Check stuff
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());
    }


    /////////BASIC TEST CASES FOR CAPABILITIES/////////////
    /////////////////////////////////////////////////
    @Test
    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100";
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 100";

        helpTestVisitor(
            input,
            output);

    }
    @Test
    public void testSelectDistinct() throws Exception {
        String input = "select distinct intkey from bqt1.smalla limit 100";
        String output = "SELECT DISTINCT SmallA.IntKey FROM SmallA LIMIT 100";

        helpTestVisitor(
            input,
            output);
    }
    @Test
    public void testSelectExpression() throws Exception {
        String input = "select  intkey, intkey + longnum / 2 as test from bqt1.smalla";
        String output = "SELECT SmallA.IntKey, (SmallA.IntKey + (SmallA.LongNum / 2)) AS test FROM SmallA";

        helpTestVisitor(
            input,
            output);
    }

    public void testBetweenCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where intnum BETWEEN 2 AND 10";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.IntNum >= 2 AND SmallA.IntNum <= 10";

        helpTestVisitor(
            input,
            output);
    }
    public void testCompareCriteriaEquals() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where intnum = 10";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.IntNum = 10";

        helpTestVisitor(
            input,
            output);
    }
    public void testCompareCriteriaOrdered() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where intnum < 10";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.IntNum < 10";

        helpTestVisitor(
            input,
            output);
    }
    public void testLikeCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where stringkey like '4%'";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.StringKey LIKE '4%'";

        helpTestVisitor(
            input,
            output);
    }
    public void testLikeWithEscapeCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where stringkey like '4\\%'";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.StringKey LIKE '4\\%'";

        helpTestVisitor(
            input,
            output);
    }

    public void testInCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where stringkey IN ('10', '11', '12')";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.StringKey IN ('10', '11', '12')";

        helpTestVisitor(
            input,
            output);
    }
    public void testInCriteriaSubQuery() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where stringkey IN (select stringkey from bqt1.smalla where intkey < 10)";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.StringKey IN (SELECT SmallA.StringKey FROM SmallA WHERE SmallA.IntKey < 10)";

        helpTestVisitor(
            input,
            output);
    }

    public void testIsNullCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where intnum IS NULL";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.IntNum IS NULL";

        helpTestVisitor(
            input,
            output);
    }
    public void testOrCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where intnum < 2 OR intnum > 10";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.IntNum < 2 OR SmallA.IntNum > 10";

        helpTestVisitor(
            input,
            output);
    }
    @Test public void testIsNotNullCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where intnum IS NOT NULL";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.IntNum IS NOT NULL";

        helpTestVisitor(
            input,
            output);
    }

    @Test public void testExistsCriteria() throws Exception {
        String input = "select  intkey, intnum from bqt1.smalla where exists (select intkey from bqt1.smallb)";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE EXISTS (SELECT SmallB.IntKey FROM SmallB LIMIT 1)";

        helpTestVisitor(
            input,
            output);
    }
    @Test public void testHavingClauseCriteria() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA GROUP BY INTKEY HAVING INTKEY = (SELECT INTKEY FROM BQT1.SMALLA WHERE STRINGKEY = 20)";
        String output = "SELECT SmallA.IntKey FROM SmallA GROUP BY SmallA.IntKey HAVING SmallA.IntKey = (SELECT SmallA.IntKey FROM SmallA WHERE SmallA.StringKey = '20' LIMIT 2)";

        helpTestVisitor(
            input,
            output);
    }

    @Test public void testScalarSubQuery() throws Exception {
        String input = "select intkey, intnum from bqt1.smalla where intnum < (0.01 * (select sum(intnum) from bqt1.smalla ))";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA WHERE SmallA.IntNum < (0.01 * (SELECT SUM(SmallA.IntNum) FROM SmallA))";

        helpTestVisitor(
            input,
            output);
    }

    @Test public void testSimpleCaseExpression() throws Exception {
        String input = "SELECT stringnum,  intnum,  CASE  BOOLEANVALUE  WHEN 'true'  then 'true' WHEN false THEN 'FALSE' ELSE 'GOOD' END    FROM bqt1.smalla;";
        String output = "SELECT SmallA.StringNum, SmallA.IntNum, CASE WHEN SmallA.BooleanValue = 1 THEN 'true' WHEN SmallA.BooleanValue = 0 THEN 'FALSE' ELSE 'GOOD' END FROM SmallA";

        helpTestVisitor(
            input,
            output);
    }

    @Test public void testSearchedCaseExpression() throws Exception {
        String input = "SELECT AVG(CASE WHEN intnum > 10 THEN intnum ELSE intkey END) \"Average\" FROM bqt1.smalla";
        String output = "SELECT AVG(CASE WHEN SmallA.IntNum > 10 THEN SmallA.IntNum ELSE SmallA.IntKey END) AS Average FROM SmallA";

        helpTestVisitor(
            input,
            output);
    }

//    @Test public void testQuantifiedCompareSOMEorANY() throws Exception {
//        String input = "SELECT INTKEY, BYTENUM FROM BQT1.SmallA WHERE BYTENUM = ANY (SELECT BYTENUM FROM BQT1.SmallA WHERE BYTENUM >= '-108')";
//        String output = "SELECT SmallA.IntKey, SmallA.ByteNum FROM SmallA WHERE SmallA.ByteNum = SOME (SELECT SmallA.ByteNum FROM SmallA WHERE SmallA.ByteNum >= -108)";
//
//        helpTestVisitor(
//            input,
//            output);
//    }

    @Test public void testQuantifiedCompareALL() throws Exception {
        String input = "SELECT INTKEY, STRINGKEY FROM BQT1.SMALLA WHERE STRINGKEY = ALL (SELECT STRINGKEY FROM BQT1.SMALLA WHERE INTKEY = 40)";
        String output = "SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA WHERE SmallA.StringKey = ALL (SELECT SmallA.StringKey FROM SmallA WHERE SmallA.IntKey = 40)";

        helpTestVisitor(
            input,
            output);
    }

    @Test public void testSelfJoin() throws Exception {
        String input = "SELECT x.intnum, y.intkey  FROM bqt1.smalla x, bqt1.smalla y   WHERE x.stringnum = y.intnum;";
        String output = "SELECT x.IntNum, y.IntKey FROM SmallA AS x, SmallA AS y WHERE x.StringNum = cast(y.IntNum AS varchar(4000))";

        helpTestVisitor(
            input,
            output);
    }

    @Test public void testLimitWithNestedInlineView() throws Exception {
        String input = "select max(intkey), stringkey from (select intkey, stringkey from bqt1.smalla order by intkey limit 100) x group by stringkey";
        String output = "SELECT MAX(x.intkey), x.stringkey FROM (SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA ORDER BY SmallA.IntKey LIMIT 100) AS x GROUP BY x.stringkey";

        helpTestVisitor( input, output);
    }

    @Test public void testAggregatesAndEnhancedNumeric() throws Exception {
        String input = "select count(*), min(intkey), max(intkey), sum(intkey), avg(intkey), count(intkey), STDDEV_SAMP(intkey), STDDEV_POP(intkey), VAR_SAMP(intkey), VAR_POP(intkey) from bqt1.smalla";
        String output = "SELECT COUNT(*), MIN(SmallA.IntKey), MAX(SmallA.IntKey), SUM(SmallA.IntKey), AVG(SmallA.IntKey), COUNT(SmallA.IntKey), STDDEV_SAMP(SmallA.IntKey), STDDEV_POP(SmallA.IntKey), VAR_SAMP(SmallA.IntKey), VAR_POP(SmallA.IntKey) FROM SmallA";

        helpTestVisitor( input, output);
    }
    @Test public void testAggregatesDistinct() throws Exception {
        String input = "select avg(DISTINCT intnum) from bqt1.smalla";
        String output = "SELECT AVG(DISTINCT SmallA.IntNum) FROM SmallA";

        helpTestVisitor( input, output);
    }


    @Test public void testExceptAsMinus() throws Exception {
        String input = "select intkey, intnum from bqt1.smalla except select intnum, intkey from bqt1.smallb";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA EXCEPT SELECT SmallB.IntNum, SmallB.IntKey FROM SmallB";

        helpTestVisitor( input, output);
    }

    @Test public void testUnionAsPlus() throws Exception {
        String input = "select intkey, intnum from bqt1.smalla union select intnum, intkey from bqt1.smallb";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA UNION SELECT SmallB.IntNum, SmallB.IntKey FROM SmallB";

        helpTestVisitor( input, output);
    }
    @Test public void testUnionAllAsPlus() throws Exception {
        String input = "select intkey, intnum from bqt1.smalla union all select intnum, intkey from bqt1.smallb";
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA UNION ALL SELECT SmallB.IntNum, SmallB.IntKey FROM SmallB";

        helpTestVisitor( input, output);
    }

    @Test public void testUnionAllAsPlusWithAggregates() throws Exception {
        String input = "select intkey, Sum(intnum) from bqt1.smalla group by intkey union all select intnum, intkey from bqt1.smallb";
        String output = "SELECT SmallA.IntKey, SUM(SmallA.IntNum) FROM SmallA GROUP BY SmallA.IntKey UNION ALL SELECT SmallB.IntNum, SmallB.IntKey AS IntKey FROM SmallB";

        helpTestVisitor( input, output);
    }


    @Test public void testintersect() throws Exception {
         String input = "select intkey from bqt1.smalla where intkey < 20 INTERSECT select intkey from bqt1.smalla where intkey > 10";
         String output = "SELECT SmallA.IntKey FROM SmallA WHERE SmallA.IntKey < 20 INTERSECT SELECT SmallA.IntKey FROM SmallA WHERE SmallA.IntKey > 10";


        helpTestVisitor( input, output);
    }


    @Test public void testUnionOrderBy() throws Exception {
        String input = "(select intkey from bqt1.smalla) union select intnum from bqt1.smalla order by intkey";
        String output = "SELECT SmallA.IntKey FROM SmallA UNION SELECT SmallA.IntNum FROM SmallA ORDER BY intkey";

        helpTestVisitor( input, output);

    }

    @Test public void testIntersectOrderBy() throws Exception {
        String input = "(select intkey from bqt1.smalla) intersect select intnum from bqt1.smalla order by intkey";
        String output = "SELECT SmallA.IntKey FROM SmallA INTERSECT SELECT SmallA.IntNum FROM SmallA ORDER BY intkey";

        helpTestVisitor( input, output);

    }

    @Test public void testExceptOrderBy() throws Exception {
        String input = "(select intkey from bqt1.smalla) except select intnum from bqt1.smalla order by intkey";
        String output = "SELECT SmallA.IntKey FROM SmallA EXCEPT SELECT SmallA.IntNum FROM SmallA ORDER BY intkey";

        helpTestVisitor( input, output);

    }


    @Test public void testRowLimitOFFSET() throws Exception {
        String input = "select intkey from bqt1.smalla limit 20, 30";
        String output = "SELECT SmallA.IntKey FROM SmallA LIMIT 30 OFFSET 20";

        helpTestVisitor( input, output);
    }


    @Test public void testOrderByNullsFirstLast() throws Exception {
        String input = "select intkey,  longnum from  bqt1.smalla order by longnum NULLS LAST";
        String output = "SELECT SmallA.IntKey, SmallA.LongNum FROM SmallA ORDER BY SmallA.LongNum NULLS LAST";

        helpTestVisitor( input, output);
    }

    @Test public void testOrderByUnRelated() throws Exception {
        String input = "select intkey,  longnum from  bqt1.smalla order by floatnum";
        String output = "SELECT SmallA.IntKey, SmallA.LongNum FROM SmallA ORDER BY SmallA.FloatNum";

        helpTestVisitor( input, output);
    }


    @Test public void testInnerJoin() throws Exception {
        String input = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA, BQT2.SmallB WHERE BQT1.SmallA.IntKey = BQT2.SmallB.IntKey AND BQT1.SmallA.IntKey >= 0 AND BQT2.SmallB.IntKey >= 0 ORDER BY BQT1.SmallA.IntKey";
        String output = "SELECT SmallA.IntKey FROM SmallA, SmallB WHERE SmallA.IntKey = SmallB.IntKey AND SmallA.IntKey >= 0 AND SmallB.IntKey >= 0 ORDER BY SmallA.IntKey";

        helpTestVisitor( input, output);
    }


    @Test public void testOuterJoin() throws Exception {
        String input = "SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA, BQT2.SmallB WHERE BQT1.SmallA.IntKey = BQT2.SmallB.IntKey AND BQT1.SmallA.IntKey >= 0 AND BQT2.SmallB.IntKey >= 0 ORDER BY BQT1.SmallA.IntKey";
        String output = "SELECT SmallA.IntKey FROM SmallA, SmallB WHERE SmallA.IntKey = SmallB.IntKey AND SmallA.IntKey >= 0 AND SmallB.IntKey >= 0 ORDER BY SmallA.IntKey";

        helpTestVisitor( input, output);
    }

    @Test public void testFullOuterJoin() throws Exception {
        String input = "SELECT BQT1.SmallA.IntNum, BQT2.SmallB.IntNum FROM BQT1.SmallA FULL OUTER JOIN BQT2.SmallB ON BQT1.SmallA.IntNum = BQT2.SmallB.IntNum ORDER BY BQT1.SmallA.IntNum";
        String output = "SELECT SmallA.IntNum, SmallB.IntNum FROM SmallA FULL OUTER JOIN SmallB ON SmallA.IntNum = SmallB.IntNum ORDER BY SmallA.IntNum";

        helpTestVisitor( input, output);
    }

    @Test public void testRightOuterJoin() throws Exception {
        String input = "SELECT BQT1.SmallA.IntNum, BQT2.SmallB.IntNum FROM BQT1.SmallA RIGHT OUTER JOIN BQT2.SmallB ON BQT1.SmallA.IntNum = BQT2.SmallB.IntNum ORDER BY BQT2.SmallB.IntNum";
        String output= "SELECT SmallA.IntNum, SmallB.IntNum FROM SmallB LEFT OUTER JOIN SmallA ON SmallA.IntNum = SmallB.IntNum ORDER BY SmallB.IntNum";

        helpTestVisitor( input, output);
    }
    @Test public void testLeftOuterJoin() throws Exception {
        String input = "SELECT BQT1.SmallA.IntNum, BQT2.SmallB.IntNum FROM BQT1.SmallA LEFT OUTER JOIN BQT2.SmallB ON BQT1.SmallA.IntNum = BQT2.SmallB.IntNum ORDER BY BQT1.SmallA.IntNum";
        String output = "SELECT SmallA.IntNum, SmallB.IntNum FROM SmallA LEFT OUTER JOIN SmallB ON SmallA.IntNum = SmallB.IntNum ORDER BY SmallA.IntNum";

        helpTestVisitor( input, output);
    }

    @Test public void testLikeRegex() throws Exception {
        String input = "SELECT BQT1.SmallA.IntNum FROM BQT1.SmallA where stringkey like_regex 'a.*' and stringkey not like_regex 'ab.*'";
        String output = "SELECT SmallA.IntNum FROM SmallA WHERE REGEXP_LIKE(SmallA.StringKey, 'a.*') AND NOT(REGEXP_LIKE(SmallA.StringKey, 'ab.*'))";

        helpTestVisitor( input, output);
    }

}
