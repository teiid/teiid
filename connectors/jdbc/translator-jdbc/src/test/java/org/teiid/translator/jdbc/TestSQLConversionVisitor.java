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

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.dqp.internal.datamgr.ExecutionContextImpl;
import org.teiid.dqp.internal.datamgr.TestDeleteImpl;
import org.teiid.dqp.internal.datamgr.TestInsertImpl;
import org.teiid.dqp.internal.datamgr.TestProcedureImpl;
import org.teiid.dqp.internal.datamgr.TestQueryImpl;
import org.teiid.dqp.internal.datamgr.TestUpdateImpl;
import org.teiid.language.LanguageObject;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

/**
 */
@SuppressWarnings("nls")
public class TestSQLConversionVisitor {

    public static final ExecutionContextImpl context = new ExecutionContextImpl("VDB",  //$NON-NLS-1$
                                                                            1,
                                                                            "Payload",  //$NON-NLS-1$
                                                                            "ConnectionID",   //$NON-NLS-1$
                                                                            "Connector", //$NON-NLS-1$
                                                                            1,
                                                                            "PartID",  //$NON-NLS-1$
                                                                            "ExecCount");     //$NON-NLS-1$

    static {
        context.setSession(new SessionMetadata());
    }

    private static JDBCExecutionFactory TRANSLATOR;

    @BeforeClass public static void oneTimeSetup() throws TranslatorException {
        TRANSLATOR = new JDBCExecutionFactory();
        TRANSLATOR.setTrimStrings(true);
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }

    public String getTestVDB() {
        return TranslationHelper.PARTS_VDB;
    }

    public void helpTestVisitor(String vdb, String input, String expectedOutput) {
        helpTestVisitor(vdb, input, expectedOutput, false);
    }

    public TranslatedCommand helpTestVisitor(String vdb, String input, String expectedOutput, boolean usePreparedStatement) {
        JDBCExecutionFactory trans = new JDBCExecutionFactory();
        trans.setUseBindVariables(usePreparedStatement);
        try {
            trans.start();
            return TranslationHelper.helpTestVisitor(vdb, input, expectedOutput, trans);
        } catch (TranslatorException e) {
            throw new RuntimeException(e);
        }
    }

    private String getStringWithContext(LanguageObject obj) throws TranslatorException {
        return getStringWithContext(obj, null);
    }

    private String getStringWithContext(LanguageObject obj, String comment) throws TranslatorException {
        JDBCExecutionFactory env = new JDBCExecutionFactory();
        env.setUseCommentsInSourceQuery(true);
        env.setUseBindVariables(false);
        if (comment != null) {
            env.setCommentFormat(comment);
        }
        env.start();

        SQLConversionVisitor visitor = env.getSQLConversionVisitor();
        visitor.setExecutionContext(context);
        visitor.append(obj);
        return visitor.toString();
    }

    @Test public void testSimple() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testAliasInSelect() {
        helpTestVisitor(getTestVDB(),
            "select part_name as x from parts", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME AS x FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testAliasedGroup() {
        helpTestVisitor(getTestVDB(),
            "select y.part_name from parts y", //$NON-NLS-1$
            "SELECT y.PART_NAME FROM PARTS AS y"); //$NON-NLS-1$
    }

    @Test public void testAliasedGroupAndElement() {
        helpTestVisitor(getTestVDB(),
            "select y.part_name AS z from parts y", //$NON-NLS-1$
            "SELECT y.PART_NAME AS z FROM PARTS AS y"); //$NON-NLS-1$
    }

    @Test public void testLiteralString() {
        helpTestVisitor(getTestVDB(),
            "select 'x' from parts", //$NON-NLS-1$
            "SELECT 'x' FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralInteger() {
        helpTestVisitor(getTestVDB(),
            "select 5 from parts", //$NON-NLS-1$
            "SELECT 5 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralFloat() {
        helpTestVisitor(getTestVDB(),
            "select 5.2 from parts", //$NON-NLS-1$
            "SELECT 5.2 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralLowFloat() {
        helpTestVisitor(getTestVDB(),
            "select 0.012 from parts", //$NON-NLS-1$
            "SELECT 0.012 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralLowFloat2() {
        helpTestVisitor(getTestVDB(),
            "select 0.00012 from parts", //$NON-NLS-1$
            "SELECT 0.00012 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralHighFloat() {
        helpTestVisitor(getTestVDB(),
            "select 12345.123 from parts", //$NON-NLS-1$
            "SELECT 12345.123 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralHighFloat2() {
        helpTestVisitor(getTestVDB(),
            "select 1234567890.1234567 from parts", //$NON-NLS-1$
            "SELECT 1234567890.1234567 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralBoolean() {
        helpTestVisitor(getTestVDB(),
            "select {b'true'}, {b'false'} from parts", //$NON-NLS-1$
            "SELECT 1, 0 FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralDate() {
        helpTestVisitor(getTestVDB(),
            "select {d '2003-12-31'} from parts", //$NON-NLS-1$
            "SELECT {d '2003-12-31'} FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralTime() {
        helpTestVisitor(getTestVDB(),
            "select {t '23:59:59'} from parts", //$NON-NLS-1$
            "SELECT {t '23:59:59'} FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralNull() {
        helpTestVisitor(getTestVDB(),
            "select null from parts", //$NON-NLS-1$
            "SELECT NULL FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testLiteralTimestamp() {
        helpTestVisitor(getTestVDB(),
            "select {ts '2003-12-31 23:59:59.123'} from parts", //$NON-NLS-1$
            "SELECT {ts '2003-12-31 23:59:59.123'} FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testSQL89Join() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p, supplier_parts s where p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p, SUPPLIER_PARTS AS s WHERE p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testSQL92Join() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p INNER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testSelfJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p join parts p2 on p.part_id = p2.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p INNER JOIN PARTS AS p2 ON p.PART_ID = p2.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testRightOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p right join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM SUPPLIER_PARTS AS s LEFT OUTER JOIN PARTS AS p ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testLeftOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p left join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p LEFT OUTER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testFullOuterJoin() {
        helpTestVisitor(getTestVDB(),
            "select p.part_name from parts p full join supplier_parts s on p.part_id = s.part_id", //$NON-NLS-1$
            "SELECT p.PART_NAME FROM PARTS AS p FULL OUTER JOIN SUPPLIER_PARTS AS s ON p.PART_ID = s.PART_ID"); //$NON-NLS-1$
    }

    @Test public void testCompare1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id = 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID = 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id <> 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID <> 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare3() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id < 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID < 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare4() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id <= 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID <= 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare5() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id > 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID > 'x'"); //$NON-NLS-1$
    }

    @Test public void testCompare6() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id >= 'x'", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID >= 'x'"); //$NON-NLS-1$
    }

    @Test public void testIn1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id in ('x')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID = 'x'"); //$NON-NLS-1$
    }

    @Test public void testIn2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id in ('x', 'y')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IN ('x', 'y')"); //$NON-NLS-1$
    }

    @Test public void testIn3() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id not in ('x', 'y')", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID NOT IN ('x', 'y')"); //$NON-NLS-1$
    }

    @Test public void testIsNull1() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id is null", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IS NULL"); //$NON-NLS-1$
    }

    @Test public void testIsNull2() {
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_id is not null", //$NON-NLS-1$
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID IS NOT NULL"); //$NON-NLS-1$
    }

    @Test public void testInsertNull() {
        helpTestVisitor(getTestVDB(),
            "insert into parts (part_id, part_name, part_color, part_weight) values ('a', null, 'c', 'd')", //$NON-NLS-1$
            "INSERT INTO PARTS (PART_ID, PART_NAME, PART_COLOR, PART_WEIGHT) VALUES ('a', NULL, 'c', 'd')"); //$NON-NLS-1$
    }

    @Test public void testUpdateNull() {
        helpTestVisitor(getTestVDB(),
            "update parts set part_weight = null where part_color = 'b'", //$NON-NLS-1$
            "UPDATE PARTS SET PART_WEIGHT = NULL WHERE PARTS.PART_COLOR = 'b'"); //$NON-NLS-1$
    }

    @Test public void testUpdateWhereNull() {
        helpTestVisitor(getTestVDB(),
            "update parts set part_weight = 'a' where part_weight = null", //$NON-NLS-1$
            "UPDATE PARTS SET PART_WEIGHT = 'a' WHERE NULL <> NULL"); //$NON-NLS-1$
    }

    @Test public void testPreparedStatementCreationWithUpdate() {
        helpTestVisitor(getTestVDB(),
                        "update parts set part_weight = 'a' || 'b' where part_weight < 50/10", //$NON-NLS-1$
                        "UPDATE PARTS SET PART_WEIGHT = ? WHERE PARTS.PART_WEIGHT < ?", //$NON-NLS-1$
                        true);
    }

    @Test public void testPreparedStatementCreationWithInsert() {
        helpTestVisitor(getTestVDB(),
                        "insert into parts (part_weight) values (50/10)", //$NON-NLS-1$
                        "INSERT INTO PARTS (PART_WEIGHT) VALUES (?)", //$NON-NLS-1$
                        true);
    }

    @Test public void testPreparedStatementCreationWithSelect() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where part_id not in ('x' || 'a', 'y' || 'b') and part_weight < '6'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_ID NOT IN (?, ?) AND PARTS.PART_WEIGHT < '6'", //$NON-NLS-1$
                        true);
    }

    @Test public void testPreparedStatementCreationWithLike() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where part_name like '%foo' || '_'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME LIKE ?", //$NON-NLS-1$
                        true);
    }

    /**
     * ideally this should not happen, but to be on the safe side
     * only the right side should get replaced
     */
    public void defer_testPreparedStatementCreationWithLeftConstant() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where 'x' = 'y'", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE 1 = ?", //$NON-NLS-1$
                        true);
    }

    /**
     * In the future, functions can be made smarter about which of their literal arguments
     * either are (or are not) eligible to be bind variables
     */
    @Test public void testPreparedStatementCreationWithFunction() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where concat(part_name, 'x') = concat('y', part_weight)", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE concat(PARTS.PART_NAME, 'x') = concat('y', PARTS.PART_WEIGHT)", //$NON-NLS-1$
                        true);
    }

    @Test public void testPreparedStatementCreationWithCase() {
        helpTestVisitor(getTestVDB(),
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_WEIGHT = CASE WHEN PARTS.PART_NAME='a' || 'b' THEN 'b' ELSE 'c' END", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_WEIGHT = CASE WHEN PARTS.PART_NAME = ? THEN 'b' ELSE 'c' END", //$NON-NLS-1$
                        true);
    }

    @Test public void testVisitIDeleteWithComment() throws Exception {
        String expected = "DELETE /*teiid sessionid:ConnectionID, requestid:ConnectionID.1.PartID*/ FROM g1 WHERE 100 >= 200 AND 500 < 600"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestDeleteImpl.example()));
    }

    @Test public void testVisitIInsertWithComment() throws Exception {
        String expected = "INSERT /*teiid sessionid:ConnectionID, requestid:ConnectionID.1.PartID*/ INTO g1 (e1, e2, e3, e4) VALUES (1, 2, 3, 4)"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestInsertImpl.example("g1"))); //$NON-NLS-1$
    }

    @Test public void testVisitISelectWithComment() throws Exception {
        String expected = "SELECT /*teiid sessionid:ConnectionID, requestid:ConnectionID.1.PartID*/ g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestQueryImpl.example(false)));
        expected = "SELECT /*teiid sessionid:ConnectionID, requestid:ConnectionID.1.PartID*/ DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestQueryImpl.example(true)));
    }

    @Test public void testVisitSelectWithCustomComment() throws Exception {
        String expected = "SELECT /* foo ConnectionID ConnectionID.1 PartID ExecCount null VDB 1 false */g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestQueryImpl.example(false), "/* foo {0} {1} {2} {3} {4} {5} {6} {7} */"));
    }

    @Test public void testVisitIUpdateWithComment() throws Exception {
        String expected = "UPDATE /*teiid sessionid:ConnectionID, requestid:ConnectionID.1.PartID*/ g1 SET e1 = 1, e2 = 1, e3 = 1, e4 = 1 WHERE 1 = 1"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestUpdateImpl.example()));
    }

    @Test public void testVisitIProcedureWithComment() throws Exception {
        String expected = "/*teiid sessionid:ConnectionID, requestid:ConnectionID.1.PartID*/ {call sq3(?,?)}"; //$NON-NLS-1$
        assertEquals(expected, getStringWithContext(TestProcedureImpl.example()));
    }

    @Test public void testTrimStrings() throws Exception {
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, "select stringkey from bqt1.smalla", "SELECT rtrim(SmallA.StringKey) FROM SmallA", TRANSLATOR); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNestedSetQuery() throws Exception {
        String input = "select part_id id FROM parts UNION ALL (select part_name FROM parts UNION select part_id FROM parts)"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION ALL (SELECT PARTS.PART_NAME FROM PARTS UNION SELECT rtrim(PARTS.PART_ID) FROM PARTS)"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testNestedSetQuery1() throws Exception {
        String input = "select part_id id FROM parts UNION (select part_name FROM parts EXCEPT select part_id FROM parts)"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION (SELECT PARTS.PART_NAME FROM PARTS EXCEPT SELECT rtrim(PARTS.PART_ID) FROM PARTS)"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testNestedSetQuery2() throws Exception {
        String input = "select part_id id FROM parts UNION select part_name FROM parts EXCEPT select part_id FROM parts"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION SELECT PARTS.PART_NAME FROM PARTS EXCEPT SELECT rtrim(PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testNestedSetQuery3() throws Exception {
        String input = "select part_id id FROM parts UNION (select part_name FROM parts Union ALL select part_id FROM parts)"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS UNION SELECT PARTS.PART_NAME FROM PARTS UNION SELECT rtrim(PARTS.PART_ID) FROM PARTS"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testUpdateTrimStrings() throws Exception {
        String input = "UPDATE PARTS SET PART_ID = NULL WHERE PARTS.PART_COLOR = 'b'"; //$NON-NLS-1$
        String output = "UPDATE PARTS SET PART_ID = NULL WHERE PARTS.PART_COLOR = 'b'"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);

        input = "insert into parts (part_id) values ('a')"; //$NON-NLS-1$
        output = "INSERT INTO PARTS (PART_ID) VALUES ('a')"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testOrderByUnrelated() throws Exception {
        String input = "select part_id id FROM parts order by part_name"; //$NON-NLS-1$
        String output = "SELECT rtrim(PARTS.PART_ID) AS id FROM PARTS ORDER BY PARTS.PART_NAME"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testVarbinary() throws Exception {
        String input = "select X'AB' FROM parts"; //$NON-NLS-1$
        String output = "SELECT X'AB' FROM PARTS"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testConcat2() {
        helpTestVisitor(getTestVDB(),
                        "select part_name from parts where concat2(part_name, 'x') = concat2(part_weight, part_id)", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME FROM PARTS WHERE concat(ifnull(PARTS.PART_NAME, ''), 'x') = CASE WHEN PARTS.PART_WEIGHT IS NULL AND PARTS.PART_ID IS NULL THEN NULL ELSE concat(ifnull(PARTS.PART_WEIGHT, ''), ifnull(PARTS.PART_ID, '')) END", //$NON-NLS-1$
                        true);
    }

    @Test public void testSelectWithoutFrom() {
        helpTestVisitor(getTestVDB(),
                        "select 1", //$NON-NLS-1$
                        "SELECT 1", //$NON-NLS-1$
                        true);
    }

    @Test public void testFunctionNativeQuery() throws SQLException {
        String ddl = "create foreign table t (x integer, y integer); create foreign function bsl (arg1 integer, arg2 integer) returns integer OPTIONS (\"teiid_rel:native-query\" '$1 << $2');";

        helpTestVisitor(ddl,
                        "select bsl(x, y) from t", //$NON-NLS-1$
                        "SELECT t.x << t.y FROM t", //$NON-NLS-1$
                        true);
        //make sure we don't treat arguments as bind values
        helpTestVisitor(ddl,
                "select bsl(x, y) from t where x = 1 + 1", //$NON-NLS-1$
                "SELECT t.x << t.y FROM t WHERE t.x = ?", //$NON-NLS-1$
                true);

        TranslatedCommand tc = helpTestVisitor(ddl,
                "select bsl(1, y) from t where x = y", //$NON-NLS-1$
                "SELECT ? << t.y FROM t WHERE t.x = t.y", //$NON-NLS-1$
                true);
        JDBCQueryExecution qe = new JDBCQueryExecution(null, null, Mockito.mock(ExecutionContext.class), TRANSLATOR);
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        qe.bind(ps, tc.getPreparedValues(), null);
        Mockito.verify(ps, Mockito.times(1)).setObject(1, 1, Types.INTEGER);
    }

    @Test public void testGroupByRollup() {
        helpTestVisitor(getTestVDB(),
                        "select part_name, max(part_weight) from parts group by rollup(part_name)", //$NON-NLS-1$
                        "SELECT PARTS.PART_NAME, MAX(PARTS.PART_WEIGHT) FROM PARTS GROUP BY ROLLUP(PARTS.PART_NAME)", //$NON-NLS-1$
                        true);
    }

    //previously would have failed in other locales
    @Test public void testDoubleFormat() {
        helpTestVisitor(getTestVDB(),
                        "select 1.0e10, -1.0e2", //$NON-NLS-1$
                        "SELECT 10000000000.0, -100.0", //$NON-NLS-1$
                        true);
    }

    @Test public void testDecimalFormat() {
        helpTestVisitor(getTestVDB(),
                        "select 100000000000.0, -.0000001", //$NON-NLS-1$
                        "SELECT 100000000000.0, -0.0000001", //$NON-NLS-1$
                        true);
    }

    @Test public void testProcedureTable() {
        helpTestVisitor("create foreign table smallb (intkey integer, stringkey string); "
                + "create foreign procedure spTest5 (param integer) returns table(stringkey string options (nameinsource 'other'), intkey integer)",
                "select smallb.intkey, x.stringkey, x.intkey "
                        + "from smallb left outer join lateral (exec spTest5(smallb.intkey)) as x on (true)",
                        "SELECT smallb.intkey, x.other, x.intkey FROM smallb LEFT OUTER JOIN LATERAL (EXEC spTest5(smallb.intkey)) AS x ON 1 = 1", //$NON-NLS-1$
                        true);
    }

    @Test
    public void testSTInsert() throws Exception {
        String input = "insert into cola_markets(name,shape) values('foo124', ST_GeomFromText('POINT (300 100)', 8307))"; //$NON-NLS-1$
        String output = "INSERT INTO COLA_MARKETS (NAME, SHAPE) VALUES ('foo124', st_geomfromwkb(?, 8307))"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }

    @Test public void testWindowFunctionOver() throws Exception {
        String input = "select nth_value(PARTS.PART_NAME, 2) over (partition by part_id order by part_name range between unbounded preceding and 1 following) FROM parts"; //$NON-NLS-1$
        String output = "SELECT NTH_VALUE(PARTS.PART_NAME, 2) OVER (PARTITION BY rtrim(PARTS.PART_ID) ORDER BY PARTS.PART_NAME RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM PARTS"; //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.PARTS_VDB,
            input,
            output, TRANSLATOR);
    }

    @Test public void testProcedureWithExpressionParameters() throws Exception {
        String expected = "{call proc(foo('1'),?)}"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor("create foreign procedure proc (param object, param2 integer); "
                + "create foreign function foo (param string) returns object;",
                "call proc(foo('1'), 1)",
                expected, TRANSLATOR);
    }

}
