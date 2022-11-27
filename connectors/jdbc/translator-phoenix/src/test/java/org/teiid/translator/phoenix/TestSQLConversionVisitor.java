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
package org.teiid.translator.phoenix;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.phoenix.PhoenixExecutionFactory;

@SuppressWarnings("nls")
public class TestSQLConversionVisitor {

    @Test
    public void testInsert() throws TranslatorException {
        String sql = "INSERT INTO Customer VALUES('106', 'Beijing', 'Kylin Soong', '$8000.00', 'Crystal Orange')";
        String expected = "UPSERT INTO \"Customer\" (ROW_ID, \"city\", \"name\", \"amount\", \"product\") VALUES ('106', 'Beijing', 'Kylin Soong', '$8000.00', 'Crystal Orange')";
        helpTest(sql, expected);

        sql = "INSERT INTO Customer(PK, city, name) VALUES ('109', 'Beijing', 'Kylin Soong')";
        expected = "UPSERT INTO \"Customer\" (ROW_ID, \"city\", \"name\") VALUES ('109', 'Beijing', 'Kylin Soong')";
        helpTest(sql, expected);
    }

    @Test
    public void testUpdate() throws TranslatorException {
        String sql = "update Customer set city = 'Beijing' where name = 'Kylin Soong'";
        String expected = "UPSERT INTO \"Customer\" (\"city\", ROW_ID) SELECT 'Beijing', \"Customer\".ROW_ID FROM \"Customer\" WHERE \"Customer\".\"name\" = 'Kylin Soong'";
        helpTest(sql, expected);

        sql = "UPDATE smalla SET StringKey = '55' WHERE smalla.StringKey IS NULL";
        expected = "UPSERT INTO smalla (stringkey) SELECT '55' FROM smalla WHERE smalla.stringkey IS NULL";
        helpTest(sql, expected);
    }

    @Test
    public void testSelect() throws TranslatorException {

        String sql = "SELECT * FROM Customer";
        String expected = "SELECT \"Customer\".ROW_ID, \"Customer\".\"city\", \"Customer\".\"name\", \"Customer\".\"amount\", \"Customer\".\"product\" FROM \"Customer\"";
        helpTest(sql, expected);

        sql = "SELECT city, amount FROM Customer";
        expected = "SELECT \"Customer\".\"city\", \"Customer\".\"amount\" FROM \"Customer\"";
        helpTest(sql, expected);

        sql = "SELECT DISTINCT city FROM Customer";
        expected = "SELECT DISTINCT \"Customer\".\"city\" FROM \"Customer\"";
        helpTest(sql, expected);

        sql = "SELECT city, amount FROM Customer WHERE PK='105'";
        expected = "SELECT \"Customer\".\"city\", \"Customer\".\"amount\" FROM \"Customer\" WHERE \"Customer\".ROW_ID = '105'";
        helpTest(sql, expected);

        sql = "SELECT city, amount FROM Customer WHERE PK='105' OR name='John White'";
        expected = "SELECT \"Customer\".\"city\", \"Customer\".\"amount\" FROM \"Customer\" WHERE \"Customer\".ROW_ID = '105' OR \"Customer\".\"name\" = 'John White'";
        helpTest(sql, expected);

        sql = "SELECT city, amount FROM Customer WHERE PK='105' AND name='John White'";
        expected = "SELECT \"Customer\".\"city\", \"Customer\".\"amount\" FROM \"Customer\" WHERE \"Customer\".ROW_ID = '105' AND \"Customer\".\"name\" = 'John White'";
        helpTest(sql, expected);
    }

    @Test
    public void testSelectOrderBy() throws TranslatorException {

        String sql = "SELECT * FROM Customer ORDER BY PK";
        String expected = "SELECT \"Customer\".ROW_ID, \"Customer\".\"city\", \"Customer\".\"name\", \"Customer\".\"amount\", \"Customer\".\"product\" FROM \"Customer\" ORDER BY \"Customer\".ROW_ID";
        helpTest(sql, expected);

        sql = "SELECT * FROM Customer ORDER BY PK ASC";
        expected = "SELECT \"Customer\".ROW_ID, \"Customer\".\"city\", \"Customer\".\"name\", \"Customer\".\"amount\", \"Customer\".\"product\" FROM \"Customer\" ORDER BY \"Customer\".ROW_ID";
        helpTest(sql, expected);

        sql = "SELECT * FROM Customer ORDER BY PK DESC";
        expected = "SELECT \"Customer\".ROW_ID, \"Customer\".\"city\", \"Customer\".\"name\", \"Customer\".\"amount\", \"Customer\".\"product\" FROM \"Customer\" ORDER BY \"Customer\".ROW_ID DESC";
        helpTest(sql, expected);

        sql = "SELECT * FROM Customer ORDER BY name, city DESC";
        expected = "SELECT \"Customer\".ROW_ID, \"Customer\".\"city\", \"Customer\".\"name\", \"Customer\".\"amount\", \"Customer\".\"product\" FROM \"Customer\" ORDER BY \"Customer\".\"name\", \"Customer\".\"city\" DESC";
        helpTest(sql, expected);
    }

    @Test
    public void testSelectGroupBy() throws TranslatorException{

        String sql = "SELECT COUNT(PK) FROM Customer WHERE name='John White'";
        String expected = "SELECT COUNT(\"Customer\".ROW_ID) FROM \"Customer\" WHERE \"Customer\".\"name\" = 'John White'";
        helpTest(sql, expected);

        sql = "SELECT name, COUNT(PK) FROM Customer GROUP BY name";
        expected = "SELECT \"Customer\".\"name\", COUNT(\"Customer\".ROW_ID) FROM \"Customer\" GROUP BY \"Customer\".\"name\"";
        helpTest(sql, expected);

        sql = "SELECT name, COUNT(PK) FROM Customer GROUP BY name HAVING COUNT(PK) > 1";
        expected = "SELECT \"Customer\".\"name\", COUNT(\"Customer\".ROW_ID) FROM \"Customer\" GROUP BY \"Customer\".\"name\" HAVING COUNT(\"Customer\".ROW_ID) > 1";
        helpTest(sql, expected);

        sql = "SELECT name, city, COUNT(PK) FROM Customer GROUP BY name, city";
        expected = "SELECT \"Customer\".\"name\", \"Customer\".\"city\", COUNT(\"Customer\".ROW_ID) FROM \"Customer\" GROUP BY \"Customer\".\"name\", \"Customer\".\"city\"";
        helpTest(sql, expected);

        sql = "SELECT name, city, COUNT(PK) FROM Customer GROUP BY name, city HAVING COUNT(PK) > 1";
        expected = "SELECT \"Customer\".\"name\", \"Customer\".\"city\", COUNT(\"Customer\".ROW_ID) FROM \"Customer\" GROUP BY \"Customer\".\"name\", \"Customer\".\"city\" HAVING COUNT(\"Customer\".ROW_ID) > 1";
        helpTest(sql, expected);
    }

    @Test
    public void testSelectLimit() throws TranslatorException {
        String sql = "SELECT * FROM Customer LIMIT 3";
        String expected = "SELECT \"Customer\".ROW_ID, \"Customer\".\"city\", \"Customer\".\"name\", \"Customer\".\"amount\", \"Customer\".\"product\" FROM \"Customer\" LIMIT 3";
        helpTest(sql, expected);

        sql = "SELECT * FROM Customer ORDER BY PK LIMIT 3";
        expected = "SELECT \"Customer\".ROW_ID, \"Customer\".\"city\", \"Customer\".\"name\", \"Customer\".\"amount\", \"Customer\".\"product\" FROM \"Customer\" ORDER BY \"Customer\".ROW_ID LIMIT 3";
        helpTest(sql, expected);
    }

    @Test
    public void testSelectLimit_2() throws TranslatorException {
        String sql = "SELECT product FROM Customer LIMIT 3, 3";
        String expected = "SELECT \"Customer\".\"product\" FROM \"Customer\" LIMIT 3 OFFSET 3";
        helpTest(sql, expected);
    }

    @Test
    public void testFuntionSubstring() throws TranslatorException {
        String sql = "SELECT SUBSTRING(q1, 10) FROM TypesTest";
        String expected = "SELECT SUBSTR(TypesTest.q1, 10) FROM TypesTest";
        helpTest(sql, expected);

        sql = "SELECT SUBSTRING(q1, 10, 5) FROM TypesTest";
        expected = "SELECT SUBSTR(TypesTest.q1, 10, 5) FROM TypesTest";
        helpTest(sql, expected);
    }

    @Test
    public void testFuntionUcaseLcase() throws TranslatorException {
        String sql = "SELECT UCASE(q1) FROM TypesTest";
        String expected = "SELECT UPPER(TypesTest.q1) FROM TypesTest";
        helpTest(sql, expected);

        sql = "SELECT LCASE(q1) FROM TypesTest";
        expected = "SELECT LOWER(TypesTest.q1) FROM TypesTest";
        helpTest(sql, expected);
    }

    @Test
    public void testFuntionLocate() throws TranslatorException {
        String sql = "SELECT LOCATE(q1, 'foo') FROM TypesTest";
        String expected = "SELECT INSTR(TypesTest.q1, 'foo') FROM TypesTest";
        helpTest(sql, expected);
    }

    @Test
    public void testFunctionCurtime() throws TranslatorException {
        String sql = "SELECT q1, CURTIME() FROM TypesTest";
        String expected = "SELECT TypesTest.q1, CURRENT_TIME() FROM TypesTest";
        helpTest(sql, expected);
    }

    @Test
    public void testBooleanLiterals() throws Exception {
        String sql = "SELECT true, false FROM Customer";
        String expected = "SELECT true, false FROM \"Customer\"";
        helpTest(sql, expected);
    }

    @Test
    public void testBigDecimalLiteral() throws Exception {
        String sql = "SELECT cast(1 as bigdecimal) FROM Customer";
        String expected = "SELECT 1.0 FROM \"Customer\"";
        helpTest(sql, expected);
    }

    @Test
    public void testDateTimeLiterals() throws Exception {
        String sql = "SELECT {d '2001-01-01'}, {t '23:00:02'}, {ts '2004-02-01 11:11:11.001'} FROM Customer";
        String expected = "SELECT DATE '2001-01-01 00:00:00.0', TIME '1970-01-01 23:00:02.0', TIMESTAMP '2004-02-01 11:11:11.001' FROM \"Customer\"";
        helpTest(sql, expected);
    }

    @Test
    public void testLikeEscape() throws TranslatorException {
        String sql = "SELECT city FROM Customer where name like '\\_%' escape '\\'";
        String expected = "SELECT \"Customer\".\"city\" FROM \"Customer\" WHERE \"Customer\".\"name\" LIKE '\\_%'";
        helpTest(sql, expected);
    }

    @Test
    public void testUnion() throws TranslatorException {
        String sql = "SELECT city as c1 FROM Customer union SELECT name FROM Customer";
        String expected = "SELECT DISTINCT c1 FROM (SELECT \"Customer\".\"city\" AS c1 FROM \"Customer\" UNION ALL SELECT \"Customer\".\"name\" FROM \"Customer\") AS x";
        helpTest(sql, expected);
    }

    @Test
    public void testCorrelatedIn() throws TranslatorException {
        String sql = "SELECT city as c1 FROM Customer where name in (select name from customer cu where cu.city = customer.city)";
        String expected = "SELECT \"Customer\".\"city\" AS c1 FROM \"Customer\" WHERE \"Customer\".\"name\" = SOME (SELECT cu.\"name\" FROM \"Customer\" AS cu WHERE cu.\"city\" = \"Customer\".\"city\")";
        helpTest(sql, expected);
        sql = "SELECT city as c1 FROM Customer where name not in (select name from customer cu where cu.city = customer.city)";
        expected = "SELECT \"Customer\".\"city\" AS c1 FROM \"Customer\" WHERE \"Customer\".\"name\" <> ALL (SELECT cu.\"name\" FROM \"Customer\" AS cu WHERE cu.\"city\" = \"Customer\".\"city\")";
        helpTest(sql, expected);
    }

    private static TranslationUtility translationUtility = new TranslationUtility(TestPhoenixUtil.queryMetadataInterface());

    private void helpTest(String sql, String expected) throws TranslatorException  {

        Command command = translationUtility.parseCommand(sql);

        PhoenixExecutionFactory ef = new PhoenixExecutionFactory();
        ef.start();

        SQLConversionVisitor vistor = ef.getSQLConversionVisitor();
        vistor.append(command);

        assertEquals(expected, vistor.toString());

    }

}
