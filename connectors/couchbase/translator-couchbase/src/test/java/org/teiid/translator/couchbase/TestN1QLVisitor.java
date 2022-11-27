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
package org.teiid.translator.couchbase;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestN1QLVisitor extends TestVisitor {

    static void helpTest(String sql, String key) throws TranslatorException {

        String actual = helpTranslate(sql);

        if(PRINT_TO_CONSOLE.booleanValue()) {
            System.out.println(actual);
        }

        if(REPLACE_EXPECTED.booleanValue()) {
            N1QL.put(key.toString(), actual);
        }

        assertEquals(key, N1QL.get(key), actual);
    }

    static String helpTranslate(String sql) {
        Command command = translationUtility.parseCommand(sql);

        N1QLVisitor visitor = TRANSLATOR.getN1QLVisitor();
        visitor.append(command);
        String actual = visitor.toString();
        return actual;
    }

    @Test
    public void testSelect() throws TranslatorException {

        String sql = "SELECT * FROM Customer";
        helpTest(sql, "N1QL0101");

        sql = "SELECT * FROM Customer_SavedAddresses";
        helpTest(sql, "N1QL0102");

        sql = "SELECT * FROM Oder";
        helpTest(sql, "N1QL0103");

        sql = "SELECT * FROM Oder_Items";
        helpTest(sql, "N1QL0104");

        sql = "SELECT DISTINCT Name FROM Customer";
        helpTest(sql, "N1QL0105");

        sql = "SELECT ALL Name FROM Customer";
        helpTest(sql, "N1QL0106");

        sql = "SELECT CreditCard_CardNumber, CreditCard_Type, CreditCard_CVN, CreditCard_Expiry FROM Oder";
        helpTest(sql, "N1QL0107");
    }

    @Test
    public void testSelect_1() throws TranslatorException {

        String sql = "SELECT 1 AS c_0 FROM Customer WHERE documentID = 'customer-3' LIMIT 1";
        helpTest(sql, "N1QL0108");

        sql = "SELECT COUNT(*) AS count FROM Customer WHERE ID = 'Customer_12345'";
        helpTest(sql, "N1QL0109");

        sql = "SELECT couchbase.CLOCK_MILLIS() FROM Oder WHERE CustomerID = 'Customer_12345' AND CreditCard_Type = 'Visa' AND CreditCard_CVN = 123";
        helpTest(sql, "N1QL0110");
    }

    @Test
    public void testNestedJson() throws TranslatorException  {

        String sql = "SELECT * FROM T3";
        helpTest(sql, "N1QL0201");

        sql = "SELECT nestedJson_nestedJson_nestedJson_Dimension FROM T3";
        helpTest(sql, "N1QL0202");
    }

    @Test
    public void testNestedArray() throws TranslatorException {

        String sql = "SELECT * FROM T3";
        helpTest(sql, "N1QL0301");

        sql = "SELECT * FROM T3_nestedArray";
        helpTest(sql, "N1QL0302");

        sql = "SELECT * FROM T3_nestedArray_dim2";
        helpTest(sql, "N1QL0303");

        sql = "SELECT * FROM T3_nestedArray_dim2_dim3";
        helpTest(sql, "N1QL0304");

        sql = "SELECT * FROM T3_nestedArray_dim2_dim3_dim4";
        helpTest(sql, "N1QL0305");

        sql = "SELECT T3_nestedArray_dim2_dim3_dim4_idx, T3_nestedArray_dim2_dim3_dim4 FROM T3_nestedArray_dim2_dim3_dim4";
        helpTest(sql, "N1QL0306");
    }

    @Test
    public void testPKColumn() throws TranslatorException {

        String sql = "SELECT documentID FROM T3";
        helpTest(sql, "N1QL0401");

        sql = "SELECT documentID FROM T3_nestedArray_dim2_dim3_dim4";
        helpTest(sql, "N1QL0402");
    }

    @Test
    public void testLimitOffsetClause() throws TranslatorException {

        String sql = "SELECT Name FROM Customer LIMIT 2";
        helpTest(sql, "N1QL0501");

        sql = "SELECT Name FROM Customer LIMIT 2, 2";
        helpTest(sql, "N1QL0502");

        sql = "SELECT Name FROM Customer OFFSET 2 ROWS";
        helpTest(sql, "N1QL0503");
    }

    @Test
    public void testOrderByClause() throws TranslatorException {

        String sql = "SELECT Name, type FROM Customer ORDER BY Name";
        helpTest(sql, "N1QL0601");

        sql = "SELECT type FROM Customer ORDER BY Name"; //Unrelated
        helpTest(sql, "N1QL0602");

        sql = "SELECT Name, type FROM Customer ORDER BY type"; //NullOrdering
        helpTest(sql, "N1QL0603");

        sql = "SELECT Name, type FROM Customer ORDER BY Name ASC";
        helpTest(sql, "N1QL0604");

        sql = "SELECT Name, type FROM Customer ORDER BY Name DESC";
        helpTest(sql, "N1QL0605");

        sql = "SELECT id as a FROM Customer ORDER BY a";
        helpTest(sql, "N1QL0606");
    }

    @Test
    public void testGroupByClause() throws TranslatorException {

        String sql = "SELECT Name, COUNT(*) FROM Customer GROUP BY Name";
        helpTest(sql, "N1QL0701");
    }

    @Test
    public void testWhereClause() throws TranslatorException {

        String sql = "SELECT Name, type  FROM Customer WHERE Name = 'John Doe'";
        helpTest(sql, "N1QL0801");

        sql = "SELECT Name, type  FROM Customer WHERE documentID = 'customer'";
        helpTest(sql, "N1QL0802");

        sql = "SELECT Name, type  FROM Customer WHERE type = 'Customer'";
        helpTest(sql, "N1QL0803");

        sql = "SELECT Name FROM Customer";
        helpTest(sql, "N1QL0804");

        sql = "SELECT Name FROM Customer WHERE documentID = 'customer'";
        helpTest(sql, "N1QL0805");

        sql = "SELECT * FROM Oder WHERE CustomerID = 'Customer_12345' AND CreditCard_Type = 'Visa' AND CreditCard_CVN = 123";
        helpTest(sql, "N1QL0806");

        sql = "SELECT CreditCard_CardNumber, CreditCard_Expiry, Name FROM Oder WHERE CustomerID = 'Customer_12345' AND CreditCard_Type = 'Visa' AND CreditCard_CVN = 123";
        helpTest(sql, "N1QL0807");

        sql = "SELECT Name, type  FROM Customer WHERE type = 'Customer?'";
        helpTest(sql, "N1QL0808");
    }

    @Test // test where clause against array
    public void testWhereClause_array() throws TranslatorException {

        String sql = "SELECT * FROM Oder_Items WHERE documentID = 'order-1'"; //only documentID
        helpTest(sql, "N1QL0811");

        sql = "SELECT * FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_Quantity = 1";
        helpTest(sql, "N1QL0812");

        sql = "SELECT * FROM Oder_Items WHERE Oder_Items_Quantity = 1";
        helpTest(sql, "N1QL0813");

        sql = "SELECT * FROM Oder_Items WHERE Oder_Items_Quantity = 1 AND Oder_Items_ItemID = 89123"; // only nested object columns of array
        helpTest(sql, "N1QL0814");

        sql = "SELECT * FROM Oder_Items WHERE Oder_Items_idx = 1"; // only array index
        helpTest(sql, "N1QL0815");

        sql = "SELECT * FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 1 AND Oder_Items_Quantity = 5 AND Oder_Items_ItemID = 92312"; //all columns
        helpTest(sql, "N1QL0816");

        sql = "SELECT Oder_Items_Quantity FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 1 AND Oder_Items_ItemID = 92312"; // unrelated nested object columns of array
        helpTest(sql, "N1QL0817");
    }

    @Test
    public void testStringFunctions() throws TranslatorException {

        String sql = "SELECT LCASE(attr_string) FROM T2";
        helpTest(sql, "N1QL0901");

        sql = "SELECT UCASE(attr_string) FROM T2";
        helpTest(sql, "N1QL0902");

        sql = "SELECT replace(attr_string, 'xy', 'z') FROM T2";
        helpTest(sql, "N1QL0903");

        sql = "SELECT couchbase.CONTAINS(attr_string, 'is') FROM T2";
        helpTest(sql, "N1QL0904");

        sql = "SELECT couchbase.TITLE(attr_string) FROM T2";
        helpTest(sql, "N1QL0905");

        sql = "SELECT couchbase.LTRIM(attr_string, 'This') FROM T2";
        helpTest(sql, "N1QL0906");

        sql = "SELECT couchbase.TRIM(attr_string, 'is') FROM T2";
        helpTest(sql, "N1QL0907");

        sql = "SELECT couchbase.RTRIM(attr_string, 'value') FROM T2";
        helpTest(sql, "N1QL0908");

        sql = "SELECT couchbase.POSITION(attr_string, 'is') FROM T2";
        helpTest(sql, "N1QL0909");

        sql = "SELECT substring(attr_string, 1, 2) FROM T2";
        helpTest(sql, "N1QL0910");

        sql = "SELECT substring(attr_string, -1, 2) FROM T2";
        helpTest(sql, "N1QL0911");
    }

    @Test
    public void testNumbericFunctions() throws TranslatorException {

        String sql = "SELECT CEILING(attr_double) FROM T2";
        helpTest(sql, "N1QL1001");

        sql = "SELECT LOG(attr_double) FROM T2";
        helpTest(sql, "N1QL1002");

        sql = "SELECT LOG10(attr_double) FROM T2";
        helpTest(sql, "N1QL1003");

        sql = "SELECT RAND(attr_integer) FROM T2";
        helpTest(sql, "N1QL1004");
    }

    @Test
    public void testConversionFunctions() throws TranslatorException {

        String sql = "SELECT convert(attr_long, string) FROM T2";
        helpTest(sql, "N1QL1101");

        sql = "SELECT convert(attr_string, float) AS FloatNum, convert(attr_string, long) AS LongNum, convert(attr_string, double) AS DoubleNum, convert(attr_string, byte) AS ByteNum, convert(attr_string, short) AS ShortValue FROM T2";
        helpTest(sql, "N1QL1102");

        sql = "SELECT convert(attr_integer, varchar) AS CharValue FROM T2";
        helpTest(sql, "N1QL1103");

        sql = "SELECT convert(attr_string, biginteger) AS BigIntegerValue, convert(attr_string, bigdecimal) AS BigDecimalValue FROM T2";
        helpTest(sql, "N1QL1104");

        sql = "SELECT convert(attr_string, object) AS ObjectValue FROM T2";
        helpTest(sql, "N1QL1105");

        sql = "SELECT convert(attr_string, object), convert(attr_integer, object), convert(attr_boolean, object) FROM T2";
        helpTest(sql, "N1QL1106");
    }

    @Test
    public void testDateFunctions() throws TranslatorException {

        String sql = "SELECT couchbase.CLOCK_MILLIS() FROM T2";
        helpTest(sql, "N1QL1201");

        sql = "SELECT couchbase.CLOCK_STR() FROM T2";
        helpTest(sql, "N1QL1202");

        sql = "SELECT couchbase.CLOCK_STR('2006-01-02') FROM T2";
        helpTest(sql, "N1QL1203");

        sql = "SELECT couchbase.DATE_ADD_MILLIS(1488873653696, 2, 'century') FROM T2";
        helpTest(sql, "N1QL1204");

        sql = "SELECT couchbase.DATE_ADD_STR('2017-03-08', 2, 'century') FROM T2";
        helpTest(sql, "N1QL1205");
    }

    @Test
    public void testProcedures() throws TranslatorException {

        String sql = "call getDocuments('customer', 'test')";
        helpTest(sql, "N1QL1301");

        sql = "call getDocument('customer', 'test')";
        helpTest(sql, "N1QL1302");
    }

    @Test
    public void testSetOperations() throws TranslatorException {
        String sql = "select Name FROM Customer intersect select attr_string from T2";
        helpTest(sql, "N1QL3001");

        sql = "select Name FROM Customer union all select attr_string from T2";
        helpTest(sql, "N1QL3002");

        sql = "select Name FROM Customer union all select attr_string from T2 order by name limit 2";
        helpTest(sql, "N1QL3003");

        sql = "select Name FROM Customer except (select attr_string from T2 order by name limit 2)";
        helpTest(sql, "N1QL3004");
    }


}
