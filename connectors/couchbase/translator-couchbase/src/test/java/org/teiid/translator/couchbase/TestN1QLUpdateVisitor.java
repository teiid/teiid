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
import static org.teiid.translator.couchbase.TestCouchbaseMetadataProcessor.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.Command;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Parameter;
import org.teiid.translator.TranslatorException;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

@SuppressWarnings("nls")
public class TestN1QLUpdateVisitor extends TestVisitor {

    private void helpTest(String sql, String key) throws TranslatorException {
        String actual = helpTranslate(sql);

        if(PRINT_TO_CONSOLE.booleanValue()) {
            System.out.println(actual);
        }

        if(REPLACE_EXPECTED.booleanValue()) {
            N1QL.put(key.toString(), actual);
        }

        assertEquals(key, N1QL.get(key), actual);
    }

    private String helpTranslate(String sql) {
        Command command = translationUtility.parseCommand(sql);

        N1QLUpdateVisitor visitor = TRANSLATOR.getN1QLUpdateVisitor();
        visitor.append(command);
        String actual = visitor.toString();
        return actual;
    }

    @Test
    public void testInsert() throws TranslatorException  {

        String sql = "INSERT INTO Customer (documentID, ID, type, Name) VALUES ('customer-1', 'Customer_12346', 'Customer', 'Kylin Soong')";
        helpTest(sql, "N1QL1401");

        sql = "INSERT INTO Customer (documentID, ID, Name, type) VALUES ('customer-1', 'Customer_12346', 'Kylin Soong', 'Customer')";
        helpTest(sql, "N1QL1402");

        sql = "INSERT INTO Oder (documentID, CustomerID, type, CreditCard_CardNumber, CreditCard_Type, CreditCard_CVN, CreditCard_Expiry, Name) VALUES ('order-1', 'Customer_12345', 'Oder', '4111 1111 1111 111', 'Visa', 123, '12/12', 'Air Ticket')";
        String translated = helpTranslate(sql);
        JsonObject json = JsonObject.create();
        json.put("type", "Oder");
        json.put("Name", "Air Ticket");
        json.put("CustomerID", "Customer_12345");
        json.put("CreditCard", JsonObject.create().put("CardNumber", "4111 1111 1111 111").put("Expiry", "12/12").put("CVN", 123).put("Type", "Visa"));
        assertEquals("INSERT INTO `test` (KEY, VALUE) VALUES ('order-1', "+json+") RETURNING META(`test`).id AS PK", translated);

        sql = "INSERT INTO Customer (ID, Name, type) VALUES ('Customer_12346', 'Kylin Soong', 'Customer')";
        try {
            helpTest(sql, "N1QL1405");
        } catch (Exception e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "INSERT INTO Customer (documentID) VALUES ('customer-1')"; // empty document, type should be added though
        helpTest(sql, "N1QL1406");

        sql = "INSERT INTO Oder (CustomerID, type, CreditCard_CardNumber, CreditCard_Type, CreditCard_CVN, CreditCard_Expiry, Name) VALUES ('Customer_12345', 'Oder', '4111 1111 1111 111', 'Visa', 123, '12/12', 'Air Ticket')";
        try {
            helpTest(sql, "N1QL1407");
        } catch (Exception e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "INSERT INTO Oder (documentID) VALUES ('order-1')"; // empty document
        helpTest(sql, "N1QL1408");

        sql = "INSERT INTO Customer_SavedAddresses VALUES ('customer-1', 2,  'Beijing')";
        helpTest(sql, "N1QL1409");

        sql = "INSERT INTO Customer_SavedAddresses (documentID, Customer_SavedAddresses_idx, Customer_SavedAddresses) VALUES ('customer-1', 2,  'Beijing')";
        helpTest(sql, "N1QL1410");

        sql = "INSERT INTO Oder_Items (documentID, Oder_Items_idx, Oder_Items_Quantity, Oder_Items_ItemID) VALUES ('order-1', 2, 5, 92312)";
        translated = helpTranslate(sql);
        json = JsonObject.create();
        json.put("Quantity", 5);
        json.put("ItemID", 92312);
        assertEquals("UPDATE `test` USE KEYS 'order-1' SET `Items` = ARRAY_CONCAT(IFMISSINGORNULL(`Items`, []), ["+json+"]) RETURNING META(`test`).id AS PK", translated);
    }

    @Test
    public void testInsertBulk()  {

        String sql = "INSERT INTO Customer (documentID) VALUES ('customer-1')";
        Insert insert = (Insert)translationUtility.parseCommand(sql);
        Parameter p = new Parameter();
        p.setType(String.class);
        p.setValueIndex(0);
        ((ExpressionValueSource)insert.getValueSource()).getValues().set(0, p);
        insert.setParameterValues(Arrays.asList(Arrays.asList("customer-1"), Arrays.asList("customer-2")).iterator());

        N1QLUpdateVisitor visitor = TRANSLATOR.getN1QLUpdateVisitor();
        visitor.append(insert);
        String actual = visitor.getBulkCommands()[0];
        assertEquals("INSERT INTO `test` (KEY, VALUE) VALUES ('customer-1', {\"type\":\"Customer\"}), VALUES ('customer-2', {\"type\":\"Customer\"}) RETURNING META(`test`).id AS PK", actual);
    }

    @Test
    public void testInsertNestedArray() throws TranslatorException {

        String sql = "INSERT INTO T3_nestedArray_dim2_dim3_dim4 VALUES ('nestedArray', 1, 1, 1, 1, 'Hello World')";
        helpTest(sql, "N1QL1501");

        sql = "INSERT INTO T3_nestedArray_dim2_dim3_dim4 (documentID, T3_nestedArray_idx, T3_nestedArray_dim2_idx, T3_nestedArray_dim2_dim3_idx, T3_nestedArray_dim2_dim3_dim4_idx, T3_nestedArray_dim2_dim3_dim4) VALUES ('nestedArray', 1, 1, 1, 1, 'Hello World')";
        helpTest(sql, "N1QL1502");

        sql = "INSERT INTO T3_nestedArray_dim2_dim3 (documentID, T3_nestedArray_idx, T3_nestedArray_dim2_idx, T3_nestedArray_dim2_dim3_idx, T3_nestedArray_dim2_dim3) VALUES ('nestedArray', 1, 1, 1, 'Hello World')";
        helpTest(sql, "N1QL1503");

        sql = "INSERT INTO T3_nestedArray_dim2_dim3 VALUES ('nestedArray', 1, 1, 1, 'Hello World')";
        helpTest(sql, "N1QL1504");

        sql = "INSERT INTO T3_nestedArray_dim2 (documentID, T3_nestedArray_idx, T3_nestedArray_dim2_idx, T3_nestedArray_dim2) VALUES ('nestedArray', 1, 1, 'Hello World')";
        helpTest(sql, "N1QL1505");

        sql = "INSERT INTO T3_nestedArray_dim2 VALUES ('nestedArray', 1, 1, 'Hello World')";
        helpTest(sql, "N1QL1506");

        sql = "INSERT INTO T3_nestedArray (documentID, T3_nestedArray_idx, T3_nestedArray) VALUES ('nestedArray', 1, 'Hello World')";
        helpTest(sql, "N1QL1507");

        sql = "INSERT INTO T3_nestedArray VALUES ('nestedArray', 1, 'Hello World')";
        helpTest(sql, "N1QL1508");

        sql = "INSERT INTO T3_nestedArray_dim2 (T3_nestedArray_idx, T3_nestedArray_dim2_idx, T3_nestedArray_dim2) VALUES (1, 1, 'Hello World')";
        try {
            helpTest(sql, "N1QL1509");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "INSERT INTO T3_nestedArray_dim2 (documentID, T3_nestedArray_idx, T3_nestedArray_dim2) VALUES ('nestedArray', 1, 'Hello World')";
        try {
            helpTest(sql, "N1QL1510");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }
    }

    @Test
    public void testDelete() throws TranslatorException {

        String sql = "DELETE FROM Customer WHERE documentID = 'customer-5'";
        helpTest(sql, "N1QL1601");

        sql = "DELETE FROM Customer WHERE documentID = 'customer-5' AND ID = 'Customer_10000' AND type = 'Customer'";
        helpTest(sql, "N1QL1602");

        sql = "DELETE FROM Customer WHERE ID = 'Customer_10000'";
        helpTest(sql, "N1QL1603");

        sql = "DELETE FROM Customer WHERE ID = 'Customer_10000' AND type = 'Customer'";
        helpTest(sql, "N1QL1604");

        sql = "DELETE FROM Customer WHERE ID = 'Customer_10000' AND type = 'Customer' AND Name = 'Kylin Soong'";
        helpTest(sql, "N1QL1605");

        sql = "DELETE FROM Customer_SavedAddresses WHERE documentID = 'customer-3' AND Customer_SavedAddresses_idx = 1";
        helpTest(sql, "N1QL1606");

        sql = "DELETE FROM Customer_SavedAddresses WHERE Customer_SavedAddresses_idx = 2";
        try {
            helpTest(sql, "N1QL1607");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "DELETE FROM Oder WHERE documentID = 'order-3'";
        helpTest(sql, "N1QL1608");

        sql = "DELETE FROM Oder WHERE documentID = 'order-3' AND CustomerID = 'Customer_12346' AND Name = 'Air Ticket' AND type = 'Order' AND CreditCard_CVN = 123 AND CreditCard_CardNumber = '4111 1111 1111 111' AND CreditCard_Expiry = '12/12' AND CreditCard_Type = 'Visa'";
        helpTest(sql, "N1QL1609");

        sql = "DELETE FROM Oder WHERE CustomerID = 'Customer_12346'";
        helpTest(sql, "N1QL1610");

        sql = "DELETE FROM Oder WHERE CustomerID = 'Customer_12346' AND Name = 'Air Ticket' AND type = 'Order' AND CreditCard_CVN = 123 AND CreditCard_CardNumber = '4111 1111 1111 111' AND CreditCard_Expiry = '12/12' AND CreditCard_Type = 'Visa'";
        helpTest(sql, "N1QL1611");

        sql = "DELETE FROM Oder_Items WHERE documentID = 'order-3' AND Oder_Items_idx = 2";
        helpTest(sql, "N1QL1612");

        sql = "DELETE FROM Oder_Items WHERE Oder_Items_idx = 2";
        try {
            helpTest(sql, "N1QL1613");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }
    }

    @Test
    public void testDelete_1() throws TranslatorException {

        String sql = "DELETE FROM Oder WHERE documentID = 'order-3' AND (CreditCard_Type = 'Visa' OR CreditCard_CVN > 100)";
        helpTest(sql, "N1QL1621");

        sql = "DELETE FROM Oder WHERE CustomerID = 'Customer_12346' AND (CreditCard_Type = 'Visa' OR CreditCard_CVN > 100)";
        helpTest(sql, "N1QL1622");

        sql = "DELETE FROM Oder WHERE (CreditCard_CardNumber = '4111 1111 1111 111' OR CreditCard_Type = 'Visa') AND (CreditCard_CVN > 100 OR CreditCard_Expiry = '12/12') AND CustomerID = 'Customer_12346'";
        helpTest(sql, "N1QL1623");

        sql = "DELETE FROM Oder WHERE (CreditCard_CardNumber = '4111 1111 1111 111' AND CreditCard_Type = 'Visa') OR (CreditCard_CVN > 100 AND CreditCard_Expiry = '12/12') OR CustomerID = 'Customer_12346'";
        helpTest(sql, "N1QL1624");
    }

    @Test
    public void testDelete_2() throws TranslatorException {

        String sql = "DELETE FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123 AND Oder_Items_Quantity = 1";
        helpTest(sql, "N1QL1631");

        sql = "DELETE FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123";
        helpTest(sql, "N1QL1632");

        sql = "DELETE FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND (Oder_Items_ItemID = 89123 OR Oder_Items_Quantity = 1)";
        helpTest(sql, "N1QL1633");

        sql = "DELETE FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID > 80000 AND Oder_Items_Quantity > 0";
        helpTest(sql, "N1QL1634");

        sql = "DELETE FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND (Oder_Items_ItemID > 80000 OR Oder_Items_Quantity > 0)";
        helpTest(sql, "N1QL1635");

        sql = "UDELETE FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx > 0";
        try {
            helpTest(sql, "N1QL1636");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "DELETE FROM Oder_Items WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID > 80000 OR Oder_Items_Quantity > 0";
        try {
            helpTest(sql, "N1QL1637");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }
    }

    @Test
    public void testDeleteNestedArray() throws TranslatorException {

        String sql = "DELETE FROM T3_nestedArray_dim2_dim3_dim4 WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1 AND T3_nestedArray_dim2_dim3_dim4_idx = 3";
        helpTest(sql, "N1QL1701");

        sql = "DELETE FROM T3_nestedArray_dim2_dim3_dim4 WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1 AND T3_nestedArray_dim2_dim3_dim4_idx = 1";
        helpTest(sql, "N1QL1702");

        sql = "DELETE FROM T3_nestedArray_dim2_dim3_dim4 WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1 AND T3_nestedArray_dim2_dim3_dim4_idx = 0";
        helpTest(sql, "N1QL1703");

        sql = "DELETE FROM T3_nestedArray_dim2_dim3 WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1";
        helpTest(sql, "N1QL1704");

        sql = "DELETE FROM T3_nestedArray_dim2_dim3 WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 0";
        helpTest(sql, "N1QL1705");

        sql = "DELETE FROM T3_nestedArray_dim2 WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1";
        helpTest(sql, "N1QL1706");

        sql = "DELETE FROM T3_nestedArray_dim2 WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 0";
        helpTest(sql, "N1QL1707");

        sql = "DELETE FROM T3_nestedArray WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1";
        helpTest(sql, "N1QL1708");

        sql = "DELETE FROM T3_nestedArray WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 0";
        helpTest(sql, "N1QL1709");

        sql = "DELETE FROM T3_nestedArray_dim2_dim3_dim4 WHERE T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1 AND T3_nestedArray_dim2_dim3_dim4_idx = 1";
        try {
            helpTest(sql, "N1QL1710");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "DELETE FROM T3_nestedArray_dim2_dim3 WHERE T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1";
        try {
            helpTest(sql, "N1QL1711");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "DELETE FROM T3_nestedArray_dim2 WHERE T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1";
        try {
            helpTest(sql, "N1QL1712");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "DELETE FROM T3_nestedArray WHERE T3_nestedArray_idx = 1";
        try {
            helpTest(sql, "N1QL1713");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }
    }

    @Test
    public void testUpdate() throws TranslatorException {

        String sql = "UPDATE Customer SET Name = 'John Doe' WHERE documentID = 'customer-5'";
        helpTest(sql, "N1QL1801");

        sql = "UPDATE Customer SET Name = 'John Doe' WHERE documentID = 'customer-5' AND ID = 'Customer_10000' AND Name = 'Kylin Soong' AND type = 'Customer'";
        helpTest(sql, "N1QL1802");

        sql = "UPDATE Customer SET Name = 'John Doe' WHERE ID = 'Customer_10000' AND Name = 'Kylin Soong' AND type = 'Customer'";
        helpTest(sql, "N1QL1803");

        sql = "UPDATE Customer SET Name = 'John Doe' WHERE ID = 'Customer_10000'";
        helpTest(sql, "N1QL1804");

        sql = "UPDATE Oder SET CreditCard_CVN = 100 WHERE documentID = 'order-3'" ;
        helpTest(sql, "N1QL1805");

        sql = "UPDATE Oder SET CreditCard_CVN = 100 WHERE CustomerID = 'Customer_12346'" ;
        helpTest(sql, "N1QL1806");

        sql = "UPDATE Oder SET CreditCard_CVN = 100, CreditCard_CardNumber = '4111 1111 1111 112', CreditCard_Expiry = '14/12' WHERE CustomerID = 'Customer_12346'" ;
        helpTest(sql, "N1QL1807");

        sql = "UPDATE Oder SET CreditCard_CVN = 100, CreditCard_CardNumber = '4111 1111 1111 111', CreditCard_Expiry = '12/12' WHERE documentID = 'order-3' AND CustomerID = 'Customer_12346'" ;
        helpTest(sql, "N1QL1808");

        sql = "UPDATE Customer_SavedAddresses SET Customer_SavedAddresses = 'Beijing' WHERE documentID = 'customer-5' AND Customer_SavedAddresses_idx = 0";
        helpTest(sql, "N1QL1809");

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000, Oder_Items_Quantity = 10 WHERE documentID = 'order-3' AND Oder_Items_idx = 0";
        helpTest(sql, "N1QL1810");
    }

    @Test
    public void testUpdate_1() throws TranslatorException {

        String sql = "UPDATE Customer SET Name = ucase(documentID) WHERE documentID = 'customer-5'";
        helpTest(sql, "N1QL1811");

        sql = "UPDATE Customer SET Name = ucase(documentID) WHERE ID = 'Customer_12345'";
        helpTest(sql, "N1QL1812");

        sql = "UPDATE Customer SET Name = type WHERE documentID = 'customer-5'";
        helpTest(sql, "N1QL1813");

        sql = "UPDATE Customer SET Name = type WHERE ID = 'Customer_12345'";
        helpTest(sql, "N1QL1814");
    }

    @Test(expected=TeiidRuntimeException.class) public void testUpdateTypeFails() throws TranslatorException {
        String sql = "UPDATE Customer SET type = 'not customer'";
        helpTest(sql, "N1QL2100");
    }

    @Test(expected=TeiidRuntimeException.class)
    public void testUpdatePk() throws TranslatorException {
        String sql = "UPDATE Customer SET documentid = 'x' WHERE documentID = 'customer-5'";
        helpTest(sql, "error");
    }

    @Test
    public void testUpdate_2() throws TranslatorException {

        String sql = "UPDATE Oder SET Name = 'Train Ticket' WHERE documentID = 'order-3' AND (CreditCard_Type = 'Visa' OR CreditCard_CVN > 100)";
        helpTest(sql, "N1QL1815");

        sql = "UPDATE Oder SET Name = 'Train Ticket' WHERE CustomerID = 'Customer_12346' AND (CreditCard_Type = 'Visa' OR CreditCard_CVN > 100)";
        helpTest(sql, "N1QL1816");

        sql = "UPDATE Oder SET Name = 'Train Ticket' WHERE (CreditCard_CardNumber = '4111 1111 1111 111' OR CreditCard_Type = 'Visa') AND (CreditCard_CVN > 100 OR CreditCard_Expiry = '12/12') AND CustomerID = 'Customer_12346'";
        helpTest(sql, "N1QL1817");

        sql = "UPDATE Oder SET Name = 'Train Ticket' WHERE (CreditCard_CardNumber = '4111 1111 1111 111' AND CreditCard_Type = 'Visa') OR (CreditCard_CVN > 100 AND CreditCard_Expiry = '12/12') OR CustomerID = 'Customer_12346'";
        helpTest(sql, "N1QL1818");
    }

    @Test
    public void testUpdateNestedArray() throws TranslatorException {

        String sql = "UPDATE T3_nestedArray_dim2_dim3_dim4 SET T3_nestedArray_dim2_dim3_dim4 = 'Hello Teiid' WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1 AND T3_nestedArray_dim2_dim3_dim4_idx = 3";
        helpTest(sql, "N1QL1901");

        sql = "UPDATE T3_nestedArray_dim2_dim3 SET T3_nestedArray_dim2_dim3 = 'Hello Teiid' WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1 AND T3_nestedArray_dim2_dim3_idx = 1";
        helpTest(sql, "N1QL1902");

        sql = "UPDATE T3_nestedArray_dim2 SET T3_nestedArray_dim2 = 'Hello Teiid' WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1 AND T3_nestedArray_dim2_idx = 1";
        helpTest(sql, "N1QL1903");

        sql = "UPDATE T3_nestedArray SET T3_nestedArray = 'Hello Teiid' WHERE documentID = 'nestedArray' AND T3_nestedArray_idx = 1";
        helpTest(sql, "N1QL1904");
    }

    @Test
    public void testUpdateNestedArray_1() throws TranslatorException {

        String sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000, Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123 AND Oder_Items_Quantity = 1";
        helpTest(sql, "N1QL1911");

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123 AND Oder_Items_Quantity = 1";
        helpTest(sql, "N1QL1912");

        sql = "UPDATE Oder_Items SET Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123 AND Oder_Items_Quantity = 1";
        helpTest(sql, "N1QL1913");

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000, Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123";
        helpTest(sql, "N1QL1914");

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123";
        helpTest(sql, "N1QL1915");

        sql = "UPDATE Oder_Items SET Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID = 89123";
        helpTest(sql, "N1QL1916");

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000, Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID > 80000 AND Oder_Items_Quantity > 0";
        helpTest(sql, "N1QL1917");

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000, Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND (Oder_Items_ItemID > 80000 OR Oder_Items_Quantity > 0)";
        helpTest(sql, "N1QL1918");

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000, Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx > 0";
        try {
            helpTest(sql, "N1QL1919");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

        sql = "UPDATE Oder_Items SET Oder_Items_ItemID = 80000, Oder_Items_Quantity = 10 WHERE documentID = 'order-1' AND Oder_Items_idx = 0 AND Oder_Items_ItemID > 80000 OR Oder_Items_Quantity > 0";
        try {
            helpTest(sql, "N1QL1920");
        } catch (TeiidRuntimeException e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }

    }

    @Test
    public void testUpdateNestedArray_2() throws TranslatorException {

        String sql = "UPDATE Oder_Items SET Oder_Items_ItemID = Oder_Items_Quantity WHERE documentID = 'order-3' AND Oder_Items_idx = 0";
        helpTest(sql, "N1QL1921");

        sql = "UPDATE Customer_SavedAddresses SET Customer_SavedAddresses = ucase(documentID) WHERE documentID = 'customer-5' AND Customer_SavedAddresses_idx = 0";
        helpTest(sql, "N1QL1922");
    }

    @Test
    public void testUpsert() throws TranslatorException  {

        String sql = "UPSERT INTO Customer (documentID, ID, type, Name) VALUES ('customer-1', 'Customer_12346', 'Customer', 'Kylin Soong')";
        helpTest(sql, "N1QL2001");

        sql = "UPSERT INTO Customer (documentID, ID, Name, type) VALUES ('customer-1', 'Customer_12346', 'Kylin Soong', 'Customer')";
        helpTest(sql, "N1QL2002");

        sql = "UPSERT INTO Oder (documentID, CustomerID, type, CreditCard_CardNumber, CreditCard_Type, CreditCard_CVN, CreditCard_Expiry, Name) VALUES ('order-1', 'Customer_12345', 'Oder', '4111 1111 1111 111', 'Visa', 123, '12/12', 'Air Ticket')";
        helpTest(sql, "N1QL2004");

        sql = "UPSERT INTO Customer_SavedAddresses VALUES ('customer-1', 2,  'Beijing')";
        helpTest(sql, "N1QL2005");

        sql = "UPSERT INTO Customer_SavedAddresses (documentID, Customer_SavedAddresses_idx, Customer_SavedAddresses) VALUES ('customer-1', 2,  'Beijing')";
        helpTest(sql, "N1QL2006");

        sql = "UPSERT INTO Oder_Items (documentID, Oder_Items_idx, Oder_Items_Quantity, Oder_Items_ItemID) VALUES ('order-1', 2, 5, 92312)";
        helpTest(sql, "N1QL2008");

    }

    @Test
    public void testSourceModel() {
        JsonObject json = JsonObject.create();
        assertNull(json.get("x"));
    }

    @Test
    public void testNestedJsonArrayType() {

        JsonObject order = formOder();
        JsonArray jsonArray = order.getArray("Items");
        List<Object> items = jsonArray.toList();
        for(int i = 0 ; i < items.size() ; i ++){
            Object item = items.get(i);
            assertEquals(item.getClass(), HashMap.class);
        }

        for(int i = 0 ; i < jsonArray.size() ; i ++) {
            Object item = jsonArray.get(i);
            assertEquals(item.getClass(), JsonObject.class);
        }
    }

}
