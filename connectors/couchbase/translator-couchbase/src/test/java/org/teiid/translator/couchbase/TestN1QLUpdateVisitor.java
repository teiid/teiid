/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.couchbase;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;

import com.couchbase.client.java.document.json.JsonObject;

@SuppressWarnings("nls")
public class TestN1QLUpdateVisitor extends TestVisitor {
    
    private void helpTest(String sql, String key) throws TranslatorException {
        Command command = translationUtility.parseCommand(sql);

        N1QLUpdateVisitor visitor = TRANSLATOR.getN1QLUpdateVisitor();
        visitor.append(command);
        String actual = visitor.toString();
        
        if(PRINT_TO_CONSOLE.booleanValue()) {
            System.out.println(actual);
        }
        
        if(REPLACE_EXPECTED.booleanValue()) {
            N1QL.put(key.toString(), actual);
        }
        
        assertEquals(key, N1QL.getProperty(key, ""), actual);
    }
    
    @Test
    public void testInsert() throws TranslatorException  {
        
        String sql = "INSERT INTO Customer VALUES ('customer-1', 'Customer_12346', 'Customer', 'Kylin Soong')";
        helpTest(sql, "N1QL1401");
        
        sql = "INSERT INTO Customer (documentID, ID, Name, type) VALUES ('customer-1', 'Customer_12346', 'Kylin Soong', 'Customer')";
        helpTest(sql, "N1QL1402");
        
        sql = "INSERT INTO Oder VALUES ('order-1', 'Customer_12345', 'Oder', '4111 1111 1111 111', 'Visa', 123, '12/12', 'Air Ticket')";
        helpTest(sql, "N1QL1403");
        
        sql = "INSERT INTO Oder (documentID, CustomerID, type, CreditCard_CardNumber, CreditCard_Type, CreditCard_CVN, CreditCard_Expiry, Name) VALUES ('order-1', 'Customer_12345', 'Oder', '4111 1111 1111 111', 'Visa', 123, '12/12', 'Air Ticket')";
        helpTest(sql, "N1QL1404");
        
        sql = "INSERT INTO Customer (ID, Name, type) VALUES ('Customer_12346', 'Kylin Soong', 'Customer')";
        try {
            helpTest(sql, "N1QL1405");
        } catch (Exception e) {
            assertEquals(TeiidRuntimeException.class, e.getClass());
        }
        
        sql = "INSERT INTO Customer (documentID) VALUES ('customer-1')"; // empty document
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
        
        sql = "INSERT INTO Oder_Items VALUES ('order-1', 2, 5, 92312)";
        helpTest(sql, "N1QL1411");
        
        sql = "INSERT INTO Oder_Items (documentID, Oder_Items_idx, Oder_Items_Quantity, Oder_Items_ItemID) VALUES ('order-1', 2, 5, 92312)";
        helpTest(sql, "N1QL1412");
        
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
        
        sql = "DELETE FROM Customer_SavedAddresses WHERE documentID = 'customer-3' AND Customer_SavedAddresses_idx = '1'";
        helpTest(sql, "N1QL1606");
        
        sql = "DELETE FROM Customer_SavedAddresses WHERE Customer_SavedAddresses_idx = '2'";
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
    public void testSourceModel() {
        JsonObject json = JsonObject.create();
        assertNull(json.get("x"));
    }

}
