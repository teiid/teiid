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
    }
    
    @Test
    public void testSourceModel() {
        JsonObject json = JsonObject.create();
        assertNull(json.get("x"));
    }

}
