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
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestN1QLVisitor extends TestVisitor {
    
    private void helpTest(String sql, String key) throws TranslatorException {

        Command command = translationUtility.parseCommand(sql);

        N1QLVisitor visitor = TRANSLATOR.getN1QLVisitor();
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
    }
    
    @Test
    public void testStringFunctions() throws TranslatorException {
        
        String sql = "SELECT LCASE(attr_string) FROM T2";
        helpTest(sql, "N1QL0901");
        
        sql = "SELECT UCASE(attr_string) FROM T2";
        helpTest(sql, "N1QL0902");
        
        sql = "SELECT TRANSLATE(attr_string, 'is', 'are') FROM T2";
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
    
}
