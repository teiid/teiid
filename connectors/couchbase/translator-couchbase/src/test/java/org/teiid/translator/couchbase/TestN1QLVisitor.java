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

import static org.teiid.translator.couchbase.TestCouchbaseMetadataProcessor.*;
import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.CoreConstants;
import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Command;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestN1QLVisitor {
    
    private static TransformationMetadata queryMetadataInterface() {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("couchbase");

            CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
            MetadataFactory mf = new MetadataFactory("couchbase", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), mmd);
            CouchbaseConnection conn = Mockito.mock(CouchbaseConnection.class);
            Mockito.stub(conn.getKeyspaceName()).toReturn(KEYSPACE);
            mp.addTable(conn, mf, KEYSPACE, formCustomer(), null);
            mp.addTable(conn, mf, KEYSPACE, formOder(), null);
            mp.addTable(conn, mf, KEYSPACE, formSimpleJson(), null);
            mp.addTable(conn, mf, KEYSPACE, formJson(), null);
            mp.addTable(conn, mf, KEYSPACE, formArray(), null);
            mp.addTable(conn, mf, KEYSPACE, layerJson(), null);
            mp.addTable(conn, mf, KEYSPACE, layerArray(), null);
            mp.addTable(conn, mf, KEYSPACE, nestedArray(), null);
            mp.addProcedures(mf, null);

            TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x");
            ValidatorReport report = new MetadataValidator().validate(tm.getVdbMetaData(), tm.getMetadataStore());
            if (report.hasItems()) {
                throw new RuntimeException(report.getFailureMessage());
            }
            return tm;
        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }
    }
    
    static TranslationUtility translationUtility = new TranslationUtility(queryMetadataInterface());
    static RuntimeMetadata runtimeMetadata = new RuntimeMetadataImpl(queryMetadataInterface());
    
    private static CouchbaseExecutionFactory TRANSLATOR;
    
    @BeforeClass
    public static void init() throws TranslatorException {
        TRANSLATOR = new CouchbaseExecutionFactory();
        TRANSLATOR.start();
        translationUtility.addUDF(CoreConstants.SYSTEM_MODEL, TRANSLATOR.getPushDownFunctions());
    }
    
    private void helpTest(String sql, String expected) throws TranslatorException {

        Command command = translationUtility.parseCommand(sql);

        N1QLVisitor visitor = TRANSLATOR.getN1QLVisitor();
        visitor.setKeySpace(KEYSPACE);
        visitor.append(command);

//        System.out.println(visitor.toString());
        assertEquals(expected, visitor.toString());
    }
    

    @Test
    public void testBasicSelect() throws TranslatorException {
        
        String sql = "SELECT * FROM test";
        helpTest(sql, "SELECT META().id AS PK, `test`.Type, `test`.ID, `test`.Name, `test`.CustomerID, `test`.attr_double, `test`.attr_number_short, `test`.attr_string, `test`.attr_boolean, `test`.attr_number_long, `test`.attr_int, `test`.attr_number_integer, `test`.attr_long, `test`.attr_number_float, `test`.attr_null, `test`.attr_number_byte, `test`.attr_number_double FROM `test`");
        
        sql = "SELECT * FROM test_CreditCard AS T";
        helpTest(sql, "SELECT META().id AS PK, T.CardNumber, T.Type, T.CVN, T.Expiry FROM `test`.`CreditCard` AS T");
        
        sql = "SELECT * FROM test_CreditCard";
        helpTest(sql, "SELECT META().id AS PK, CardNumber, Type, CVN, Expiry FROM `test`.`CreditCard`");
    }
    
    @Test
    public void testNestedArray() throws TranslatorException {
        
        String sql = "SELECT * FROM test_SavedAddresses";
        helpTest(sql, "SELECT META().id AS PK, SavedAddresses FROM `test`.`SavedAddresses`");
        
        sql = "SELECT * FROM test_SavedAddresses AS T";
        helpTest(sql, "SELECT META().id AS PK, T FROM `test`.`SavedAddresses` AS T");
        
        sql = "SELECT * FROM test_Items";
        helpTest(sql, "SELECT META().id AS PK, Items FROM `test`.`Items`");
        
        sql = "SELECT * FROM test_Items AS T";
        helpTest(sql, "SELECT META().id AS PK, T FROM `test`.`Items` AS T");
    }
    
    @Test
    public void testNestedArrayLoop() throws TranslatorException {
        String sql = "SELECT * FROM test_nestedArray";
        helpTest(sql, "SELECT META().id AS PK, nestedArray FROM `test`.`nestedArray`");
        
        sql = "SELECT * FROM test_nestedArray AS T";
        helpTest(sql, "SELECT META().id AS PK, T FROM `test`.`nestedArray` AS T");
    }
    
    @Test
    public void testPKColumn() throws TranslatorException {
        
        String sql = "SELECT documentId FROM test";
        helpTest(sql, "SELECT META().id AS PK FROM `test`");
        
        sql = "SELECT documentId FROM test_CreditCard";
        helpTest(sql, "SELECT META().id AS PK FROM `test`.`CreditCard`");
    }
    
    @Test
    public void testLimitOffsetClause() throws TranslatorException {
        
        String sql = "SELECT Name FROM test LIMIT 2";
        helpTest(sql, "SELECT `test`.Name FROM `test` LIMIT 2");
        
        sql = "SELECT Name FROM test LIMIT 2, 2";
        helpTest(sql, "SELECT `test`.Name FROM `test` LIMIT 2 OFFSET 2");
        
        sql = "SELECT Name FROM test OFFSET 2 ROWS";
        helpTest(sql, "SELECT `test`.Name FROM `test` LIMIT 2147483647 OFFSET 2");
    }
    
    @Test
    public void testOrderByClause() throws TranslatorException {
        
        String sql = "SELECT Name, Type FROM test ORDER BY Name";
        helpTest(sql, "SELECT `test`.Name, `test`.Type FROM `test` ORDER BY `test`.Name");
        
        sql = "SELECT Type FROM test ORDER BY Name"; //Unrelated
        helpTest(sql, "SELECT `test`.Type FROM `test` ORDER BY `test`.Name");
        
        sql = "SELECT Name, Type FROM test ORDER BY Type"; //NullOrdering
        helpTest(sql, "SELECT `test`.Name, `test`.Type FROM `test` ORDER BY `test`.Type");
    }
    
    @Test
    public void testGroupByClause() throws TranslatorException {
        
        String sql = "SELECT Name, COUNT(*) FROM test GROUP BY Name";
        helpTest(sql, "SELECT `test`.Name, COUNT(*) FROM `test` GROUP BY `test`.Name");
    }
    
    @Test
    public void testWhereClause() throws TranslatorException {
        
        String sql = "SELECT Name, Type  FROM test WHERE Name = 'John Doe'";
        helpTest(sql, "SELECT `test`.Name, `test`.Type FROM `test` WHERE `test`.Name = 'John Doe'");
        
        sql = "SELECT Name, Type  FROM test WHERE documentId = 'customer'";
        helpTest(sql, "SELECT `test`.Name, `test`.Type FROM `test` WHERE META().id = 'customer'");
    }
    
    @Test
    public void testSelectClause() throws TranslatorException {
        
        String sql = "SELECT DISTINCT attr_int FROM test";
        helpTest(sql, "SELECT DISTINCT `test`.attr_int FROM `test`");
        
        sql = "SELECT ALL attr_int FROM test";
        helpTest(sql, "SELECT `test`.attr_int FROM `test`");
    }
    
    @Test
    public void testSetOperations() throws TranslatorException {
        
        String sql = "SELECT attr_int, attr_long FROM test UNION ALL SELECT attr_int, attr_long FROM test_attr_jsonObject";
        helpTest(sql, "SELECT `test`.attr_int, `test`.attr_long FROM `test` UNION ALL SELECT attr_int, attr_long FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT attr_int, attr_long FROM test UNION SELECT attr_int, attr_long FROM test_attr_jsonObject";
        helpTest(sql, "SELECT `test`.attr_int, `test`.attr_long FROM `test` UNION SELECT attr_int, attr_long FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT attr_int, attr_long FROM test INTERSECT SELECT attr_int, attr_long FROM test_attr_jsonObject";
        helpTest(sql, "SELECT `test`.attr_int, `test`.attr_long FROM `test` INTERSECT SELECT attr_int, attr_long FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT attr_int, attr_long FROM test EXCEPT SELECT attr_int, attr_long FROM test_attr_jsonObject";
        helpTest(sql, "SELECT `test`.attr_int, `test`.attr_long FROM `test` EXCEPT SELECT attr_int, attr_long FROM `test`.`attr_jsonObject`");
    }
    
    @Test
    public void testStringFunctions() throws TranslatorException {
        
        String sql = "SELECT LCASE(attr_string) FROM test_attr_jsonObject";
        helpTest(sql, "SELECT LOWER(attr_string) FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT UCASE(attr_string) FROM test_attr_jsonObject";
        helpTest(sql, "SELECT UPPER(attr_string) FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT TRANSLATE(attr_string, 'is', 'are') FROM test_attr_jsonObject";
        helpTest(sql, "SELECT REPLACE(attr_string, 'is', 'are') FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT couchbase.CONTAINS(attr_string, 'is') FROM test_attr_jsonObject";
        helpTest(sql, "SELECT CONTAINS(attr_string, 'is') FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT couchbase.TITLE(attr_string) FROM test_attr_jsonObject";
        helpTest(sql, "SELECT TITLE(attr_string) FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT couchbase.LTRIM(attr_string, 'This') FROM test_attr_jsonObject";
        helpTest(sql, "SELECT LTRIM(attr_string, 'This') FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT couchbase.TRIM(attr_string, 'is') FROM test_attr_jsonObject";
        helpTest(sql, "SELECT TRIM(attr_string, 'is') FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT couchbase.RTRIM(attr_string, 'value') FROM test_attr_jsonObject";
        helpTest(sql, "SELECT RTRIM(attr_string, 'value') FROM `test`.`attr_jsonObject`");
        
        sql = "SELECT couchbase.POSITION(attr_string, 'is') FROM test_attr_jsonObject";
        helpTest(sql, "SELECT POSITION(attr_string, 'is') FROM `test`.`attr_jsonObject`");
    }
    
    @Test
    public void testNumbericFunctions() throws TranslatorException {
        
        String sql = "SELECT CEILING(attr_number_float) FROM test_attr_jsonObject";
        helpTest(sql, "SELECT CEIL(TONUMBER(attr_number_float)) FROM `test`.`attr_jsonObject`"); 
        
        sql = "SELECT LOG(attr_number_double) FROM test_attr_jsonObject";
        helpTest(sql, "SELECT LN(attr_number_double) FROM `test`.`attr_jsonObject`"); 
        
        sql = "SELECT LOG10(attr_number_double) FROM test_attr_jsonObject";
        helpTest(sql, "SELECT LOG(attr_number_double) FROM `test`.`attr_jsonObject`"); 
        
        sql = "SELECT RAND(attr_number_integer) FROM test_attr_jsonObject";
        helpTest(sql, "SELECT RANDOM(attr_number_integer) FROM `test`.`attr_jsonObject`"); 
    }
    
    @Test
    public void testConversionFunctions() throws TranslatorException {
        
        String sql = "SELECT convert(attr_number_float, double) FROM test";
        helpTest(sql, "SELECT TONUMBER(`test`.attr_number_float) FROM `test`"); 
        
        sql = "SELECT convert(attr_number_byte, boolean) FROM test";
        helpTest(sql, "SELECT TOBOOLEAN(`test`.attr_number_byte) FROM `test`");
        
        sql = "SELECT convert(attr_number_long, string) FROM test";
        helpTest(sql, "SELECT TOSTRING(`test`.attr_number_long) FROM `test`");
    }
    
    @Test
    public void testDateFunctions() throws TranslatorException {
        
        String sql = "SELECT couchbase.CLOCK_MILLIS() FROM test";
        helpTest(sql, "SELECT CLOCK_MILLIS() FROM `test`"); 
        
        sql = "SELECT couchbase.CLOCK_STR() FROM test";
        helpTest(sql, "SELECT CLOCK_STR() FROM `test`"); 
        
        sql = "SELECT couchbase.CLOCK_STR('2006-01-02') FROM test";
        helpTest(sql, "SELECT CLOCK_STR('2006-01-02') FROM `test`");
                
        sql = "SELECT couchbase.DATE_ADD_MILLIS(1488873653696, 2, 'century') FROM test";
        helpTest(sql, "SELECT DATE_ADD_MILLIS(1488873653696, 2, 'century') FROM `test`"); 
        
        sql = "SELECT couchbase.DATE_ADD_STR('2017-03-08', 2, 'century') FROM test";
        helpTest(sql, "SELECT DATE_ADD_STR('2017-03-08', 2, 'century') FROM `test`"); 
    }
    
    @Test
    public void testProcedures() throws TranslatorException {
        
        String sql = "call getTextDocuments('customer')";
        helpTest(sql, "SELECT META().id AS id, result FROM `test` AS result WHERE META().id LIKE 'customer'");
        
        sql = "call getTextDocuments('%e%')";
        helpTest(sql, "SELECT META().id AS id, result FROM `test` AS result WHERE META().id LIKE '%e%'");
        
        sql = "call getDocuments('customer')";
        helpTest(sql, "SELECT result FROM `test` AS result WHERE META().id LIKE 'customer'");
        
        sql = "call getTextDocument('customer')";
        helpTest(sql, "SELECT META().id AS id, result FROM `test` AS result USE PRIMARY KEYS 'customer'");
        
        sql = "call getDocument('customer')";
        helpTest(sql, "SELECT result FROM `test` AS result USE PRIMARY KEYS 'customer'");
        
        sql = "call saveDocument('k001', '{\"key\": \"value\"}')";
        helpTest(sql, "UPSERT INTO `test` AS result (KEY, VALUE) VALUES ('k001', '{\"key\": \"value\"}') RETURNING result");
        
        sql = "call deleteDocument('k001')";
        helpTest(sql, "DELETE FROM `test` AS result USE PRIMARY KEYS 'k001' RETURNING result");
        
        sql = "call getTextMetadataDocument()";
        helpTest(sql, "SELECT META() AS result FROM `test`");
        
        sql = "call getMetadataDocument()";
        helpTest(sql, "SELECT META() AS result FROM `test`");
    }
   

}
