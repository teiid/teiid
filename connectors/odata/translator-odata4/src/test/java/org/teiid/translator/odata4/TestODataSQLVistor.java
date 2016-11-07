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
package org.teiid.translator.odata4;

import static org.junit.Assert.assertEquals;

import java.net.URLDecoder;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestODataSQLVistor {

    private void helpExecute(String query, String expected) throws Exception {
        MetadataFactory mf = TestODataMetadataProcessor.tripPinMetadata();
        
        helpExecute(mf, query, expected);
    }
    
    private void helpExecute(MetadataFactory mf, String query, String expected) throws Exception {
        ODataExecutionFactory ef = new ODataExecutionFactory();
        TranslationUtility utility = new TranslationUtility(TestODataMetadataProcessor.getTransformationMetadata(mf, ef));
        
        Select cmd = (Select)utility.parseCommand(query);
        ODataSQLVisitor visitor = new ODataSQLVisitor(ef, utility.createRuntimeMetadata());
        visitor.visitNode(cmd); 
        String actual = URLDecoder.decode(visitor.buildURL(""), "UTF-8");
        assertEquals(expected, actual);
    }    

    @Test
    public void testSelectStar() throws Exception {
    	helpExecute("select * from People", 
    	        "People?$select=UserName,FirstName,LastName,Emails,Gender,Concurrency");
    }
    
    @Test
    public void testSelectSpecificColumns() throws Exception {
    	helpExecute("select UserName from People", "People?$select=UserName");
    }

    @Test
    public void testPKBasedFilter() throws Exception {
    	helpExecute("select UserName from People where UserName = 'ALSK'", 
    	        "People?$select=UserName&$filter=UserName eq 'ALSK'");
    }
    
    @Test
    public void testPKBasedFilter2() throws Exception {
        helpExecute("select UserName from People where UserName = 'ALSK' or UserName= 'ABCD'", 
                "People?$select=UserName&$filter=UserName eq 'ABCD' or UserName eq 'ALSK'");
    }    
    
    @Test
    public void testMultiKeyKeyBasedFilter() throws Exception {
    	helpExecute("select Price from PurchaseDetails where ItemId = 1 and SaleId = 12 and Quantity = 2", 
    	        "PurchaseDetails?$select=Price&$filter=ItemId eq 1 and SaleId eq 12 and Quantity eq 2");
    }    
    
    @Test
    public void testAddFilter() throws Exception {
    	helpExecute("select Price from PurchaseDetails where ItemId = 1 and (Quantity+2) > ItemId", 
    	        "PurchaseDetails?$select=Price&$filter=ItemId eq 1 and cast((Quantity add 2),Edm.Int64) gt ItemId");
    } 
    
    @Test
    public void testMultiKeyKeyBasedFilterOr() throws Exception {
    	helpExecute("select Price from PurchaseDetails where (ItemId = 1 and SaleId = 12) or Quantity = 2", 
    	        "PurchaseDetails?$select=Price&$filter=(ItemId eq 1 and SaleId eq 12) or Quantity eq 2");
    }   
    
    @Test
    public void testPartialPK() throws Exception {
    	helpExecute("select Price from PurchaseDetails where Quantity >= 2 and SaleId = 12", 
    	        "PurchaseDetails?$select=Price&$filter=Quantity ge 2 and SaleId eq 12");
    }    
    
    @Test
    public void testSimpleJoinWithAnotherEntity() throws Exception {
    	helpExecute("SELECT p.UserName, pf.UserName FROM People p "
    	        + "JOIN People_Friends pf ON p.UserName=pf.People_UserName and p.UserName='russlwhyte'", 
    	        "People?$select=UserName&$filter=UserName eq 'russlwhyte'&$expand=Friends($select=UserName)");
    }    
    
    @Test
    public void testSimpleJoinWithAnotherEntity2() throws Exception {
        helpExecute("SELECT p.UserName, pf.UserName FROM People p "
                + "JOIN People_Friends pf ON p.UserName=pf.People_UserName", 
                "People?$select=UserName&$expand=Friends($select=UserName)");
    }     
    
    @Test
    public void testJoinBasedTwoPK() throws Exception {
        helpExecute(TestODataMetadataProcessor.oneToManyRelationMetadata(),
                "SELECT G2.e3 FROM G1 "
                + "JOIN G2 "
                + "ON G1.e1 = G2.e1 and G2.e1=12 and G2.e2='foo' WHERE G1.e1=12", 
                "G1?$select=e1&$filter=e1 eq 12&$expand=G2($select=e3;$filter=e1 eq 12 and e2 eq 'foo')");
    }
    
    @Test
    public void testJoinWithWhereORConditionsOnSameEntity() throws Exception {
        helpExecute(TestODataMetadataProcessor.oneToManyRelationMetadata(),
                "SELECT G2.e3 FROM G1 "
                + "JOIN G2 "
                + "ON G1.e1 = G2.e1 WHERE G1.e1=12 and (G2.e1=12 or G2.e2='foo')", 
                "G1?$select=e1&$filter=e1 eq 12&$expand=G2($select=e3;$filter=e1 eq 12 or e2 eq 'foo')");
    }
    
    @Test (expected=TranslatorException.class)
    public void testJoinWithWhereORConditionsOnDifferentEntity() throws Exception {
        helpExecute(TestODataMetadataProcessor.oneToManyRelationMetadata(),
                "SELECT G2.e3 FROM G1 "
                + "JOIN G2 "
                + "ON G1.e1 = G2.e1 WHERE G1.e1=12 or (G2.e1=12 and G2.e2='foo')", 
                "G1?$select=e1&$filter=e1 eq 12&$expand=G2($select=e3;$filter=e1 eq 12 or e2 eq 'foo')");
    }
    
    @Test
    public void testComplexTableJoin() throws Exception {
        helpExecute(TestODataMetadataProcessor.getEntityWithComplexProperty(),
                "select p.name, pa.city from Persons p JOIN Persons_address pa ON p.ssn = pa.ssn",
                "Persons?$select=name,address");
    }
    
    @Test
    public void testComplexTableJoinWithPK() throws Exception {
        helpExecute(TestODataMetadataProcessor.getEntityWithComplexProperty(),
                "select p.name, pa.city from Persons p "
                + "JOIN Persons_address pa ON p.ssn = pa.ssn WHERE p.ssn=12",
                "Persons?$select=name,address&$filter=ssn eq 12");
    }
    
    @Test
    public void testTwoComplexTableJoinWithPK() throws Exception {
        helpExecute(TestODataMetadataProcessor.getEntityWithComplexProperty(),
                "select p.name, pa.city, ps.city from Persons p "
                + "JOIN Persons_address pa ON p.ssn = pa.ssn "
                + "JOIN Persons_secondaddress ps ON p.ssn = ps.ssn "
                + "WHERE p.ssn=12",
                "Persons?$select=name,address,secondaddress&$filter=ssn eq 12");
    }    
    
    @Test
    public void testFunction() throws Exception {
    	helpExecute("SELECT UserName FROM People WHERE odata.startswith(UserName, 'CN')", 
    	        "People?$select=UserName&$filter=startswith(UserName,'CN') eq true");
    }
    
    @Test
    public void testLimit() throws Exception {
    	helpExecute("SELECT UserName FROM People limit 10", 
    	        "People?$select=UserName&$top=10");
    }  
    
    @Test
    public void testLimitOffset() throws Exception {
    	helpExecute("SELECT UserName FROM People limit 10, 19", 
    	        "People?$select=UserName&$skip=10&$top=19");
    }  
    
    @Test
    public void testUseAirthmaticFunction() throws Exception {
    	helpExecute("SELECT UserName FROM People WHERE Concurrency/10 > Concurrency", 
    	        "People?$select=UserName&$filter=(Concurrency div 10) gt Concurrency");
    }  
    
    @Test
    public void testOrderBy() throws Exception {
    	helpExecute("SELECT UserName FROM People Order By UserName", 
    	        "People?$select=UserName&$orderby=UserName");
    }  
    
    @Test
    public void testOrderByDESC() throws Exception {
        helpExecute("SELECT UserName FROM People Order By UserName DESC", 
                "People?$select=UserName&$orderby=UserName desc");
    } 
    
    @Test
    public void testOrderByMultiple() throws Exception {
    	helpExecute("SELECT UserName FROM People Order By UserName DESC, FirstName", 
    	        "People?$select=UserName&$orderby=UserName desc,FirstName");
    }     
    
    @Test
    public void testisNotNull() throws Exception {
    	helpExecute("SELECT UserName FROM People WHERE UserName is NOT NULL", 
    	        "People?$select=UserName&$filter=not(UserName eq null)");
    }    

    @Test
    public void testisNull() throws Exception {
        helpExecute("SELECT UserName FROM People WHERE UserName is NULL", 
                "People?$select=UserName&$filter=UserName eq null");
    }    
    
    @Test
    public void testCountStar() throws Exception {
    	helpExecute("SELECT count(*) FROM People", "People/$count");
    }  
    
    @Test
    public void testSelectFromNavigationTable() throws Exception {
        helpExecute("SELECT UserName FROM People_Friends WHERE People_UserName = 'russelwhyte'", 
                "People?$select=UserName&$filter=UserName eq 'russelwhyte'&$expand=Friends($select=UserName)");
    }
    @Test
    public void testSelectFromNavigationTable2() throws Exception {
        helpExecute("SELECT UserName FROM People_Friends WHERE People_UserName = 'russelwhyte' and UserName= 'jdoe'", 
                "People?$select=UserName&$filter=UserName eq 'russelwhyte'&$expand=Friends($select=UserName;$filter=UserName eq 'jdoe')");
    }    
    @Test
    public void testSelectFromComplexTable() throws Exception {
        helpExecute("SELECT * FROM People_AddressInfo where Address = 'foo'", 
                "People?$select=UserName,AddressInfo&$filter=AddressInfo/Address eq 'foo'");
    }     
    
}


