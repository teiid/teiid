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
package org.teiid.translator.odata4;

import static org.junit.Assert.*;

import java.net.URLDecoder;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Call;
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
    public void testMultiKeyKeyBasedFilter() throws Exception {
        helpExecute("select Price from PurchaseDetails where ItemId = 1 and SaleId = 12 and Quantity = 2",
                "PurchaseDetails?$select=Price&$filter=ItemId eq 1 and SaleId eq 12 and Quantity eq 2");
    }

    @Test
    public void testBigDecimalLiteral() throws Exception {
        helpExecute("select ItemId from PurchaseDetails where price > 12.0",
                "PurchaseDetails?$select=ItemId&$filter=Price gt 12.0");
    }

    @Test
    public void testBigDecimalParameter() throws Exception {
        ODataExecutionFactory ef = new ODataExecutionFactory();
        TranslationUtility utility = new TranslationUtility(TestODataMetadataProcessor.getTransformationMetadata(TestODataMetadataProcessor.tripPinMetadata(), ef));

        Call cmd = (Call)utility.parseCommand("call GetNearestAirport(1.1, 1.2, 2.0)");
        String params = ODataProcedureExecution.getQueryParameters(cmd);
        assertEquals("lat=1.1&lon=1.2&within=2.0", params);
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
                "select p.name, pa.city from Persons p JOIN Persons_address pa ON p.ssn = pa.Persons_ssn",
                "Persons?$select=name,address");
    }

    @Test
    public void testComplexTableJoinWithPK() throws Exception {
        helpExecute(TestODataMetadataProcessor.getEntityWithComplexProperty(),
                "select p.name, pa.city from Persons p "
                + "JOIN Persons_address pa ON p.ssn = pa.Persons_ssn WHERE p.ssn=12",
                "Persons?$select=name,address&$filter=ssn eq 12");
    }

    @Test
    public void testTwoComplexTableJoinWithPK() throws Exception {
        helpExecute(TestODataMetadataProcessor.getEntityWithComplexProperty(),
                "select p.name, pa.city, ps.city from Persons p "
                + "JOIN Persons_address pa ON p.ssn = pa.Persons_ssn "
                + "JOIN Persons_secondaddress ps ON p.ssn = ps.Persons_ssn "
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

        helpExecute("SELECT UserName FROM People WHERE 10/Concurrency > Concurrency",
                "People?$select=UserName&$filter=(10 div Concurrency) gt Concurrency");
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
                "People?$select=UserName&$filter=UserName ne null");
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

    @Test
    public void testConvert() throws Exception {
        helpExecute("select username from People where convert(gender, integer) = 1",
                "People?$select=UserName&$filter=cast(Gender,Edm.Int32) eq 1");
    }

    @Test
    public void testMod() throws Exception {
        helpExecute(TestODataMetadataProcessor.oneToManyRelationMetadata(), "select e2 from G1 where MOD(e1, 2) = 1",
                "G1?$select=e2&$filter=(e1 mod 2) eq 1");
    }

    @Test
    public void testLocate() throws Exception {
        helpExecute("select UserName from People where locate(UserName, 'a') = 1",
                "People?$select=UserName&$filter=(indexof('a',UserName) add 1) eq 1");
    }

    @Test
    public void testSubstring() throws Exception {
        helpExecute("select UserName from People where substring(UserName, 1) = 'a' and substring(username, 2, 2) = 'bc'",
                "People?$select=UserName&$filter=substring(UserName,1) eq 'a' and substring(UserName,(2 add 1),2) eq 'bc'");
    }

}


