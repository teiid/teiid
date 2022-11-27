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
package org.teiid.translator.odata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestODataSQLVistor {
    private static final boolean printPayload = false;
    private ODataExecutionFactory translator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
        translator = new ODataExecutionFactory();
        translator.start();

        String csdl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.xml"));
        EdmDataServices eds = new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new InputStreamReader(new ByteArrayInputStream(csdl.getBytes()))));
        ODataMetadataProcessor processor = new ODataMetadataProcessor();
        Properties props = new Properties();
        props.setProperty("schemaNamespace", "ODataWeb.Northwind.Model");
        props.setProperty("entityContainer", "NorthwindEntities");
        MetadataFactory mf = new MetadataFactory("vdb", 1, "nw", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);

        processor.getMetadata(mf, eds);

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "northwind", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        //TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");
        utility = new TranslationUtility(metadata);
    }

    private ArrayList<TranslatorException> helpExecute(String query, String expected) throws Exception {
        Select cmd = (Select)this.utility.parseCommand(query);
        ODataSQLVisitor visitor = new ODataSQLVisitor(this.translator, utility.createRuntimeMetadata());
        visitor.visitNode(cmd);
        String actual = URLDecoder.decode(visitor.buildURL(), "UTF-8");
        assertEquals(expected, actual);
        return visitor.exceptions;
    }

    @Test
    public void testSelectStar() throws Exception {
        helpExecute("select * from customers", "Customers?$select=CustomerID,CompanyName,ContactName,ContactTitle,Mailing,Shipping");
    }

    @Test
    public void testSelectSpecificColumns() throws Exception {
        helpExecute("select CustomerID from customers", "Customers?$select=CustomerID");
    }

    @Test
    public void testPKBasedFilter() throws Exception {
        helpExecute("select CompanyName from Customers where CustomerID = 'ALSK'", "Customers('ALSK')?$select=CompanyName");
    }

    @Test
    public void testMultiKeyKeyBasedFilter() throws Exception {
        helpExecute("select UnitPrice from Order_Details where OrderID = 1 and ProductID = 12 and Quantity = 2", "Order_Details(OrderID=1,ProductID=12)?$filter=Quantity eq 2&$select=UnitPrice");
    }

    @Test
    public void testAddFilter() throws Exception {
        helpExecute("select UnitPrice from Order_Details where OrderID = 1 and (Quantity+2) > OrderID", "Order_Details?$filter=OrderID eq 1 and (cast(Quantity,'integer') add 2) gt OrderID&$select=UnitPrice");
    }

    @Test
    public void testEndsWithFilter() throws Exception {
        helpExecute("select CompanyName from Customers where endswith('k', CustomerID)", "Customers?$filter=endswith(CustomerID,'k') eq true&$select=CompanyName");
    }

    @Test
    public void testMultiKeyKeyBasedFilterOr() throws Exception {
        helpExecute("select UnitPrice from Order_Details where (OrderID = 1 and ProductID = 12) or Quantity = 2", "Order_Details?$filter=(OrderID eq 1 and ProductID eq 12) or Quantity eq 2&$select=UnitPrice");
    }

    @Test
    public void testPartialPK() throws Exception {
        helpExecute("select UnitPrice from Order_Details where Quantity >= 2 and ProductID = 12", "Order_Details?$filter=Quantity ge 2 and ProductID eq 12&$select=UnitPrice");
    }

    @Test
    public void testSimpleJoin() throws Exception {
        helpExecute("SELECT od.UnitPrice FROM Orders o JOIN Order_Details od ON o.OrderID=od.OrderID and o.OrderID=12", "Orders(12)/Order_Details?$select=UnitPrice");
    }

    @Test
    public void testJoinBasedTwoPK() throws Exception {
        helpExecute("SELECT od.UnitPrice FROM Orders o JOIN Order_Details od ON o.OrderID=od.OrderID and o.OrderID=12 WHERE od.ProductID=1", "Orders(12)/Order_Details(OrderID=12,ProductID=1)?$select=UnitPrice");
    }

    @Test
    public void testJoinBasedTwoPKOnKey() throws Exception {
        helpExecute("SELECT od.UnitPrice FROM Orders o JOIN Order_Details od ON o.OrderID=od.OrderID and o.OrderID=12 and od.ProductID=1", "Orders(12)/Order_Details(OrderID=12,ProductID=1)?$select=UnitPrice");
    }

    @Test
    public void testEmbeddedJoin() throws Exception {
        testSelectStar(); // customer table is embedded
    }

    @Test
    public void testFunction() throws Exception {
        helpExecute("SELECT ContactName FROM Customers WHERE odata.startswith(CompanyName, 'CN')", "Customers?$filter=startswith(CompanyName,'CN') eq true&$select=ContactName");
    }

    @Test
    public void testBooleanFunction() throws Exception {
        this.translator.setSupportsOdataBooleanFunctionsWithComparison(false);
        try {
            helpExecute("SELECT ContactName FROM Customers WHERE odata.startswith(CompanyName, 'CN')", "Customers?$filter=startswith(CompanyName,'CN')&$select=ContactName");
            helpExecute("SELECT ContactName FROM Customers WHERE odata.startswith(CompanyName, 'CN') = 0", "Customers?$filter=NOT (startswith(CompanyName,'CN'))&$select=ContactName");
            helpExecute("SELECT ContactName FROM Customers WHERE odata.startswith(CompanyName, 'CN') <> 1", "Customers?$filter=NOT (startswith(CompanyName,'CN'))&$select=ContactName");
            helpExecute("SELECT ContactName FROM Customers WHERE odata.startswith(CompanyName, 'CN') <> 0", "Customers?$filter=startswith(CompanyName,'CN')&$select=ContactName");

            helpExecute("SELECT ContactName FROM Customers WHERE endswith('CN', CompanyName)", "Customers?$filter=endswith(CompanyName,'CN')&$select=ContactName");
            helpExecute("SELECT ContactName FROM Customers WHERE odata.substringof(CompanyName, 'CN')", "Customers?$filter=substringof(CompanyName,'CN')&$select=ContactName");


            ArrayList<TranslatorException> exceptions =
                    helpExecute("SELECT ContactName FROM Customers WHERE endswith('CN', CompanyName) > endswith('CN1', CompanyName)", "Customers?$filter=NOT (endswith(CompanyName,'CN'))&$select=ContactName");
            assertTrue(!exceptions.isEmpty());
            assertTrue(exceptions.get(0).getMessage().contains(ODataPlugin.Event.TEIID17018.name()));
        } finally {
            this.translator.setSupportsOdataBooleanFunctionsWithComparison(true);
        }
    }

    @Test
    public void testLimit() throws Exception {
        helpExecute("SELECT ContactName FROM Customers limit 10", "Customers?$select=ContactName&$top=10");
    }

    @Test
    public void testLimitOffset() throws Exception {
        helpExecute("SELECT ContactName FROM Customers limit 10, 19", "Customers?$select=ContactName&$skip=10&$top=19");
    }

    @Test
    public void testUseAirthmaticFunction() throws Exception {
        helpExecute("SELECT LastName FROM Employees WHERE EmployeeID/10 > EmployeeID", "Employees?$filter=(EmployeeID div 10) gt EmployeeID&$select=LastName");
    }

    @Test
    public void testOrderBy() throws Exception {
        helpExecute("SELECT LastName FROM Employees Order By LastName", "Employees?$orderby=LastName&$select=LastName");
    }

    @Test
    public void testOrderByDESC() throws Exception {
        helpExecute("SELECT LastName FROM Employees Order By LastName DESC", "Employees?$orderby=LastName desc&$select=LastName");
    }

    @Test
    public void testOrderByMultiple() throws Exception {
        helpExecute("SELECT LastName FROM Employees Order By LastName DESC, EmployeeId", "Employees?$orderby=LastName desc,EmployeeID&$select=LastName");
    }

    @Test
    public void testisNotNull() throws Exception {
        helpExecute("SELECT LastName FROM Employees WHERE LastName is NOT NULL", "Employees?$filter=not(LastName eq null)&$select=LastName");
    }

    @Test
    public void testisNull() throws Exception {
        helpExecute("SELECT LastName FROM Employees WHERE LastName is NULL", "Employees?$filter=LastName eq null&$select=LastName");
    }

    @Test
    public void testCountStar() throws Exception {
        helpExecute("SELECT count(*) FROM Employees", "Employees/$count");
    }

    private void helpFunctionExecute(String query, String expected) throws Exception {
        Call cmd = (Call)this.utility.parseCommand(query);
        ODataProcedureVisitor visitor = new ODataProcedureVisitor(translator, utility.createRuntimeMetadata());
        visitor.visitNode(cmd);
        String odataCmd = visitor.buildURL();

        assertEquals(expected, odataCmd);
        assertEquals("GET", visitor.getMethod());
    }

    @Test
    public void testProcedureExec() throws Exception {
        helpFunctionExecute("Exec TopCustomers('newyork')", "TopCustomers?city=%27newyork%27");
    }

    @Test
    public void testProcedureExecEncoded() throws Exception {
        helpFunctionExecute("Exec CommonCustomers('new york', 'los angeles')", "CommonCustomers?city1=%27new%20york%27&city2=%27los%20angeles%27");
    }

    private void helpUpdateExecute(String query, String expected, String expectedMethod, boolean checkPayload) throws Exception {
        Command cmd = this.utility.parseCommand(query);

        ODataUpdateVisitor visitor = new ODataUpdateVisitor(translator, utility.createRuntimeMetadata());
        visitor.visitNode(cmd);

        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        String odataCmd = visitor.buildURL();

        if (checkPayload) {
            assertNotNull(visitor.getPayload());
        }

        if (printPayload) {
            System.out.println(visitor.getPayload());
        }

        assertEquals(expected, odataCmd);
        assertEquals(expectedMethod, visitor.getMethod());
    }

    @Test
    public void testInsert() throws Exception {
        helpUpdateExecute("INSERT INTO Regions (RegionID,RegionDescription) VALUES (10,'Asian')", "Regions", "POST", true);
    }

    @Test(expected=TranslatorException.class)
    public void testDeletewithoutPK() throws Exception {
        helpUpdateExecute("Delete From Regions", "Regions", "DELETE", false);
    }

    @Test
    public void testDelete() throws Exception {
        helpUpdateExecute("Delete From Regions where RegionID=10", "Regions(10)", "DELETE", false);
    }

    @Test(expected=TranslatorException.class)
    public void testDeleteOtherClause() throws Exception {
        helpUpdateExecute("Delete From Regions where RegionDescription='foo'", "Regions", "DELETE", false);
    }

    @Test
    public void testUpdate() throws Exception {
        helpUpdateExecute("UPDATE Regions SET RegionDescription='foo' WHERE RegionID=10", "Regions(10)", "PUT", true);
    }

    @Test(expected=TranslatorException.class)
    public void testUpdatewithoutPK() throws Exception {
        helpUpdateExecute("UPDATE Regions SET RegionDescription='foo'", "Regions(10)", "PATCH", true);
    }

    @Test(expected=TranslatorException.class)
    public void testUpdateOtherClause() throws Exception {
        helpUpdateExecute("UPDATE Regions SET RegionID=10 WHERE RegionDescription='foo'", "Regions(10)", "PATCH", true);
    }
}
