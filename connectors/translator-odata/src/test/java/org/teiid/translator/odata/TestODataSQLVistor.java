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
package org.teiid.translator.odata;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings("nls")
public class TestODataSQLVistor {
	private ODataExecutionFactory translator;
	private TranslationUtility utility;
	
    @Before
    public void setUp() throws Exception {
    	translator = new ODataExecutionFactory();
    	translator.start();
    	
		String csdl = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.xml"));
		ODataMetadataProcessor processor = new ODataMetadataProcessor();
		Properties props = new Properties();
		props.setProperty("schemaNamespace", "ODataWeb.Northwind.Model");
		props.setProperty("entityContainer", "NorthwindEntities");
		MetadataFactory mf = new MetadataFactory("vdb", 1, "nw", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
		
		processor.getMetadata(mf, new EdmxFormatParser().parseMetadata(StaxUtil.newXMLEventReader(new InputStreamReader(new ByteArrayInputStream(csdl.getBytes())))));
		
		TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "northwind", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
    	ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
    	if (report.hasItems()) {
    		throw new RuntimeException(report.getFailureMessage());
    	}
    	//TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");
    	utility = new TranslationUtility(metadata);
    }
    
    private void helpExecute(String query, String expected) throws Exception {
    	Select cmd = (Select)this.utility.parseCommand(query);
    	ODataSQLVisitor visitor = new ODataSQLVisitor(this.translator, utility.createRuntimeMetadata());
    	visitor.visitNode(cmd); 
    	String actual = visitor.buildURL();
    	assertEquals(expected, actual);
    }

    @Test
    public void testSelectStar() throws Exception {
    	helpExecute("select * from customers", "Customers?$select=CustomerID,CompanyName,ContactName,ContactTitle,Mailing_Address,Mailing_City,Mailing_Region,Mailing_PostalCode,Mailing_Country,Mailing_Phone,Mailing_Fax,Shipping_Address,Shipping_City,Shipping_Region,Shipping_PostalCode,Shipping_Country,Shipping_Phone,Shipping_Fax");
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
    	helpExecute("select UnitPrice from Order_Details where OrderID = 1 and ProductID = 12 and Quantity = 2", "Order_Details(ProductID=12,OrderID=1)?$filter=Quantity eq 2&$select=UnitPrice");
    }    
    
    @Test
    public void testMultiKeyKeyBasedFilterOr() throws Exception {
    	helpExecute("select UnitPrice from Order_Details where (OrderID = 1 and ProductID = 12) or Quantity = 2", "Order_Details?$filter=(OrderID eq 1 AND ProductID eq 12) OR Quantity eq 2&$select=UnitPrice");
    }   
    
    @Test
    public void testPartialPK() throws Exception {
    	helpExecute("select UnitPrice from Order_Details where Quantity >= 2 and ProductID = 12", "Order_Details?$filter=Quantity ge 2 AND ProductID eq 12&$select=UnitPrice");
    }    
    
    @Test
    public void testSimpleJoin() throws Exception {
    	helpExecute("SELECT od.UnitPrice FROM Orders o JOIN Order_Details od ON o.OrderID=od.OrderID and o.OrderID=12", "Orders(12)/Order_Details?$select=UnitPrice");
    }    
    
    @Test
    public void testJoinBasedTwoPK() throws Exception {
    	helpExecute("SELECT od.UnitPrice FROM Orders o JOIN Order_Details od ON o.OrderID=od.OrderID and o.OrderID=12 WHERE od.ProductID=1", "Orders(12)/Order_Details(ProductID=1,OrderID=12)?$select=UnitPrice");
    }   
    
    @Test
    public void testJoinBasedTwoPKOnKey() throws Exception {
    	helpExecute("SELECT od.UnitPrice FROM Orders o JOIN Order_Details od ON o.OrderID=od.OrderID and o.OrderID=12 and od.ProductID=1", "Orders(12)/Order_Details(ProductID=1,OrderID=12)?$select=UnitPrice");
    }     
    
    @Test
    public void testEmbeddedJoin() throws Exception {
    	testSelectStar(); // customer table is embedded
    }
    
    @Test
    public void testFunction() throws Exception {
    	helpExecute("SELECT ContactName FROM Customers WHERE odata.startswith(CompanyName, 'CN')", "Customers?$filter=startswith(CompanyName'CN') eq TRUE&$select=ContactName");
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
    	helpExecute("SELECT LastName FROM Employees Order By LastName DESC", "Employees?$orderby=LastName DESC&$select=LastName");
    }    
    
    @Test
    public void testOrderByMultiple() throws Exception {
    	helpExecute("SELECT LastName FROM Employees Order By LastName DESC, EmployeeId", "Employees?$orderby=LastName DESC,EmployeeId&$select=LastName");
    }     
    
    @Test
    public void testisNotNull() throws Exception {
    	helpExecute("SELECT LastName FROM Employees WHERE LastName is NOT NULL", "Employees?$filter=NOT(LastName eq NULL)&$select=LastName");
    }    
    
    @Test
    public void testisNull() throws Exception {
    	helpExecute("SELECT LastName FROM Employees WHERE LastName is NULL", "Employees?$filter=LastName eq NULL&$select=LastName");
    }     
    
    @Test
    public void testCountStar() throws Exception {
    	helpExecute("SELECT count(*) FROM Employees", "Employees/$count");
    }      
    
    private void helpFunctionExecute(String query, String expected) throws Exception {
    	Call cmd = (Call)this.utility.parseCommand(query);
		ODataProcedureVisitor visitor = new ODataProcedureVisitor(translator, utility.createRuntimeMetadata());
    	visitor.visitNode(cmd); 
    	String odataCmd = visitor.toString();
    	
    	assertEquals(expected, odataCmd);
    	assertEquals("GET", visitor.getMethod());
    }
    
    @Test
    public void testProcedureExec() throws Exception {
    	helpFunctionExecute("Exec TopCustomers('newyork')", "TopCustomers?city='newyork'");
    }     
}
