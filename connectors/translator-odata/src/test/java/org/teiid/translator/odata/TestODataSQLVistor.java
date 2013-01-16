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

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Select;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestODataSQLVistor {
	private ODataExecutionFactory translator;
	private TranslationUtility utility;
	
    @Before
    public void setUp() throws Exception {
    	translator = new ODataExecutionFactory();
    	translator.start();
    	
    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");
    	utility = new TranslationUtility(metadata);
    }
    
    private void helpExecute(String query, String expected) throws Exception {
    	Select cmd = (Select)this.utility.parseCommand(query);
    	String jpaCommand = ODataSQLVisitor.getODataString(cmd, translator, utility.createRuntimeMetadata());
    	assertEquals(expected, jpaCommand);
    }

    @Test
    public void testSelectStar() throws Exception {
    	helpExecute("select * from customers", "Customers?$select=CustomerID,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax");
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
    	helpExecute("select UnitPrice from OrderDetails where OrderID = 1 and ProductID = 12 and Quantity = 2", "OrderDetails(ProductID=12,OrderID=1)?$filter=Quantity eq 2&$select=UnitPrice");
    }    
    
    @Test
    public void testMultiKeyKeyBasedFilterOr() throws Exception {
    	helpExecute("select UnitPrice from OrderDetails where (OrderID = 1 and ProductID = 12) or Quantity = 2", "OrderDetails?$filter=(OrderID eq 1 AND ProductID eq 12) OR Quantity eq 2&$select=UnitPrice");
    }   
    
    @Test
    public void testPartialPK() throws Exception {
    	helpExecute("select UnitPrice from OrderDetails where Quantity >= 2 and ProductID = 12", "OrderDetails?$filter=Quantity ge 2 AND ProductID eq 12&$select=UnitPrice");
    }    
    
    @Test
    public void testSimpleJoin() throws Exception {
    	helpExecute("SELECT od.UnitPrice FROM Orders o JOIN OrderDetails od ON o.OrderID=od.OrderID and o.OrderID=12", "Order(12)/OrderDetails?$select=UnitPrice");
    }     
    
    @Test
    public void testEmbeddedJoin() throws Exception {
    	
    }
    
    @Test
    public void testJoinColumnInSelect() {
    	
    }
}
