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

package org.teiid.query.processor.xml;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.relational.rules.RuleChooseDependent;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.FakeMetadataObject;
import org.teiid.query.unittest.FakeMetadataStore;


public class TestXMLPlanningEnhancements {

    private FakeMetadataFacade getMetadata(String query) {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata();
        
        FakeMetadataStore store = metadata.getStore();
        
        FakeMetadataObject xmltest = store.findObject("xmltest", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        QueryNode rsQueryY = new QueryNode(query); //$NON-NLS-1$
        rsQueryY.addBinding("xmltest.group.items.itemNum"); //$NON-NLS-1$
        rsQueryY.addBinding("xmltest.group.items.itemNum"); //$NON-NLS-1$
        FakeMetadataObject rsQY = FakeMetadataFactory.createVirtualGroup("xmltest.suppliersY", xmltest, rsQueryY); //$NON-NLS-1$
        
        List rsElementsY = FakeMetadataFactory.createElements(rsQY, 
                                                              new String[] { "supplierNum", "supplierName", "supplierZipCode" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                                              new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
                                                         
        FakeMetadataObject doc18a = FakeMetadataFactory.createVirtualGroup("xmltest.doc18a", xmltest, TestXMLProcessor.createXMLPlanNested("xmltest.suppliersY")); //$NON-NLS-1$ //$NON-NLS-2$
        List docE18a = FakeMetadataFactory.createElements(doc18a, new String[] { "Catalogs", "Catalogs.Catalog", "Catalogs.Catalog.items", "Catalogs.Catalog.items.item", "Catalogs.Catalog.items.item.@ItemID", "Catalogs.Catalog.items.item.Name", "Catalogs.Catalog.items.item.Quantity", "Catalogs.Catalog.items.item.suppliers", "Catalogs.Catalog.items.item.suppliers.supplier", "Catalogs.Catalog.items.item.suppliers.supplier.@SupplierID", "Catalogs.Catalog.items.item.suppliers.supplier.zip", "Catalogs.Catalog.items.item.suppliers.supplier.Name" },  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$ //$NON-NLS-11$ //$NON-NLS-12$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        store.addObject(rsQY);
        store.addObjects(rsElementsY);
        store.addObject(doc18a);
        store.addObjects(docE18a);
        return metadata;
    }
    
    private FakeMetadataFacade getTempTableMetadata(boolean simpleTempSelect) {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata();
        
        FakeMetadataStore store = metadata.getStore();
        
        FakeMetadataObject xmltest = store.findObject("xmltest", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeMetadataObject docJoin = FakeMetadataFactory.createVirtualGroup("xmltest.docJoin", xmltest, createXMLPlanNestedJoin()); //$NON-NLS-1$
        List docEJoin = FakeMetadataFactory.createElements(docJoin, new String[] { "Catalogs", "Catalogs.Catalog", "Catalogs.Catalog.items", "Catalogs.Catalog.items.item", "Catalogs.Catalog.items.item.@ItemID", "Catalogs.Catalog.items.item.Name", "Catalogs.Catalog.items.item.Quantity", "Catalogs.Catalog.items.item.suppliers", "Catalogs.Catalog.items.item.suppliers.supplier", "Catalogs.Catalog.items.item.suppliers.supplier.@SupplierID", "Catalogs.Catalog.items.item.suppliers.supplier.zip", "Catalogs.Catalog.items.item.suppliers.supplier.Name", "Catalogs.Catalog.items.item.suppliers.supplier.orders", "Catalogs.Catalog.items.item.suppliers.supplier.orders.order", "Catalogs.Catalog.items.item.suppliers.supplier.orders.order.@OrderID" ,"Catalogs.Catalog.items.item.suppliers.supplier.orders.order.OrderDate", "Catalogs.Catalog.items.item.suppliers.supplier.orders.order.OrderQuantity", "Catalogs.Catalog.items.item.suppliers.supplier.orders.order.OrderStatus"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$ //$NON-NLS-11$ //$NON-NLS-12$ //$NON-NLS-13$ //$NON-NLS-14$ //$NON-NLS-15$ //$NON-NLS-16$ //$NON-NLS-17$ //$NON-NLS-18$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        QueryNode tempQueryJoin = null;
        if (!simpleTempSelect) {
            tempQueryJoin = new QueryNode("SELECT stock.orders.* FROM stock.orders"); //$NON-NLS-1$ //$NON-NLS-2$
        } else {
            tempQueryJoin = new QueryNode("SELECT stock.orders.* FROM stock.orders join stock.suppliers on stock.orders.supplierFK=stock.suppliers.supplierNum"); //$NON-NLS-1$ //$NON-NLS-2$
        }

        FakeMetadataObject tempJoin = FakeMetadataFactory.createVirtualGroup("tempGroup.orders", xmltest, tempQueryJoin); //$NON-NLS-1$

        // Created virtual group w/ nested result set & binding - selects from 2nd temp root group
        QueryNode rsQueryJoin = null;
        if (!simpleTempSelect) {
            rsQueryJoin = new QueryNode("SELECT orderNum, orderDate, orderQty, orderStatus FROM tempGroup.orders join stock.suppliers on tempGroup.orders.supplierFK=stock.suppliers.supplierNum WHERE itemFK = ? AND supplierNameFK = ?"); //$NON-NLS-1$ //$NON-NLS-2$
            rsQueryJoin.addBinding("xmltest.group.items.itemNum"); //$NON-NLS-1$
            rsQueryJoin.addBinding("xmltest.suppliers.supplierName"); //$NON-NLS-1$
        } else {
            rsQueryJoin = new QueryNode("SELECT orderNum, orderDate, orderQty, orderStatus FROM tempGroup.orders WHERE supplierNameFK = ?"); //$NON-NLS-1$ //$NON-NLS-2$
            rsQueryJoin.addBinding("xmltest.suppliers.supplierName"); //$NON-NLS-1$
        }
        FakeMetadataObject rsJoin = FakeMetadataFactory.createVirtualGroup("xmltest.ordersC", xmltest, rsQueryJoin); //$NON-NLS-1$
        
        List tempJoinElements = FakeMetadataFactory.createElements(tempJoin, 
                                                                   new String[] { "orderNum", "itemFK", "supplierFK", "supplierNameFK", "orderDate", "orderQty", "orderStatus" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
                                                                   new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING});

        List rsElementsJoin = FakeMetadataFactory.createElements(rsJoin, 
                                                                 new String[] { "orderNum", "orderDate", "orderQty", "orderStatus" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                                                                 new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING});
        store.addObject(docJoin);
        store.addObjects(docEJoin);
        store.addObjects(tempJoinElements);
        store.addObjects(rsElementsJoin);        
        store.addObject(tempJoin);
        store.addObject(rsJoin);
        
        return metadata;
    }
    
    private static MappingNode createXMLPlanNestedJoin() {
        
        MappingDocument doc = new MappingDocument(true);
        
        MappingElement root = doc.addChildElement(new MappingElement("Catalogs")); //$NON-NLS-1$

        root.setStagingTables(Arrays.asList(new String[] {"tempGroup.orders"})); //$NON-NLS-1$ 
        
        MappingElement cats = root.addChildElement(new MappingElement("Catalog")); //$NON-NLS-1$
        MappingElement items = cats.addChildElement(new MappingElement("Items")); //$NON-NLS-1$

        MappingElement item = items.addChildElement(new MappingElement("Item")); //$NON-NLS-1$
        item.setSource("xmltest.group.items"); //$NON-NLS-1$
        item.setMaxOccurrs(-1);    
        item.addAttribute(new MappingAttribute("ItemID", "xmltest.group.items.itemNum")); //$NON-NLS-1$ //$NON-NLS-2$
        item.addChildElement(new MappingElement("Name", "xmltest.group.items.itemName")); //$NON-NLS-1$ //$NON-NLS-2$
        item.addChildElement(new MappingElement("Quantity", "xmltest.group.items.itemQuantity")); //$NON-NLS-1$ //$NON-NLS-2$
        
        //NESTED STUFF======================================================================
        MappingElement nestedWrapper = item.addChildElement(new MappingElement("Suppliers")); //$NON-NLS-1$
        
        MappingElement supplier = nestedWrapper.addChildElement(new MappingElement("Supplier")); //$NON-NLS-1$
        supplier.setSource("xmltest.suppliers"); //$NON-NLS-1$
        supplier.setMaxOccurrs(-1);
        supplier.addAttribute(new MappingAttribute("SupplierID", "xmltest.suppliers.supplierNum")); //$NON-NLS-1$ //$NON-NLS-2$
        supplier.addChildElement(new MappingElement("Name","xmltest.suppliers.supplierName")); //$NON-NLS-1$ //$NON-NLS-2$
        supplier.addChildElement(new MappingElement("Zip", "xmltest.suppliers.supplierZipCode")); //$NON-NLS-1$ //$NON-NLS-2$
        
        MappingElement ordersWrapper = supplier.addChildElement(new MappingElement("Orders")); //$NON-NLS-1$        
        
        MappingElement order = ordersWrapper.addChildElement(new MappingElement("Order")); //$NON-NLS-1$
        order.setSource("xmltest.ordersC"); //$NON-NLS-1$
        order.setMaxOccurrs(-1);
        order.addAttribute(new MappingAttribute("OrderID", "xmltest.ordersC.orderNum")); //$NON-NLS-1$ //$NON-NLS-2$
        order.addChildElement(new MappingElement("OrderDate", "xmltest.ordersc.orderDate")); //$NON-NLS-1$ //$NON-NLS-2$
        order.addChildElement(new MappingElement("OrderQuantity", "xmltest.ordersC.orderQty")); //$NON-NLS-1$ //$NON-NLS-2$
        order.addChildElement(new MappingElement("OrderStatus", "xmltest.ordersC.orderStatus")) //$NON-NLS-1$ //$NON-NLS-2$
            .setMinOccurrs(0);                
        //NESTED STUFF======================================================================
        return doc;  
    } 
    
    @Test public void testBaseballPlayersDocDefect19541() throws Exception {
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleCase3225();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerCase3225(metadata);
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<BaseballPlayers>\r\n" + //$NON-NLS-1$
            "   <Player PlayerID=\"1001\">\r\n" + //$NON-NLS-1$
            "      <FirstName>Albert</FirstName>\r\n" + //$NON-NLS-1$
            "      <LastName>Pujols</LastName>\r\n" + //$NON-NLS-1$
            "      <Manager ManagerID=\"1004\">\r\n" + //$NON-NLS-1$
            "         <FirstName>Tony</FirstName>\r\n" + //$NON-NLS-1$
            "         <LastName>LaRussa</LastName>\r\n" + //$NON-NLS-1$
            "         <Owner OwnerID=\"1009\">\r\n" + //$NON-NLS-1$
            "            <FirstName>Bill</FirstName>\r\n" + //$NON-NLS-1$
            "            <LastName>DeWitt</LastName>\r\n" + //$NON-NLS-1$
            "         </Owner>\r\n" + //$NON-NLS-1$
            "      </Manager>\r\n" + //$NON-NLS-1$
            "   </Player>\r\n" + //$NON-NLS-1$
            "   <Player PlayerID=\"1002\">\r\n" + //$NON-NLS-1$
            "      <FirstName>Jim</FirstName>\r\n" + //$NON-NLS-1$
            "      <LastName>Edmunds</LastName>\r\n" + //$NON-NLS-1$
            "      <Manager ManagerID=\"1004\">\r\n" + //$NON-NLS-1$
            "         <FirstName>Tony</FirstName>\r\n" + //$NON-NLS-1$
            "         <LastName>LaRussa</LastName>\r\n" + //$NON-NLS-1$
            "         <Owner OwnerID=\"1009\">\r\n" + //$NON-NLS-1$
            "            <FirstName>Bill</FirstName>\r\n" + //$NON-NLS-1$
            "            <LastName>DeWitt</LastName>\r\n" + //$NON-NLS-1$
            "         </Owner>\r\n" + //$NON-NLS-1$
            "      </Manager>\r\n" + //$NON-NLS-1$
            "   </Player>\r\n" + //$NON-NLS-1$
            "   <Player PlayerID=\"1003\">\r\n" + //$NON-NLS-1$
            "      <FirstName>David</FirstName>\r\n" + //$NON-NLS-1$
            "      <LastName>Eckstein</LastName>\r\n" + //$NON-NLS-1$
            "      <Manager ManagerID=\"1004\">\r\n" + //$NON-NLS-1$
            "         <FirstName>Tony</FirstName>\r\n" + //$NON-NLS-1$
            "         <LastName>LaRussa</LastName>\r\n" + //$NON-NLS-1$
            "         <Owner OwnerID=\"1009\">\r\n" + //$NON-NLS-1$
            "            <FirstName>Bill</FirstName>\r\n" + //$NON-NLS-1$
            "            <LastName>DeWitt</LastName>\r\n" + //$NON-NLS-1$
            "         </Owner>\r\n" + //$NON-NLS-1$
            "      </Manager>\r\n" + //$NON-NLS-1$
            "   </Player>\r\n" + //$NON-NLS-1$
            "</BaseballPlayers>\r\n\r\n"; //$NON-NLS-1$
        
        TestXMLProcessor.helpTestProcess("select * from xmltest.playersDoc where owner.@ownerid = '1009'", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
        
    }   
    
    @Test public void testNested2WithContextCriteria5c() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5c.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);

        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9 WHERE NOT(SupplierID='52') AND (OrderID='5' OR OrderID='2')", expectedDoc, metadata, dataMgr); //$NON-NLS-1$        
    }
    
    @Test public void testNested2WithContextCriteria5d() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5d.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);

        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9 WHERE OrderID='5' OR context(SupplierID, OrderID)='2'", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }

    @Test public void testNested2WithContextCriteria5d1() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5d.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);

        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9 WHERE context(SupplierID, OrderID)='5' OR OrderID='2'", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }

    @Test public void testNested2WithContextCriteria5e() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String resultFile = "TestXMLProcessor-testNested2WithContextCriteria5e.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);

        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9 WHERE OrderID='5' OR SupplierID='52'", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }
    
    @Test public void testXQTChoice_6796() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata2();
        FakeDataManager dataMgr = TestXMLProcessor.exampleXQTDataManager(metadata);
        String resultFile = "TestXMLProcessor-testXQTChoice_6796.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xqttest.doc4 WHERE root.key.keys.nestedkey = 4", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
    }
    
    @Test public void testOrderByWithChoiceCriteriaElement() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata2();
        FakeDataManager dataMgr = TestXMLProcessor.exampleXQTDataManager(metadata);
        String expectedDoc = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root>\n   <wrapper/>\n   <wrapper/>\n   <wrapper/>\n   <wrapper/>\n   <wrapper/>\n   <wrapper/>\n   <wrapper/>\n   <wrapper/>\n   <wrapper/>\n</root>"; //$NON-NLS-1$
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xqttest.doc5 order by root.wrapper.key", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
    }
    
    @Test public void testXQTChoice_withContextCriteria() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata2();
        FakeDataManager dataMgr = TestXMLProcessor.exampleXQTDataManager(metadata);
        String resultFile = "TestXMLProcessor-testXQTChoice_withContextCriteria.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xqttest.doc4 WHERE context(root.key.keys.nestedkey, root.key.keys.nestedkey) = 4", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
    }
    
    @Test public void testXQTChoice_withContextCriteria1() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata2();
        FakeDataManager dataMgr = TestXMLProcessor.exampleXQTDataManager(metadata);
        String resultFile = "TestXMLProcessor-testXQTChoice_withContextCriteria1.xml"; //$NON-NLS-1$
        String expectedDoc = TestXMLProcessor.readFile(resultFile);
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xqttest.doc4 WHERE context(root.key.keys.nestedkey, root.key.keys.nestedkey) = 4 and context(root.wrapper.key.keys.nestedkey, root.wrapper.key.keys.nestedkey) = 3", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
    }
    
    @Test public void testMappingClassWithInlineViewAndCriteria() throws Exception {
        FakeMetadataFacade metadata = getMetadata("SELECT upper(x.supplierNum) as supplierNum, x.supplierName, x.supplierZipCode from (select stock.suppliers.supplierNum, supplierName, supplierZipCode, itemNum FROM stock.suppliers, stock.item_supplier WHERE stock.suppliers.supplierNum = stock.item_supplier.supplierNum) x where x.itemNum = ?"); //$NON-NLS-1$

        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = TestXMLProcessor.readFile("TestXMLPlanningEnhancements-testMappingClassWithStoredProcedureAndCriteria.xml"); //$NON-NLS-1$ 
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc18a where supplierID<56", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }
    
    @Test public void testMappingClassWithUnionAndCriteria() throws Exception {
        FakeMetadataFacade metadata = getMetadata("SELECT concat(stock.suppliers.supplierNum, '') as supplierNum, supplierName, supplierZipCode FROM stock.suppliers, stock.item_supplier WHERE stock.suppliers.supplierNum = stock.item_supplier.supplierNum AND stock.item_supplier.itemNum = ? union all SELECT concat(stock.suppliers.supplierNum, '1'), supplierName, convert(12345, string) FROM stock.suppliers WHERE stock.suppliers.supplierNum = ?"); //$NON-NLS-1$

        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = TestXMLProcessor.readFile("TestXMLPlanningEnhancements-testMappingClassWithStoredProcedureAndCriteria.xml"); //$NON-NLS-1$ 
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc18a where supplierID<56", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }
    
    @Test public void testMappingClassWithInputSetElementNameConflict() throws Exception {
        FakeMetadataFacade metadata = getMetadata("SELECT concat(stock.suppliers.supplierNum, '') as supplierNum, supplierName, supplierZipCode FROM stock.suppliers, stock.item_supplier WHERE stock.suppliers.supplierNum = stock.item_supplier.supplierNum AND stock.item_supplier.supplierNum = ?"); //$NON-NLS-1$

        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Catalogs>\n<Catalog>\n<Items/>\n</Catalog>\n</Catalogs>"; //$NON-NLS-1$ 
        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc18a where supplierID<56", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }
    
    @Test public void testAutoStagingFailsForMappingClassWithProcRelational() throws Exception {
        FakeMetadataFacade metadata = getMetadata("SELECT supplierNum, supplierName, supplierZipCode FROM v1.supplierProc where itemnum = ?"); //$NON-NLS-1$

        FakeMetadataObject v1 = FakeMetadataFactory.createVirtualModel("v1"); //$NON-NLS-1$
        
        FakeMetadataObject rs1 = FakeMetadataFactory.createResultSet("v1.rs1", v1, new String[] {"supplierNum", "supplierName", "supplierZipCode"}, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING,DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        FakeMetadataObject rs1p1 = FakeMetadataFactory.createParameter("ret", 1, ParameterInfo.RESULT_SET, DataTypeManager.DefaultDataTypes.OBJECT, rs1);  //$NON-NLS-1$
        FakeMetadataObject rs1p2 = FakeMetadataFactory.createParameter("itemNum", 2, ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING, null);  //$NON-NLS-1$
        QueryNode n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT concat(stock.suppliers.supplierNum, '') as supplierNum, supplierName, supplierZipCode FROM stock.suppliers, stock.item_supplier WHERE stock.suppliers.supplierNum = stock.item_supplier.supplierNum AND stock.item_supplier.itemNum = v1.supplierProc.itemNum; END"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject vt1 = FakeMetadataFactory.createVirtualProcedure("v1.supplierProc", v1, Arrays.asList(new FakeMetadataObject[] { rs1p1, rs1p2 }), n1); //$NON-NLS-1$

        metadata.getStore().addObject(vt1);

        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = TestXMLProcessor.readFile("TestXMLPlanningEnhancements-testMappingClassWithStoredProcedureAndCriteria.xml"); //$NON-NLS-1$ 
 
        XMLPlan plan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc18a where supplierID<56", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$

        Map stats = XMLProgramUtil.getProgramStats(plan.getOriginalProgram());
        assertNull(stats.get(ExecStagingTableInstruction.class));
    }
    
    @Test public void testMappingClassWitSelectDistinctAndCriteria() throws Exception {
        FakeMetadataFacade metadata = getMetadata("SELECT distinct '1' as supplierNum, '2' as supplierName, '3' as supplierZipCode FROM stock.suppliers, stock.item_supplier WHERE stock.suppliers.supplierNum = stock.item_supplier.supplierNum AND stock.item_supplier.itemNum like substring(?,1,1) || '%'"); //$NON-NLS-1$

        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);

        String expectedDoc = TestXMLProcessor.readFile("TestXMLPlanningEnhancements-testMappingClassWitSelectDistinctAndCriteria.xml"); //$NON-NLS-1$ 
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc18a", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }

    /**
     * based upon testNestedWithStoredQueryInMappingClass
     * 
     * Ensures that correlated references to outer scoped groups can
     * be used as inputs
     */
    @Test public void testMappingClassWithStoredProcedureAndCriteria() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = TestXMLProcessor.readFile("TestXMLPlanningEnhancements-testMappingClassWithStoredProcedureAndCriteria.xml"); //$NON-NLS-1$ 
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc18 where supplierID<56", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }
    
    public void defer_testXMLQueryWithFalseRootCriteria() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        String expectedDoc = ""; //$NON-NLS-1$ 
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc18 where 1 = 0", expectedDoc, metadata, dataMgr);         //$NON-NLS-1$
    }
    
    /**
     * @see #testNested2WithCriteria2
     */
    @Test public void testAutoStagingByCosting() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        FakeMetadataObject suppliers = metadata.getStore().findObject("stock.suppliers", FakeMetadataObject.GROUP); //$NON-NLS-1$
        FakeMetadataObject itemSuppliers = metadata.getStore().findObject("stock.item_supplier", FakeMetadataObject.GROUP); //$NON-NLS-1$

        suppliers.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10));
        itemSuppliers.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10));

        String expectedDoc = TestXMLProcessor.readFile("TestXMLProcessor-FullSuppliers.xml"); //$NON-NLS-1$
        
        XMLPlan plan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
        
        //check for staging; one for staging and for unloading
        Map stats = XMLProgramUtil.getProgramStats(plan.getOriginalProgram());
        assertEquals(2, ((List)stats.get(ExecStagingTableInstruction.class)).size());
    }
    
    /**
     * @see #testNested2WithCriteria2
     */
    @Test public void testAutoStagingFailsByCosting() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        FakeMetadataObject suppliers = metadata.getStore().findObject("stock.suppliers", FakeMetadataObject.GROUP); //$NON-NLS-1$
        FakeMetadataObject itemSuppliers = metadata.getStore().findObject("stock.item_supplier", FakeMetadataObject.GROUP); //$NON-NLS-1$

        suppliers.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10000));
        itemSuppliers.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10000));

        String expectedDoc = TestXMLProcessor.readFile("TestXMLProcessor-FullSuppliers.xml"); //$NON-NLS-1$
        
        XMLPlan plan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
        
        //check for no staging
        Map stats = XMLProgramUtil.getProgramStats(plan.getOriginalProgram());
        assertNull(stats.get(ExecStagingTableInstruction.class));
    }

    @Test public void testAutoStagingFailsByNoCache() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);

        String expectedDoc = TestXMLProcessor.readFile("TestXMLProcessor-FullSuppliers.xml"); //$NON-NLS-1$
        
        XMLPlan plan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9 OPTION NOCACHE", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
        
        //check for no staging
        Map stats = XMLProgramUtil.getProgramStats(plan.getOriginalProgram());
        assertNull(stats.get(ExecStagingTableInstruction.class));
    }    
    
    @Test public void testAutoStagingFailsByNoCacheByGroup() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadataCached();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);

        String expectedDoc = TestXMLProcessor.readFile("TestXMLProcessor-FullSuppliers.xml"); //$NON-NLS-1$
        
        XMLPlan plan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9 OPTION NOCACHE XMLTEST.SUPPLIERS", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
        
        //check for no staging by the mapping class name
        Map stats = XMLProgramUtil.getProgramStats(plan.getOriginalProgram());
        assertNull(stats.get(ExecStagingTableInstruction.class));
        
        plan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9 OPTION NOCACHE XMLTEST.SUPPLIERS", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
        
        //check for no staging by the alias mapping class name
        stats = XMLProgramUtil.getProgramStats(plan.getOriginalProgram());
        assertNull(stats.get(ExecStagingTableInstruction.class));
    }     
    
    // see the next test with costing information too.
    @Test public void testUseOfStagingCardinalityOnDependentJoinsNoCost() throws Exception {
        FakeMetadataFacade metadata = getTempTableMetadata(false);
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);

        String expectedDoc = TestXMLProcessor.readFile("TestXMLProcessor-FullSuppliers.xml"); //$NON-NLS-1$
        
        XMLPlan xmlPlan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.docJoin", expectedDoc, metadata, dataMgr); //$NON-NLS-1$        
        Map stats = XMLProgramUtil.getProgramStats(xmlPlan.getOriginalProgram());
        List list = (List)stats.get(ExecSqlInstruction.class);
        
        ExecSqlInstruction instr = (ExecSqlInstruction)list.get(2);
        
        ProcessorPlan plan = instr.info.getPlan();  
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin (**We are merge join with out any costing info**)
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });        
        
        TestOptimizer.checkDependentJoinCount(plan, 0);
    }    
    
    @Test public void testUseOfStagingCardinalityOnDependentJoinsWithCost() throws Exception {
        FakeMetadataFacade metadata = getTempTableMetadata(false);
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        FakeMetadataObject orders = metadata.getStore().findObject("stock.orders", FakeMetadataObject.GROUP); //$NON-NLS-1$
        FakeMetadataObject suppliers = metadata.getStore().findObject("stock.suppliers", FakeMetadataObject.GROUP); //$NON-NLS-1$

        // supply the costing information for OrdersC
        orders.putProperty(FakeMetadataObject.Props.CARDINALITY, BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE - 1);
        suppliers.putProperty(FakeMetadataObject.Props.CARDINALITY, RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1);        

        String expectedDoc = TestXMLProcessor.readFile("TestXMLProcessor-FullSuppliers.xml"); //$NON-NLS-1$
        
        XMLPlan xmlPlan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.docJoin", expectedDoc, metadata, dataMgr); //$NON-NLS-1$        
        Map stats = XMLProgramUtil.getProgramStats(xmlPlan.getOriginalProgram());
        List list = (List)stats.get(ExecSqlInstruction.class);
        
        ExecSqlInstruction instr = (ExecSqlInstruction)list.get(2);
        
        ProcessorPlan plan = instr.info.getPlan();        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin (**We are merge join with out any costing info**)
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });   
        
        TestOptimizer.checkDependentJoinCount(plan, 1);
    } 
    
    @Test public void testNoRedundentStagingTables() throws Exception {
        FakeMetadataFacade metadata = getTempTableMetadata(true);
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        FakeMetadataObject suppliers = metadata.getStore().findObject("stock.suppliers", FakeMetadataObject.GROUP); //$NON-NLS-1$
        FakeMetadataObject orders = metadata.getStore().findObject("stock.orders", FakeMetadataObject.GROUP); //$NON-NLS-1$

        suppliers.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10));
        orders.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(10));

        String expectedDoc = TestXMLProcessor.readFile("TestXMLProcessor-OnlySupplier51.xml"); //$NON-NLS-1$
        
        XMLPlan xmlPlan = (XMLPlan)TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.docJoin where context(Supplier, SupplierID) = 51", expectedDoc, metadata, dataMgr); //$NON-NLS-1$        
        //check for staging; one for staging and for unloading - only two pairs are expected
        Map stats = XMLProgramUtil.getProgramStats(xmlPlan.getOriginalProgram());
        assertEquals(4, ((List)stats.get(ExecStagingTableInstruction.class)).size());
    }
    
    @Test public void testEmptyMappingClass() throws Exception {
        FakeMetadataFacade metadata = TestXMLProcessor.exampleMetadata();
        FakeDataManager dataMgr = TestXMLProcessor.exampleDataManagerNested(metadata);
        FakeMetadataObject suppliers = metadata.getStore().findObject("xmltest.suppliers", FakeMetadataObject.GROUP); //$NON-NLS-1$
        QueryNode node = (QueryNode)suppliers.getProperty(FakeMetadataObject.Props.PLAN);
        node.setQuery(null);
        
        FakeMetadataObject docGroup = metadata.getStore().findObject("xmltest.doc9", FakeMetadataObject.GROUP); //$NON-NLS-1$
        MappingDocument doc = (MappingDocument)docGroup.getProperty(FakeMetadataObject.Props.PLAN);
        MappingNode suppliersNode = MappingNode.findNode(doc, "Catalogs.Catalog.Items.Item.Suppliers.Supplier".toUpperCase());
        suppliersNode.setExclude(true);
        
        String expectedDoc = TestXMLProcessor.readFile("TestXMLPlanningEnhancements-testEmptyMappingClass.xml"); //$NON-NLS-1$
        
        TestXMLProcessor.helpTestProcess("SELECT * FROM xmltest.doc9", expectedDoc, metadata, dataMgr); //$NON-NLS-1$
    }
}
