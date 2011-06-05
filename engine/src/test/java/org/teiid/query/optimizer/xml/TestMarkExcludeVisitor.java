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

package org.teiid.query.optimizer.xml;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;

import junit.framework.TestCase;

import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingOutputter;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.processor.xml.TestXMLProcessor;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;



/** 
 * 
 */
public class TestMarkExcludeVisitor extends TestCase {
    
    void helpTest(String sql, String expected) throws Exception{
        QueryMetadataInterface metadata = TestXMLProcessor.exampleMetadataCached();
        Query query = (Query)TestXMLProcessor.helpGetCommand(sql, metadata); 

        Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(query, true);
        GroupSymbol group = groups.iterator().next();
        
        MappingDocument docOrig = (MappingDocument)metadata.getMappingNode(metadata.getGroupID(group.getName())); 
        MappingDocument doc = docOrig.clone(); 
        
        doc = XMLPlanner.preMarkExcluded(query, doc);       
        XMLPlanner.removeExcluded(doc);
        
        MappingOutputter out = new MappingOutputter();
        StringWriter sw = new StringWriter();
        out.write(doc, new PrintWriter(sw));
        String actual = sw.toString();
        
        expected = expected.replaceAll("\\s*<", "<"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, actual);
    }
    
    public void testAttribute() throws Exception {        
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                "<xmlMapping>" +  //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>true</formattedDocument>" +  //$NON-NLS-1$                                                                
                "   <mappingNode>" +  //$NON-NLS-1$
                "       <name>Catalogs</name>" +  //$NON-NLS-1$
                "       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "       <mappingNode>" +  //$NON-NLS-1$
                "           <name>Catalog</name>" +  //$NON-NLS-1$
                "           <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "           <mappingNode>" +  //$NON-NLS-1$
                "               <name>Items</name>" +  //$NON-NLS-1$
                "               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>Item</name>" +  //$NON-NLS-1$
                "                       <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                       <source>xmltest.group.items</source>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                       <mappingNode>" +  //$NON-NLS-1$
                "                           <name>ItemID</name>" +  //$NON-NLS-1$
                "                           <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                           <symbol>xmltest.group.items.itemNum</symbol>" +  //$NON-NLS-1$
                "                           <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                       </mappingNode>" +  //$NON-NLS-1$
                "                   </mappingNode>" +  //$NON-NLS-1$
                "           </mappingNode>" +  //$NON-NLS-1$
                "       </mappingNode>" +  //$NON-NLS-1$
                "   </mappingNode>" +  //$NON-NLS-1$
                "</xmlMapping>"; //$NON-NLS-1$
        
        helpTest("SELECT Catalogs.Catalog.Items.Item.@ItemID FROM xmltest.doc9", expected); //$NON-NLS-1$                
    }
    
    
    public void testGroupNode() throws Exception{
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                "<xmlMapping>" +  //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>true</formattedDocument>" +  //$NON-NLS-1$                                                                
                "   <mappingNode>" +  //$NON-NLS-1$
                "       <name>Catalogs</name>" +  //$NON-NLS-1$
                "       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "       <mappingNode>" +  //$NON-NLS-1$
                "           <name>Catalog</name>" +  //$NON-NLS-1$
                "           <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "           <mappingNode>" +  //$NON-NLS-1$
                "               <name>Items</name>" +  //$NON-NLS-1$
                "               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "               <mappingNode>" +  //$NON-NLS-1$
                "                   <name>Item</name>" +  //$NON-NLS-1$
                "                   <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                   <source>xmltest.group.items</source>" +  //$NON-NLS-1$                
                "                   <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>Suppliers</name>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                       <mappingNode>" +  //$NON-NLS-1$
                "                           <name>Supplier</name>" +  //$NON-NLS-1$
                "                           <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                           <source>xmltest.suppliers</source>" +  //$NON-NLS-1$                
                "                           <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>SupplierID</name>" +  //$NON-NLS-1$
                "                               <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                               <symbol>xmltest.suppliers.supplierNum</symbol>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>Name</name>" +  //$NON-NLS-1$
                "                               <symbol>xmltest.suppliers.supplierName</symbol>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>Zip</name>" +  //$NON-NLS-1$
                "                               <symbol>xmltest.suppliers.supplierZipCode</symbol>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>Orders</name>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                               <mappingNode>" +  //$NON-NLS-1$
                "                                   <name>Order</name>" +  //$NON-NLS-1$
                "                                   <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                                   <source>xmltest.orders</source>" +  //$NON-NLS-1$                
                "                                   <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderID</name>" +  //$NON-NLS-1$
                "                                       <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderNum</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderDate</name>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderDate</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderQuantity</name>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderQty</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderStatus</name>" +  //$NON-NLS-1$
                "                                       <minOccurs>0</minOccurs>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderStatus</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                               </mappingNode>" +  //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                       </mappingNode>" +  //$NON-NLS-1$
                "                   </mappingNode>" +  //$NON-NLS-1$
                "               </mappingNode>" +  //$NON-NLS-1$
                "           </mappingNode>" +  //$NON-NLS-1$
                "       </mappingNode>" +  //$NON-NLS-1$
                "   </mappingNode>" +  //$NON-NLS-1$
                "</xmlMapping>"; //$NON-NLS-1$
        
        helpTest("SELECT Catalogs.Catalog.Items.Item.Suppliers.Supplier.* FROM xmltest.doc9", expected); //$NON-NLS-1$
        helpTest("SELECT Supplier.* FROM xmltest.doc9", expected); //$NON-NLS-1$
    }    
    
    public void testElements() throws Exception {
        String expected="<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                "<xmlMapping>" +  //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>true</formattedDocument>" +  //$NON-NLS-1$                                                                
                "   <mappingNode>" +  //$NON-NLS-1$
                "       <name>Catalogs</name>" +  //$NON-NLS-1$
                "       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "       <mappingNode>" +  //$NON-NLS-1$
                "           <name>Catalog</name>" +  //$NON-NLS-1$
                "           <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "           <mappingNode>" +  //$NON-NLS-1$
                "               <name>Items</name>" +  //$NON-NLS-1$
                "               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "               <mappingNode>" +  //$NON-NLS-1$
                "                   <name>Item</name>" +  //$NON-NLS-1$
                "                   <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                   <source>xmltest.group.items</source>" +  //$NON-NLS-1$                
                "                   <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>ItemID</name>" +  //$NON-NLS-1$
                "                       <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                       <symbol>xmltest.group.items.itemNum</symbol>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                   </mappingNode>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>Suppliers</name>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                       <mappingNode>" +  //$NON-NLS-1$
                "                           <name>Supplier</name>" +  //$NON-NLS-1$
                "                           <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                           <source>xmltest.suppliers</source>" +  //$NON-NLS-1$                
                "                           <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>SupplierID</name>" +  //$NON-NLS-1$
                "                               <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                               <symbol>xmltest.suppliers.supplierNum</symbol>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>"+ //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>Orders</name>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                               <mappingNode>" +  //$NON-NLS-1$
                "                                   <name>Order</name>" +  //$NON-NLS-1$
                "                                   <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                                   <source>xmltest.orders</source>" +  //$NON-NLS-1$                
                "                                   <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderID</name>" +  //$NON-NLS-1$
                "                                       <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderNum</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderQuantity</name>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderQty</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                               </mappingNode>" +  //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                       </mappingNode>" +  //$NON-NLS-1$
                "                   </mappingNode>" +  //$NON-NLS-1$
                "               </mappingNode>" +  //$NON-NLS-1$
                "           </mappingNode>" +  //$NON-NLS-1$
                "       </mappingNode>" +  //$NON-NLS-1$
                "   </mappingNode>" +  //$NON-NLS-1$
                "</xmlMapping>"; //$NON-NLS-1$
        
        helpTest("SELECT OrderQuantity, SupplierID, ItemID, OrderID FROM xmltest.doc9"  + //$NON-NLS-1$
                 " ORDER BY Catalogs.Catalog.Items.Item.Suppliers.Supplier.SupplierID ASC," + //$NON-NLS-1$
                 " OrderID DESC, Catalogs.Catalog.Items.Item.ItemID DESC", expected);           //$NON-NLS-1$
    }
    
    public void testSelectAll() throws Exception {
        String expected= "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
                "<xmlMapping>" +  //$NON-NLS-1$
                "<documentEncoding>UTF-8</documentEncoding>" +  //$NON-NLS-1$
                "<formattedDocument>true</formattedDocument>" +  //$NON-NLS-1$                                                                
                "   <mappingNode>" +  //$NON-NLS-1$
                "       <name>Catalogs</name>" +  //$NON-NLS-1$
                "       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "       <mappingNode>" +  //$NON-NLS-1$
                "           <name>Catalog</name>" +  //$NON-NLS-1$
                "           <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "           <mappingNode>" +  //$NON-NLS-1$
                "               <name>Items</name>" +  //$NON-NLS-1$
                "               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "               <mappingNode>" +  //$NON-NLS-1$
                "                   <name>Item</name>" +  //$NON-NLS-1$
                "                   <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                   <source>xmltest.group.items</source>" +  //$NON-NLS-1$
                "                   <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>ItemID</name>" +  //$NON-NLS-1$
                "                       <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                       <symbol>xmltest.group.items.itemNum</symbol>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                   </mappingNode>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>Name</name>" +  //$NON-NLS-1$
                "                       <symbol>xmltest.group.items.itemName</symbol>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                   </mappingNode>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>Quantity</name>" +  //$NON-NLS-1$
                "                       <symbol>xmltest.group.items.itemQuantity</symbol>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                   </mappingNode>" +  //$NON-NLS-1$
                "                   <mappingNode>" +  //$NON-NLS-1$
                "                       <name>Suppliers</name>" +  //$NON-NLS-1$
                "                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                       <mappingNode>" +  //$NON-NLS-1$
                "                           <name>Supplier</name>" +  //$NON-NLS-1$
                "                           <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                           <source>xmltest.suppliers</source>" +  //$NON-NLS-1$                
                "                           <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>SupplierID</name>" +  //$NON-NLS-1$
                "                               <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                               <symbol>xmltest.suppliers.supplierNum</symbol>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>Name</name>" +  //$NON-NLS-1$
                "                               <symbol>xmltest.suppliers.supplierName</symbol>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>Zip</name>" +  //$NON-NLS-1$
                "                               <symbol>xmltest.suppliers.supplierZipCode</symbol>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                           <mappingNode>" +  //$NON-NLS-1$
                "                               <name>Orders</name>" +  //$NON-NLS-1$
                "                               <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                               <mappingNode>" +  //$NON-NLS-1$
                "                                   <name>Order</name>" +  //$NON-NLS-1$
                "                                   <maxOccurs>-1</maxOccurs>" +  //$NON-NLS-1$
                "                                   <source>xmltest.orders</source>" +  //$NON-NLS-1$
                "                                   <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderID</name>" +  //$NON-NLS-1$
                "                                       <nodeType>attribute</nodeType>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderNum</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>"+ //$NON-NLS-1$                
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderDate</name>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderDate</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderQuantity</name>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderQty</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                                   <mappingNode>" +  //$NON-NLS-1$
                "                                       <name>OrderStatus</name>" +  //$NON-NLS-1$
                "                                       <minOccurs>0</minOccurs>" +  //$NON-NLS-1$
                "                                       <symbol>xmltest.orders.orderStatus</symbol>" +  //$NON-NLS-1$
                "                                       <includeAlways>false</includeAlways>" +  //$NON-NLS-1$
                "                                   </mappingNode>" +  //$NON-NLS-1$
                "                               </mappingNode>" +  //$NON-NLS-1$
                "                           </mappingNode>" +  //$NON-NLS-1$
                "                       </mappingNode>" +  //$NON-NLS-1$
                "                   </mappingNode>" +  //$NON-NLS-1$
                "               </mappingNode>" +  //$NON-NLS-1$
                "           </mappingNode>" +  //$NON-NLS-1$
                "       </mappingNode>" +  //$NON-NLS-1$
                "   </mappingNode>" +  //$NON-NLS-1$
                "</xmlMapping>" ; //$NON-NLS-1$
        
        // everything as it is..
        helpTest("SELECT * FROM xmltest.doc9", expected); //$NON-NLS-1$
    }
}
