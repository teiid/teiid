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

package org.teiid.translator.simpledb.visitors;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Command;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.SimpleDBExecutionFactory;
import org.teiid.translator.simpledb.SimpleDBSQLVisitor;

@SuppressWarnings("nls")
public class TestSimpleDBSQLVisitor {

    @Test 
    public void testSelect() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" string, attribute string);", "x", "y");
        TranslationUtility tu = new TranslationUtility(tm);

        Command c = tu.parseCommand("select \"itemname()\" from item");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT itemName() FROM item", visitor.toString());

        c = tu.parseCommand("select \"itemname()\", attribute from item");
        visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item", visitor.toString());
        assertEquals(2, visitor.getProjectedColumns().size());
    }
    
    @Test 
    public void testNE() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" string, attribute string);", "x", "y");
        TranslationUtility tu = new TranslationUtility(tm);

        Command c = tu.parseCommand("select \"itemname()\" from item where \"itemname()\" <> 'name'");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT itemName() FROM item WHERE itemName() != 'name'", visitor.toString());
    }    
    
    @Test 
    public void testComparisionWithOR() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" string, attribute string);", "x", "y");
        TranslationUtility tu = new TranslationUtility(tm);

        Command c = tu.parseCommand("select \"itemname()\" from item where \"itemname()\" > 'name' and attribute < 'name'");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT itemName() FROM item WHERE itemName() > 'name' AND attribute < 'name'", visitor.toString());
        
    }    
    
    @Test(expected=TranslatorException.class) 
    public void testOrderBy() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" string, attribute string);", "x", "y");
        TranslationUtility tu = new TranslationUtility(tm);

        Command c = tu.parseCommand("select * from item order by \"itemName()\"");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        visitor.checkExceptions();
        assertEquals("SELECT attribute FROM item ORDER BY itemName()", visitor.toString());
    } 
    
    @Test
    public void testOrderByAllow() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" string, attribute string);", "x", "y");
        TranslationUtility tu = new TranslationUtility(tm);

        Command c = tu.parseCommand("select * from item where \"itemName()\" = 'name' order by \"itemName()\"");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        visitor.checkExceptions();
        assertEquals("SELECT attribute FROM item WHERE itemName() = 'name' ORDER BY itemName()", visitor.toString());
    }     
    
    @Test 
    public void testIN() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" integer, attribute string);", "x", "y");
        TranslationUtility tu = new TranslationUtility(tm);

        Command c = tu.parseCommand("select * from item where \"itemName()\" in (1, 2)");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE itemName() IN ('1', '2')", visitor.toString());
    }
    
    @Test 
    public void testEvery() throws Exception {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();
        
        MetadataFactory mf = TestDDLParser.helpParse("create foreign table item (\"itemName()\" integer, attribute string[]);", "y");
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        TranslationUtility tu = new TranslationUtility(metadata);

        Command c = tu.parseCommand("select * from item where simpledb.every(attribute) = '1'");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE SIMPLEDB.EVERY(attribute) = '1'", visitor.toString());
    } 
    
    @Test 
    public void testEveryLike() throws Exception {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();
        
        MetadataFactory mf = TestDDLParser.helpParse("create foreign table item (\"itemName()\" integer, attribute string[]);", "y");
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        TranslationUtility tu = new TranslationUtility(metadata);

        Command c = tu.parseCommand("select * from item where simpledb.every(attribute) like '1%'");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE SIMPLEDB.EVERY(attribute) LIKE '1%'", visitor.toString());
    }   
    
    @Test 
    public void testEveryNotNull() throws Exception {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();
        
        MetadataFactory mf = TestDDLParser.helpParse("create foreign table item (\"itemName()\" integer, attribute string[]);", "y");
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        TranslationUtility tu = new TranslationUtility(metadata);

        Command c = tu.parseCommand("select * from item where simpledb.every(attribute) is not null");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE SIMPLEDB.EVERY(attribute) IS NOT NULL", visitor.toString());
    }     
    
    @Test 
    public void testEvery2() throws Exception {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();
        
        MetadataFactory mf = TestDDLParser.helpParse("create foreign table item (\"itemName()\" integer, attribute string[]);", "y");
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        TranslationUtility tu = new TranslationUtility(metadata);

        Command c = tu.parseCommand("select * from item where simpledb.every(attribute) = '1' or  simpledb.every(attribute) = '2'");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE SIMPLEDB.EVERY(attribute) IN ('2', '1')", visitor.toString());
    }    
    
    @Test 
    public void testIntersection() throws Exception {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();
        
        MetadataFactory mf = TestDDLParser.helpParse("create foreign table item (\"itemName()\" integer, attribute string[]);", "y");
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        TranslationUtility tu = new TranslationUtility(metadata);

        Command c = tu.parseCommand("select * from item where simpledb.intersection(attribute,'1', '2')");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE attribute = '1' INTERSECTION attribute = '2'", visitor.toString());
    }    
    
    @Test 
    public void testIntersection2() throws Exception {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();
        
        MetadataFactory mf = TestDDLParser.helpParse("create foreign table item (\"itemName()\" integer, attribute string[]);", "y");
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        TranslationUtility tu = new TranslationUtility(metadata);

        Command c = tu.parseCommand("select * from item where simpledb.intersection(attribute,'1', '2', '3') = true");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE attribute = '1' INTERSECTION attribute = '2' INTERSECTION attribute = '3'", visitor.toString());
    }    
    
    @Test 
    public void testArrayCompare() throws Exception {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();
        
        MetadataFactory mf = TestDDLParser.helpParse("create foreign table item (\"itemName()\" integer, attribute string[]);", "y");
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        TranslationUtility tu = new TranslationUtility(metadata);

        Command c = tu.parseCommand("select * from item where attribute = ('1','2')");
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(c);
        assertEquals("SELECT attribute FROM item WHERE attribute = '1' OR attribute = '2'", visitor.toString());
    }     
}
