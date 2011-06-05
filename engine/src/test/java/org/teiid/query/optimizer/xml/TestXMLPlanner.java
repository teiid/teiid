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

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.id.IntegerIDFactory;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingSequenceNode;
import org.teiid.query.mapping.xml.Namespace;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.xml.BlockInstruction;
import org.teiid.query.processor.xml.EndBlockInstruction;
import org.teiid.query.processor.xml.ExecSqlInstruction;
import org.teiid.query.processor.xml.ExecStagingTableInstruction;
import org.teiid.query.processor.xml.MoveCursorInstruction;
import org.teiid.query.processor.xml.Program;
import org.teiid.query.processor.xml.TestXMLProcessor;
import org.teiid.query.processor.xml.WhileInstruction;
import org.teiid.query.processor.xml.XMLPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestXMLPlanner {

    // ################################## TEST HELPERS ################################

    public static XMLPlan helpPlan(String sql, QueryMetadataInterface md) throws Exception {
        Command command = TestXMLProcessor.helpGetCommand(sql, md);
        return preparePlan(command, md, TestOptimizer.getGenericFinder(), null);
    }

    private void helpPlanException(String sql, QueryMetadataInterface md) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        Command command = TestXMLProcessor.helpGetCommand(sql, md);

        try {
            preparePlan(command, md, TestOptimizer.getGenericFinder(), null);
            fail("Expected exception for planning " + sql); //$NON-NLS-1$
        } catch (QueryPlannerException e) {
            // this is expected
        } 
    }

    public static TransformationMetadata example1() {
    	MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = RealMetadataFactory.createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema vm1 = RealMetadataFactory.createVirtualModel("vm1", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = RealMetadataFactory.createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = RealMetadataFactory.createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm1g3 = RealMetadataFactory.createPhysicalGroup("g3", pm1); //$NON-NLS-1$
        Table pm1g4 = RealMetadataFactory.createPhysicalGroup("g4", pm1); //$NON-NLS-1$
        Table pm1g5 = RealMetadataFactory.createPhysicalGroup("g5", pm1); //$NON-NLS-1$

        // Create physical elements
        RealMetadataFactory.createElements(
                pm1g1,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                pm1g2,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                pm1g3,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                pm1g4,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                pm1g5,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT * FROM tm1.g1"); //$NON-NLS-1$ 
        //selects from temp group
        Table vm1g1 = RealMetadataFactory.createVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        QueryNode vm1g2n1 =
            new QueryNode("SELECT * FROM pm1.g2 where pm1.g2.e1=?"); //$NON-NLS-1$ 
        vm1g2n1.addBinding("vm1.g1.e1"); //$NON-NLS-1$
        Table vm1g2 = RealMetadataFactory.createVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$

        QueryNode vm1g3n1 =
            new QueryNode("SELECT * FROM pm1.g3 where pm1.g3.e1=?"); //$NON-NLS-1$ 
        vm1g3n1.addBinding("vm1.g2.e1"); //$NON-NLS-1$
        Table vm1g3 = RealMetadataFactory.createVirtualGroup("g3", vm1, vm1g3n1); //$NON-NLS-1$

        QueryNode vm1g4n1 = new QueryNode("SELECT * FROM pm1.g4"); //$NON-NLS-1$ 
        Table vm1g4 = RealMetadataFactory.createVirtualGroup("g4", vm1, vm1g4n1); //$NON-NLS-1$

        QueryNode vm1g5n1 =
            new QueryNode(
                "SELECT * FROM pm1.g5 where pm1.g5.e1=? AND pm1.g5.e2=?"); //$NON-NLS-1$
        vm1g5n1.addBinding("vm1.g4.e1"); //$NON-NLS-1$
        vm1g5n1.addBinding("vm1.g1.e1"); //$NON-NLS-1$
        Table vm1g5 = RealMetadataFactory.createVirtualGroup("g5", vm1, vm1g5n1); //$NON-NLS-1$

        QueryNode tempGroup1 =
            new QueryNode("SELECT * FROM pm1.g1 where e2 < '5'"); //$NON-NLS-1$ 
        Table tm1g1 = RealMetadataFactory.createVirtualGroup("tm1.g1", vm1, tempGroup1); //$NON-NLS-1$

        // Create virtual elements
        RealMetadataFactory.createElements(
                vm1g1,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                vm1g2,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                vm1g3,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                vm1g4,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                vm1g5,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(
                tm1g1,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });

        // Create virtual documents
        // DOC 1
        Table doc1 = RealMetadataFactory.createXmlDocument("doc1", vm1, doc1()); //$NON-NLS-1$
        RealMetadataFactory.createElements(
                doc1,
                new String[] {
                    "a0", //$NON-NLS-1$
                    "a0.a1", //$NON-NLS-1$
                    "a0.a1.a1", //$NON-NLS-1$
                    "a0.a1.b1", //$NON-NLS-1$
                    "a0.a1.c1" }, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });

        // DOC 2 
        Table doc2 = RealMetadataFactory.createXmlDocument("doc2", vm1, doc2()); //$NON-NLS-1$
        RealMetadataFactory.createElements(
                doc2,
                new String[] { "a1" }, //$NON-NLS-1$
                new String[] { DataTypeManager.DefaultDataTypes.STRING });

        // DOC 3
        Table doc3 = RealMetadataFactory.createXmlDocument("doc3", vm1, doc3()); //$NON-NLS-1$
        RealMetadataFactory.createElements(
                doc3,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.n1", //$NON-NLS-1$
                    "root.n1.m1", //$NON-NLS-1$
                    "root.n1.m1.n2", //$NON-NLS-1$
                    "root.n1.m1.n2.leaf1" }, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });

        // DOC 4
        Table doc4 = RealMetadataFactory.createXmlDocument("doc4", vm1, doc4()); //$NON-NLS-1$
        RealMetadataFactory.createElements(
                doc4,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.n4a", //$NON-NLS-1$
                    "root.n4a.n4b", //$NON-NLS-1$
                    "root.n4a.n4c", //$NON-NLS-1$
                    "root.n4a.fake", //$NON-NLS-1$
                    "root.n4a.n4c.n4d", //$NON-NLS-1$
                    "root.n4a.n4c.n4e", //$NON-NLS-1$
                    "root.n4a.n4c.n4e.n4f" }, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });

        // DOC 5
        Table doc5 = RealMetadataFactory.createXmlDocument("doc5", vm1, doc5()); //$NON-NLS-1$
        RealMetadataFactory.createElements(
                doc5,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.nodea", //$NON-NLS-1$
                    "root.nodea.nodeb", //$NON-NLS-1$
                    "root.nodea.nodec", //$NON-NLS-1$
                    "root.nodea.nodec.noded", //$NON-NLS-1$
                    "root.nodea.nodec.nodee", //$NON-NLS-1$
                    "root.nodea.nodec.nodee.nodef", //$NON-NLS-1$
                    "root.nodea.nodec.nodee.nodeg", //$NON-NLS-1$
                    "root.nodea.nodec.nodee.nodeg.nodeh", //$NON-NLS-1$
                    "root.nodea.nodec.nodee.nodeg.nodeI", //$NON-NLS-1$
                    "root.nodea.nodec.nodee.nodeg.nodeI.nodeJ" }, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });

        // DOC 6
        Table doc6 = RealMetadataFactory.createXmlDocument("doc6", vm1, doc6()); //$NON-NLS-1$
        RealMetadataFactory.createElements(
                doc6,
                new String[] { "tempGroupTest" }, //$NON-NLS-1$
                new String[] { DataTypeManager.DefaultDataTypes.STRING });

        // DOC with excluded fragment
        Table docWithExcluded = RealMetadataFactory.createXmlDocument(
                "vm1.docWithExcluded", //$NON-NLS-1$
                vm1,
                docWithExcluded());
        RealMetadataFactory.createElements(
                docWithExcluded,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.n1", //$NON-NLS-1$
                    "root.n1.m1", //$NON-NLS-1$
                    "root.n1.m1.n2", //$NON-NLS-1$
                    "root.n1.m1.n2.leaf1" }, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });

        // DOC 2 with excluded fragment
        Table doc2WithExcluded = RealMetadataFactory.createXmlDocument(
                "vm1.docWithExcluded2", //$NON-NLS-1$
                vm1,
                docWithExcluded2());
        RealMetadataFactory.createElements(
                doc2WithExcluded,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.n1", //$NON-NLS-1$
                    "root.n1.m1", //$NON-NLS-1$
                    "root.n1.m2", //$NON-NLS-1$
                    "root.n1.m3" }, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });


        // DOC with attribute
        Table docWithAttribute = RealMetadataFactory.createXmlDocument(
                "vm1.docWithAttribute", //$NON-NLS-1$
                vm1,
                docTestConvertCriteriaWithAttribute());
        Table docWithAttribute3 = RealMetadataFactory.createXmlDocument(
                "vm1.docWithAttribute3", //$NON-NLS-1$
                vm1,
                docTestCriteriaWithAttribute());
        RealMetadataFactory.createElements(
               docWithAttribute,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.myElement", //$NON-NLS-1$
                    "root.@myAttribute"}, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });
        // DOC with attribute2
        Table docWithAttribute2 = RealMetadataFactory.createXmlDocument(
                "vm1.docWithAttribute2", //$NON-NLS-1$
                vm1,
                docTestConvertCriteriaWithAttribute2());
        RealMetadataFactory.createElements(
                docWithAttribute2,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.myElement", //$NON-NLS-1$
                    "root.@myAttribute"}, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });
        
        RealMetadataFactory.createElements(
                docWithAttribute3,
                new String[] {
                    "root", //$NON-NLS-1$
                    "root.myElement", //$NON-NLS-1$
                    "root.@type"}, //$NON-NLS-1$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.STRING });
        
        // Create the facade from the store
        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example1");
    }

    private static MappingDocument doc1() {
        // DOC 1
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement node= doc.addChildElement(new MappingElement("a0")); //$NON-NLS-1$
        
        MappingElement sourceNode = node.addChildElement(new MappingElement("a1")); //$NON-NLS-1$
        sourceNode.setSource("vm1.g1"); //$NON-NLS-1$
        
        sourceNode.addStagingTable("tm1.g1"); //$NON-NLS-1$

        sourceNode.addChildElement(new MappingElement("a1", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        sourceNode.addChildElement(new MappingElement("b1", "vm1.g1.e2")); //$NON-NLS-1$ //$NON-NLS-2$
        sourceNode.addChildElement(new MappingElement("c1", "vm1.g1.e3")); //$NON-NLS-1$ //$NON-NLS-2$
        return doc;
    }

    private static MappingDocument doc2() {
        MappingDocument doc = new MappingDocument(false);
        MappingElement A1 = doc.addChildElement(new MappingElement("a1", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        A1.setSource("vm1.g1"); //$NON-NLS-1$
        return doc;
    }

    private static MappingDocument docTestConvertCriteriaWithAttribute() {
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        root.setSource("vm1.g1"); //$NON-NLS-1$

        root.addChildElement(new MappingElement("myElement", "vm1.g1.e2")); //$NON-NLS-1$ //$NON-NLS-2$
        root.addAttribute(new MappingAttribute("myAttribute", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        return doc;
    }

    private static MappingDocument docTestConvertCriteriaWithAttribute2() {
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        root.setSource("vm1.g1"); //$NON-NLS-1$
        root.addAttribute(new MappingAttribute("myAttribute", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingSequenceNode seq = root.addSequenceNode(new MappingSequenceNode());
        seq.addChildElement(new MappingElement("myElement", "vm1.g1.e2")); //$NON-NLS-1$ //$NON-NLS-2$
        return doc;
    }    

    /*
     * Note: this should fail when validator starts verifying that vm1.g1 cannot
     * be executed before the temp group tm1.g1 is, since it selects from that 
     * group but is not in it's scope 
     */
    private static MappingDocument doc3() {
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        MappingElement n1 = root.addChildElement(new MappingElement("n1")); //$NON-NLS-1$
        n1.setSource("vm1.g1"); //$NON-NLS-1$
        
        MappingElement m1 = n1.addChildElement(new MappingElement("m1")); //$NON-NLS-1$
        
        MappingElement n2 = m1.addChildElement(new MappingElement("n2")); //$NON-NLS-1$
        n2.setSource("vm1.g2"); //$NON-NLS-1$
        n2.addStagingTable("tm1.g1"); //$NON-NLS-1$

        n2.addChildElement(new MappingElement("leaf1", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        return doc;
    }

    private static MappingDocument doc4() {
        
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        MappingElement n4a = root.addChildElement(new MappingElement("n4a")); //$NON-NLS-1$
        n4a.setMaxOccurrs(-1);
        n4a.setSource("vm1.g1"); //$NON-NLS-1$
        n4a.addStagingTable("tm1.g1"); //$NON-NLS-1$
        n4a.addChildElement(new MappingElement("n4b", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingElement n4c = n4a.addChildElement(new MappingElement("n4c")); //$NON-NLS-1$
        n4c.setMaxOccurrs(-1);
        n4c.setSource("vm1.g2");         //$NON-NLS-1$

        //this node can't be used for anything but searching through mapping doc for this property
        MappingElement fake = n4a.addChildElement(new MappingElement("fake")); //$NON-NLS-1$
        fake.setSource("fakeResultSet"); //$NON-NLS-1$
        
        n4c.addChildElement(new MappingElement("n4d", "vm1.g2.e1")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingElement n4e = n4c.addChildElement(new MappingElement("n4e")); //$NON-NLS-1$
        n4e.setSource("vm1.g3"); //$NON-NLS-1$
        n4e.setMaxOccurrs(-1);
        n4e.addChildElement(new MappingElement("n4f", "vm1.g3.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        return doc;
    }

    private static MappingDocument doc5() {
        
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        
        MappingElement nodea = root.addChildElement(new MappingElement("nodea")); //$NON-NLS-1$
        nodea.setSource("vm1.g1"); //$NON-NLS-1$
        nodea.addStagingTable("tm1.g1"); //$NON-NLS-1$
        nodea.setMaxOccurrs(-1);
        
        nodea.addChildElement(new MappingElement("nodeb", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingElement nodec = nodea.addChildElement(new MappingElement("nodec")); //$NON-NLS-1$
        nodec.setMaxOccurrs(-1);
        nodec.setSource("vm1.g2"); //$NON-NLS-1$

        nodec.addChildElement(new MappingElement("noded", "vm1.g2.e1")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingElement nodee = nodec.addChildElement(new MappingElement("nodee")); //$NON-NLS-1$
        nodee.setSource("vm1.g3"); //$NON-NLS-1$
        nodee.setMaxOccurrs(-1);

        nodee.addChildElement(new MappingElement("nodef", "vm1.g3.e1")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingElement nodeg = nodee.addChildElement(new MappingElement("nodeg")); //$NON-NLS-1$
        nodeg.setSource("vm1.g4"); //$NON-NLS-1$
        nodeg.setMaxOccurrs(-1);
        
        nodeg.addChildElement(new MappingElement("nodeh", "vm1.g4.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        MappingElement nodeI = nodeg.addChildElement(new MappingElement("nodeI")); //$NON-NLS-1$
        nodeI.setMaxOccurrs(-1);
        nodeI.setSource("vm1.g5"); //$NON-NLS-1$
        nodeI.addChildElement(new MappingElement("nodeJ", "vm1.g5.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        return doc;
    }

    private static MappingDocument doc6() {
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement simpleRoot = doc.addChildElement(new MappingElement("tempGroupTest")); //$NON-NLS-1$
        simpleRoot.setSource("vm1.g1"); //$NON-NLS-1$
        simpleRoot.addStagingTable("tm1.g1"); //$NON-NLS-1$
        return doc;
    }

    private static MappingDocument docWithExcluded() {
        
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        MappingElement n1 = root.addChildElement(new MappingElement("n1")); //$NON-NLS-1$
        n1.setSource("vm1.g1"); //$NON-NLS-1$

        MappingElement m1 = n1.addChildElement(new MappingElement("m1", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        MappingElement n2 = m1.addChildElement(new MappingElement("n2")); //$NON-NLS-1$
        n2.setSource("vm1.g2");         //$NON-NLS-1$
        n2.setExclude(true);
        n2.addChildElement(new MappingElement("leaf1", "vm1.g2.e2")); //$NON-NLS-1$ //$NON-NLS-2$        
        return doc;
    }

    private static MappingDocument docWithExcluded2() {
        
        MappingDocument doc = new MappingDocument(false);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        MappingElement n1 = root.addChildElement(new MappingElement("n1")); //$NON-NLS-1$
        n1.setSource("vm1.g1"); //$NON-NLS-1$
        
        n1.addChildElement(new MappingElement("m1", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        MappingElement m2 = n1.addChildElement(new MappingElement("m2", "vm1.g1.e2")); //$NON-NLS-1$ //$NON-NLS-2$
        m2.setExclude(true);

        n1.addChildElement(new MappingElement("m3", "vm1.g1.e3")); //$NON-NLS-1$ //$NON-NLS-2$

        return doc;
    }
        
    private static MappingDocument docTestCriteriaWithAttribute() {
        
        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        root.setSource("vm1.g1"); //$NON-NLS-1$
        root.addAttribute(new MappingAttribute("type", new Namespace("xsi", ""))); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        root.addAttribute(new MappingAttribute("type", "vm1.g1.e1")); //$NON-NLS-1$ //$NON-NLS-2$
        root.addChildElement(new MappingElement("myElement", "vm1.g1.e2")); //$NON-NLS-1$ //$NON-NLS-2$        
        return doc;
   }

    
    @Test public void test1() throws Exception {
        helpPlan("SELECT * FROM vm1.doc1", example1()); //$NON-NLS-1$
    }

    /**
     * Tests that a user cannot cite an invalid XML node in criteria
     * (a node that is not mapped to data)
     * (Also duplicate defect 8130)
     */
    @Test public void test1_defect7341() throws Exception {
        helpPlanException("SELECT * FROM vm1.doc1 WHERE a0 = '3'", example1()); //$NON-NLS-1$
    }

    /**
     * Tests that a user cannot cite an invalid XML node in criteria
     * (a node that is not mapped to data)
     * (Also duplicate defect 8130)
     */
    @Test public void test1_defect7341_a() throws Exception {
        helpPlanException(
            "SELECT * FROM vm1.doc3 WHERE context(m1, m1) = '3'", //$NON-NLS-1$
            example1());
    }

    @Test public void test2() throws Exception {
        helpPlan("SELECT * FROM vm1.doc2", example1()); //$NON-NLS-1$
    }

    @Test public void test3() throws Exception {
        helpPlan("SELECT * FROM vm1.doc1 where a0.a1.a1='x'", example1()); //$NON-NLS-1$
    }

    /**
     * Note: this should fail when validator starts verifying that vm1.g1 cannot
     * be executed before the temp group tm1.g1 is, since it selects from that 
     * group but is not in it's scope
     */
    @Test public void test4() throws Exception {
        helpPlan("SELECT * FROM vm1.doc3", example1()); //$NON-NLS-1$
    }

    @Test public void testTempGroupPlan() throws Exception {
        QueryMetadataInterface qmi = example1();
        
        XMLPlan plan = helpPlan("SELECT * FROM vm1.doc6", qmi); //$NON-NLS-1$
        
        Program program = plan.getOriginalProgram();

        int i = 0;
        assertTrue(program.getInstructionAt(i++) instanceof ExecStagingTableInstruction);
        assertTrue(program.getInstructionAt(i++) instanceof ExecSqlInstruction);
        assertTrue(program.getInstructionAt(i++) instanceof BlockInstruction);
        assertTrue(program.getInstructionAt(i++) instanceof MoveCursorInstruction);
        assertTrue(program.getInstructionAt(i++) instanceof WhileInstruction);
        assertTrue(program.getInstructionAt(i++) instanceof EndBlockInstruction);
        assertTrue(program.getInstructionAt(i++) instanceof ExecStagingTableInstruction);
    }

    @Test public void testPreparePlan() throws Exception {
        helpPlan(
            "SELECT * FROM vm1.doc1 ORDER BY vm1.doc1.a0.a1.c1", //$NON-NLS-1$
            example1());
    }

    @Test public void testPreparePlan2() throws Exception {
        helpPlan(
            "SELECT root.@myAttribute FROM vm1.docWithAttribute", //$NON-NLS-1$
            example1());
    }
    
    /**
     * This method takes in a Command object of the user's query and returns a XML plan
     * as a XMLNode object.
     * @param command The Command object for which query plan is to be returned
     * @param metadata The metadata needed for planning
     * @return The XML plan returned as an XMLPlan object
     * @throws QueryPlannerException
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    public static XMLPlan preparePlan(Command command, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, CommandContext context)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        IDGenerator idGenerator = new IDGenerator();
        idGenerator.setDefaultFactory(new IntegerIDFactory());
        AnalysisRecord analysis = new AnalysisRecord(false, DEBUG);
        try {
            if (DEBUG) {
                System.out.println("\n####################################\n" + command); //$NON-NLS-1$
            }
            return XMLPlanner.preparePlan(command, metadata, analysis, new XMLPlannerEnvironment(metadata), idGenerator, capFinder, context);
        } finally {
            if (DEBUG) {
                System.out.println(analysis.getDebugLog());
            }
        }
    }

    @Test public void testDefect18227() throws Exception {
        QueryMetadataInterface metadata = example1();       
        String sql = "select * from vm1.docWithAttribute3 where root.@type = '3'"; //$NON-NLS-1$
        
        Query query = (Query)TestXMLProcessor.helpGetCommand(sql, metadata);
        
        try {
            preparePlan(query, metadata, TestOptimizer.getGenericFinder(), new CommandContext());
            fail("Expected to get error about criteria against unmapped type attribute"); //$NON-NLS-1$
        } catch(QueryPlannerException e) {
            // Expect to get exception about criteria against xsi:type
        }
    }
    
    @Test public void testDefect21983() throws Exception {
        QueryMetadataInterface metadata = example1();       
        String sql = "select root.@type from vm1.docWithAttribute3"; //$NON-NLS-1$
        
        Query query = (Query)TestXMLProcessor.helpGetCommand(sql, metadata);

        //here's the test
        preparePlan(query, metadata, TestOptimizer.getGenericFinder(), new CommandContext());
    }
    
    /**
     * Test that if non-fully-qualified staging table name is used in user criteria, that fully-qualified
     * name is returned by XMLPlanner 
     * @throws Exception
     */
    @Test public void testRootStagingTableCase4308() throws Exception{
        
        String sql = "select * from vm1.doc1 where stagingTable2.e1 IN ('a', 'b', 'c')"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = exampleCase4308();
        
        Query query = (Query)new QueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(query, metadata);
        
        String expectedStagingTableResultSet = "vm1.doc1.stagingTable2"; //$NON-NLS-1$
        String actualStagingTableResultSet = CriteriaPlanner.getStagingTableForConjunct(query.getCriteria(), metadata);
        
        assertEquals(expectedStagingTableResultSet, actualStagingTableResultSet);
        
    }
    
    private TransformationMetadata exampleCase4308(){
    	MetadataStore metadataStore = new MetadataStore();
        
        // Create models
        Schema pm1 = RealMetadataFactory.createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema vm1 = RealMetadataFactory.createVirtualModel("vm1", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = RealMetadataFactory.createPhysicalGroup("g1", pm1); //$NON-NLS-1$

        // Create physical elements
        RealMetadataFactory.createElements(
                pm1g1,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });


        QueryNode stagingTableNode =
            new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ 
        Table stagingTable = RealMetadataFactory.createXmlStagingTable("doc1.stagingTable2", vm1, stagingTableNode); //$NON-NLS-1$
        
        RealMetadataFactory.createElements(
                stagingTable,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] {
                    DataTypeManager.DefaultDataTypes.STRING,
                    DataTypeManager.DefaultDataTypes.INTEGER,
                    DataTypeManager.DefaultDataTypes.BOOLEAN,
                    DataTypeManager.DefaultDataTypes.DOUBLE });

        MappingDocument doc = new MappingDocument(false);
        
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$
        
        root.addStagingTable("vm1.doc1.stagingTable2");

        // Create virtual documents
        // DOC 1
        RealMetadataFactory.createXmlDocument(
                "doc1", //$NON-NLS-1$
                vm1,
                doc);

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "case4308");
    }

    private static final boolean DEBUG = false;
}
