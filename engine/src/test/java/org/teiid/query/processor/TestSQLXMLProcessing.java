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

package org.teiid.query.processor;

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.query.unittest.FakeMetadataFactory;

@SuppressWarnings({"nls", "unchecked"})
public class TestSQLXMLProcessing {
	
	@Test public void testXmlElementTextContent() throws Exception {
		String sql = "SELECT xmlelement(foo, '<bar>', convert('<bar1/>', xml))"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<foo>&lt;bar&gt;<bar1/></foo>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
	
	/**
	 * Repeat of the above test, but with a document declaration.  Because of the way we do event filtering, we end
	 * up with a slightly different, but equivalent answer.
	 */
	@Test public void testXmlElementTextContent1() throws Exception {
		String sql = "SELECT xmlelement(foo, '<bar>', convert('<?xml version=\"1.0\" encoding=\"UTF-8\"?><bar1/>', xml))"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<foo>&lt;bar&gt;<bar1></bar1></foo>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
	}
	
    @Test public void testXmlElement() {
        String sql = "SELECT xmlelement(e1, e2) from pm1.g1 order by e1, e2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<e1>1</e1>"),
        		Arrays.asList("<e1>0</e1>"),
        		Arrays.asList("<e1>0</e1>"),
        		Arrays.asList("<e1>3</e1>"),
        		Arrays.asList("<e1>2</e1>"),
                Arrays.asList("<e1>1</e1>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlElementWithConcat() {
        String sql = "SELECT xmlelement(e1, e2, xmlconcat(xmlelement(x), xmlelement(y, e3))) from pm1.g1 order by e1, e2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<e1>1<x></x><y>false</y></e1>"),
        		Arrays.asList("<e1>0<x></x><y>false</y></e1>"),
        		Arrays.asList("<e1>0<x></x><y>false</y></e1>"),
        		Arrays.asList("<e1>3<x></x><y>true</y></e1>"),
        		Arrays.asList("<e1>2<x></x><y>false</y></e1>"),
                Arrays.asList("<e1>1<x></x><y>true</y></e1>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlElementWithForest() {
        String sql = "SELECT xmlelement(x, xmlforest(e1, e2, '1' as val)) from pm1.g1 order by e1, e2 limit 2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x><e2>1</e2><val>1</val></x>"), //note e1 is not present, because it's null
        		Arrays.asList("<x><e1>a</e1><e2>0</e2><val>1</val></x>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlElementWithAttributes() {
        String sql = "SELECT xmlelement(x, xmlattributes(e1, e2, '1' as val)) from pm1.g1 order by e1, e2 limit 2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x e2=\"1\" val=\"1\"></x>"), //note e1 is not present, because it's null
        		Arrays.asList("<x e1=\"a\" e2=\"0\" val=\"1\"></x>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlElementWithPi() {
        String sql = "SELECT xmlelement(x, xmlpi(name e1, '  1'))"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x><?e1 1?></x>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlElementWithNamespaces() {
        String sql = "SELECT xmlelement(x, xmlnamespaces(no default, 'http://foo' as x, 'http://foo1' as y), xmlattributes(e1), e2) from pm1.g1 order by e1, e2 limit 2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x xmlns=\"\" xmlns:x=\"http://foo\" xmlns:y=\"http://foo1\">1</x>"), //note e1 is not present, because it's null
        		Arrays.asList("<x xmlns=\"\" xmlns:x=\"http://foo\" xmlns:y=\"http://foo1\" e1=\"a\">0</x>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlAgg() {
        String sql = "SELECT xmlelement(parent, xmlAgg(xmlelement(x, xmlattributes(e1, e2)))) from pm1.g1"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<parent><x e1=\"a\" e2=\"0\"></x><x e2=\"1\"></x><x e1=\"a\" e2=\"3\"></x><x e1=\"c\" e2=\"1\"></x><x e1=\"b\" e2=\"2\"></x><x e1=\"a\" e2=\"0\"></x></parent>"), 
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlAggOrderBy() {
        String sql = "SELECT xmlelement(parent, xmlAgg(xmlelement(x, xmlattributes(e1, e2)) order by e2)) from pm1.g1"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<parent><x e1=\"a\" e2=\"0\"></x><x e1=\"a\" e2=\"0\"></x><x e2=\"1\"></x><x e1=\"c\" e2=\"1\"></x><x e1=\"b\" e2=\"2\"></x><x e1=\"a\" e2=\"3\"></x></parent>"), 
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlSerialize() {
    	String sql = "SELECT xmlserialize(document xmlelement(parent) as string)"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<parent></parent>"), 
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlTable() {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string path '@x', val string path '/.') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList(null, "first"),
        		Arrays.asList("attr", "second"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
	@Test public void testXmlTableDefaultAndParent() {
        String sql = "select * from xmltable('/a/b' passing convert('<a y=\"rev\"><b>first</b><b x=\"1\">second</b></a>', xml) columns x integer path '@x' default -1, val string path '../@y') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList(-1, "rev"),
        		Arrays.asList(1, "rev"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlTableReturnXml() {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"1\">second</b></a>', xml) columns val xml path '.') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<b>first</b>"),
        		Arrays.asList("<b x=\"1\">second</b>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlTableNoColumns() {
        String sql = "select * from xmltable('/a' passing convert('<a><b>first</b><b x=\"1\">second</b></a>', xml)) as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<a><b>first</b><b x=\"1\">second</b></a>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testXmlTablePassing() {
        String sql = "select * from xmltable('<root>{for $x in $a/a/b return <c>{$x}</c>}</root>' passing convert('<a><b>first</b><b x=\"1\">second</b></a>', xml) as a columns x xml path 'c[1]/b') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<b>first</b>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Ignore
    @Test public void testXmlTableForOrdinality() {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b><c>1</c></b><b>1</b><b><c>1</c></b><b>1</b></a>', xml) as a columns x for ordinality, c integer path '.') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<b>first</b>"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), FakeMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }

}
