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

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;

@SuppressWarnings({"nls", "unchecked"})
public class TestSQLXMLProcessing {
	
	@Test public void testXmlElementTextContent() throws Exception {
		String sql = "SELECT xmlelement(foo, '<bar>', convert('<bar1/>', xml))"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<foo>&lt;bar&gt;<bar1/></foo>"),
        };    
    
        process(sql, expected);
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
    
        process(sql, expected);
	}
	
    @Test public void testXmlElement() throws Exception {
        String sql = "SELECT xmlelement(e1, e2) from pm1.g1 order by e1, e2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<e1>1</e1>"),
        		Arrays.asList("<e1>0</e1>"),
        		Arrays.asList("<e1>0</e1>"),
        		Arrays.asList("<e1>3</e1>"),
        		Arrays.asList("<e1>2</e1>"),
                Arrays.asList("<e1>1</e1>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlElementWithConcat() throws Exception {
        String sql = "SELECT xmlelement(e1, e2, xmlconcat(xmlelement(x), xmlelement(y, e3))) from pm1.g1 order by e1, e2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<e1>1<x></x><y>false</y></e1>"),
        		Arrays.asList("<e1>0<x></x><y>false</y></e1>"),
        		Arrays.asList("<e1>0<x></x><y>false</y></e1>"),
        		Arrays.asList("<e1>3<x></x><y>true</y></e1>"),
        		Arrays.asList("<e1>2<x></x><y>false</y></e1>"),
                Arrays.asList("<e1>1<x></x><y>true</y></e1>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlElementWithForest() throws Exception {
        String sql = "SELECT xmlelement(x, xmlforest(e1, e2, '1' as val)) from pm1.g1 order by e1, e2 limit 2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x><e2>1</e2><val>1</val></x>"), //note e1 is not present, because it's null
        		Arrays.asList("<x><e1>a</e1><e2>0</e2><val>1</val></x>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlElementWithAttributes() throws Exception {
        String sql = "SELECT xmlelement(x, xmlattributes(e1, e2, '1' as val)) from pm1.g1 order by e1, e2 limit 2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x e2=\"1\" val=\"1\"></x>"), //note e1 is not present, because it's null
        		Arrays.asList("<x e1=\"a\" e2=\"0\" val=\"1\"></x>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlElementWithPi() throws Exception {
        String sql = "SELECT xmlelement(x, xmlpi(name e1, '  1'))"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x><?e1 1?></x>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlElementWithNamespaces() throws Exception {
        String sql = "SELECT xmlelement(x, xmlnamespaces(no default, 'http://foo' as x, 'http://foo1' as y), xmlattributes(e1), e2) from pm1.g1 order by e1, e2 limit 2"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<x xmlns=\"\" xmlns:x=\"http://foo\" xmlns:y=\"http://foo1\">1</x>"), //note e1 is not present, because it's null
        		Arrays.asList("<x xmlns=\"\" xmlns:x=\"http://foo\" xmlns:y=\"http://foo1\" e1=\"a\">0</x>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlAgg() throws Exception {
        String sql = "SELECT xmlelement(parent, xmlAgg(xmlelement(x, xmlattributes(e1, e2)))) from pm1.g1"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<parent><x e1=\"a\" e2=\"0\"></x><x e2=\"1\"></x><x e1=\"a\" e2=\"3\"></x><x e1=\"c\" e2=\"1\"></x><x e1=\"b\" e2=\"2\"></x><x e1=\"a\" e2=\"0\"></x></parent>"), 
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlAggOrderBy() throws Exception {
        String sql = "SELECT xmlelement(parent, xmlAgg(xmlelement(x, xmlattributes(e1, e2)) order by e2)) from pm1.g1"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<parent><x e1=\"a\" e2=\"0\"></x><x e1=\"a\" e2=\"0\"></x><x e2=\"1\"></x><x e1=\"c\" e2=\"1\"></x><x e1=\"b\" e2=\"2\"></x><x e1=\"a\" e2=\"3\"></x></parent>"), 
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlSerialize() throws Exception {
    	String sql = "SELECT xmlserialize(document xmlelement(parent) as string)"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<parent></parent>"), 
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlTable() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string path '@x', val string path '/.') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList(null, "first"),
        		Arrays.asList("attr", "second"),
        };    
    
        process(sql, expected);
    }
    
	@Test public void testXmlTableDefaultAndParent() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a y=\"rev\"><b>first</b><b x=\"1\">second</b></a>', xml) columns x integer default -1 path '@x' , val string path '../@y') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList(-1, "rev"),
        		Arrays.asList(1, "rev"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlTableReturnXml() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"1\">second</b></a>', xml) columns val xml path '.') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<b>first</b>"),
        		Arrays.asList("<b x=\"1\">second</b>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlTableNoColumns() throws Exception {
        String sql = "select * from xmltable('/a' passing convert('<a><b>first</b><b x=\"1\">second</b></a>', xml)) as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<a><b>first</b><b x=\"1\">second</b></a>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlTablePassing() throws Exception {
        String sql = "select * from xmltable('<root>{for $x in $a/a/b return <c>{$x}</c>}</root>' passing convert('<a><b>first</b><b x=\"1\">second</b></a>', xml) as a columns x xml path 'c[1]/b') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<b>first</b>"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlTableForOrdinalityAndDefaultPath() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b><c>1</c></b><b>1</b><b><c>1</c></b><b>1</b></a>', xml) columns x for ordinality, c integer) as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList(1, 1),
        		Arrays.asList(2, null),
        		Arrays.asList(3, 1),
        		Arrays.asList(4, null),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlTableDescendantPath() throws Exception {
        String sql = "select * from xmltable('<a>{for $i in (1 to 5) return $i}</a>' columns x string path '//text()') as x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("1 2 3 4 5"),
        };    
    
        process(sql, expected);
    }

    @Test public void testXmlQuery() throws Exception {
        String sql = "select xmlquery('for $i in (1 to 5) return $i')"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("1 2 3 4 5"),
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlQueryEmptyNull() throws Exception {
    	String sql = "select xmlquery('/a' passing xmlparse(document '<x/>') null on empty)"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList((String)null)
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlNameEscaping() throws Exception {
    	String sql = "select xmlforest(\"xml\") from (select 1 as \"xml\") x"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<_u0078_ml>1</_u0078_ml>")
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlParseDoc() throws Exception {
    	String sql = "select xmlparse(document '<a/>')"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("<a/>")
        };    
    
        process(sql, expected);
    }
    
    @Test(expected=ExpressionEvaluationException.class) public void testXmlParseDocException() throws Exception {
    	String sql = "select xmlparse(document 'a<a/>')"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        };    
    
        process(sql, expected);
    }
    
    @Test public void testXmlParseContent() throws Exception {
    	String sql = "select xmlparse(content 'a<a/>')"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("a<a/>")
        };    
    
        process(sql, expected);
    }
    
    @Test(expected=ExpressionEvaluationException.class) public void testXmlParseContentException() throws Exception {
    	String sql = "select xmlparse(content 'a<')"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        };    
    
        process(sql, expected);
    }
    
    //by pass the validation
    @Test public void testXmlParseContentWellformed() throws Exception {
    	String sql = "select xmlparse(content 'a<' WELLFORMED)"; //$NON-NLS-1$
        
    	List<?>[] expected = new List<?>[] {
        		Arrays.asList("a<")
        };   
    
        process(sql, expected);
    }
    
	@Test public void testXmlParseClob() throws Exception {
    	String sql = "select xmlparse(document cast(? as clob)) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(ObjectConverterUtil.convertToString(new FileInputStream(UnitTestUtil.getTestDataFile("udf.xmi")))),
        };    
    
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(TestTextTable.clobFromFile("udf.xmi")));
    }
	
	@Test public void testXmlParseBlob() throws Exception {
    	String sql = "select xmlparse(document cast(? as blob)) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(ObjectConverterUtil.convertToString(new FileInputStream(UnitTestUtil.getTestDataFile("udf.xmi")))),
        };    
    
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(blobFromFile("udf.xmi")));
    }
	
	@Test public void testXmlParseBlobWithEncoding() throws Exception {
    	String sql = "select xmlparse(document cast(? as blob)) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(ObjectConverterUtil.convertToString(new InputStreamReader(new FileInputStream(UnitTestUtil.getTestDataFile("encoding.xml")), Charset.forName("ISO-8859-1")))),
        };    
    
        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(blobFromFile("encoding.xml")));
    }
	
    @Test public void testXmlTableTypes() throws Exception {
        String sql = "select * from xmltable('/a' passing xmlparse(document '<a>2000-01-01T01:01:00.2-06:00</a>') columns x timestamp path 'xs:dateTime(./text())', y timestamp path '.') as x"; //$NON-NLS-1$
        Timestamp ts = TimestampUtil.createTimestamp(100, 0, 1, 1, 1, 0, 200000000);
        List<?>[] expected = new List<?>[] {
        		Arrays.asList(ts, ts),
        };    
    
        process(sql, expected);
    }
    
	@Test public void testXmlTableSubquery() throws Exception {
		String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">c</b></a>', xml) columns x string path '@x', val string path '/.') as x where val = (select max(e1) from pm1.g1 as x)";
    	
        List[] expected = new List[] {
        		Arrays.asList("attr", "c"),
        };    

        process(sql, expected);
    }

    private static FakeDataManager dataManager = new FakeDataManager();
    
    @BeforeClass public static void oneTimeSetUp() {
    	TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
    	sampleData1(dataManager);
    }
    
    @AfterClass public static void oneTimeTearDown() {
    	TimestampWithTimezone.resetCalendar(null); //$NON-NLS-1$
    }
    
	private void process(String sql, List<?>[] expected) throws Exception {
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), createCommandContext());
        
        helpProcess(plan, createCommandContext(), dataManager, expected);
	}
	
	public static BlobType blobFromFile(final String file) {
		return new BlobType(new BlobImpl(new InputStreamFactory.FileInputStreamFactory(UnitTestUtil.getTestDataFile(file))));
	}

}
