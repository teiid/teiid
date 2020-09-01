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

package org.teiid.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.teiid.query.processor.TestProcessor.createCommandContext;
import static org.teiid.query.processor.TestProcessor.doProcess;
import static org.teiid.query.processor.TestProcessor.helpGetPlan;
import static org.teiid.query.processor.TestProcessor.helpParse;
import static org.teiid.query.processor.TestProcessor.helpProcess;
import static org.teiid.query.processor.TestProcessor.processPreparedStatement;
import static org.teiid.query.processor.TestProcessor.sampleData1;
import static org.teiid.query.processor.TestProcessor.setParameterValues;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.stax.StAXSource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.ResultsMessage;
import org.teiid.client.util.ResultsFuture;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.process.TestDQPCore;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.relational.rules.TestCalculateCostUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;
import org.teiid.util.StAXSQLXML;

@SuppressWarnings({"nls", "unchecked"})
public class TestSQLXMLProcessing {

    @Test public void testXmlElementTextContent() throws Exception {
        String sql = "SELECT xmlelement(foo, '<bar>', convert('<bar1/>', xml))"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<foo>&lt;bar&gt;<bar1/></foo>"),
        };

        process(sql, expected);
    }

    @Test public void testXmlElementNamespaces() throws Exception {
        String sql = "SELECT xmlelement(NAME x, XMLNAMESPACES(no DEFAULT)"
                + ", 'a')"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<x xmlns=\"\">a</x>"),
        };

        process(sql, expected);

        sql = "SELECT xmlelement(NAME x, XMLNAMESPACES(DEFAULT 'http://abc')"
                + ", 'a')"; //$NON-NLS-1$

        expected = new List<?>[] {
                Arrays.asList("<x xmlns=\"http://abc\">a</x>"),
        };

        process(sql, expected);

        sql = "SELECT xmlelement(NAME x, XMLNAMESPACES('http://abc' as x, no default, 'http://def' as y)"
                + ", 'a')"; //$NON-NLS-1$

        expected = new List<?>[] {
                Arrays.asList("<x xmlns:x=\"http://abc\" xmlns=\"\" xmlns:y=\"http://def\">a</x>"),
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

    @Test(expected=QueryValidatorException.class) public void testXmlForestWithArray() throws Exception {
        String sql = "SELECT xmlforest((1,2) as val)"; //$NON-NLS-1$

        process(sql, null);
    }

    @Test public void testXmlForestWithBinary() throws Exception {
        String sql = "SELECT xmlforest(X'AB' as val)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<val>qw==</val>"),
        };

        process(sql, expected);
    }

    @Test public void testXmlForestWithBlob() throws Exception {
        String sql = "SELECT xmlforest(cast(X'AB' as blob) as val)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<val>qw==</val>"),
        };

        process(sql, expected);
    }

    @Test public void testXmlElementWithForestView() throws Exception {
        String sql = "SELECT xmlelement(x, xmlforest(x, y, '1' as val)) from (select e1 as x, e2 as y from pm1.g1) a order by x, y limit 2"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<x><y>1</y><val>1</val></x>"), //note e1 is not present, because it's null
                Arrays.asList("<x><x>a</x><y>0</y><val>1</val></x>"),
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

    @Test public void testXmlSerialize1() throws Exception {
        String sql = "SELECT xmlserialize(document xmlelement(parent) as string including xmldeclaration)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<?xml version=\"1.0\" encoding=\"UTF-8\"?><parent></parent>"),
        };

        process(sql, expected);
    }

    @Test public void testXmlSerialize2() throws Exception {
        String sql = "SELECT xmlserialize(document xmlelement(parent) as string version '1.2' including xmldeclaration)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<?xml version=\"1.2\" encoding=\"UTF-8\"?><parent></parent>"),
        };

        process(sql, expected);
    }

    @Test public void testXmlSerializeBinary() throws Exception {
        String sql = "SELECT xmlserialize(document xmlelement(parent) as varbinary version '1.2' including xmldeclaration)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(new BinaryType("<?xml version=\"1.2\" encoding=\"UTF-8\"?><parent></parent>".getBytes(Charset.forName("UTF-8")))),
        };

        process(sql, expected);
    }

    @Test public void testXmlSerializeBinary1() throws Exception {
        String sql = "SELECT xmlserialize(document xmlelement(parent) as varbinary encoding \"UTF-16\" version '1.2' including xmldeclaration)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(new BinaryType("<?xml version=\"1.2\" encoding=\"UTF-16\"?><parent></parent>".getBytes(Charset.forName("UTF-16")))),
        };

        process(sql, expected);
    }

    @Test public void testXmlSerializeBinary2() throws Exception {
        String sql = "SELECT cast(xmlserialize(document xmlelement(other) as blob encoding \"UTF-16\" version '1.2' including xmldeclaration) as varbinary)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(new BinaryType("<?xml version=\"1.2\" encoding=\"UTF-16\"?><other></other>".getBytes(Charset.forName("UTF-16")))),
        };

        process(sql, expected);
    }

    /**
     * if not specifically excluding, then leave the declaration intact (pre-8.2 behavior)
     */
    @Test public void testXmlSerialize3() throws Exception {
        String sql = "SELECT xmlserialize(document xmlparse(document '<?xml version=\"1.1\" encoding=\"UTF-8\"?><a></a>') as string)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<?xml version=\"1.1\" encoding=\"UTF-8\"?><a></a>"),
        };

        process(sql, expected);
    }

    @Test public void testXmlTableXmlArray() throws Exception {
        String doc = "'[\n" +
                "    {\n" +
                "        \"at\": \"ABC\",\n" +
                "        \"values\": [\n" +
                "            [\n" +
                "                \"v1-value1\",\n" +
                "                \"v1-value2\",\n" +
                "                \"v1-value3\"\n" +
                "            ],\n" +
                "             [\n" +
                "                \"v2-value1\",\n" +
                "                \"v2-value2\",\n" +
                "             ]\n" +
                "        ]\n" +
                "    }\n" +
                "]'";
        String sql = "SELECT A.\"values\"[2] FROM XMLTABLE('/response/response' PASSING JSONTOXML('response', "+doc+") COLUMNS at string PATH 'at/text()', \"values\" xml[] PATH 'values') AS A;"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<values><values>v2-value1</values><values>v2-value2</values></values>"),
        };

        process(sql, expected);
    }


    @Test public void testXmlTable() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string path '@x', val string path '.') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(null, "first"),
                Arrays.asList("attr", "second"),
        };

        process(sql, expected);
    }

    @Test public void testXmlTableWithPeriodAlias() throws Exception {
        String sql = "select \"x.b\".* from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string path '@x', val string path '.') as \"x.b\""; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(null, "first"),
                Arrays.asList("attr", "second"),
        };

        process(sql, expected);
    }

    @Test(expected=QueryParserException.class) public void testXmlTableWithComplexIdentifier() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns \"x.y\" string path '@x', val string path '/.') as x"; //$NON-NLS-1$

        QueryParser.getQueryParser().parseCommand(sql);
    }

    @Test public void testXmlTableNull() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert(null, xml) columns x string path '@x', val string path '/.') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
        };

        process(sql, expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testXmlTableSequence() throws Exception {
        String sql = "select * from xmltable('/a' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string path 'b') as x"; //$NON-NLS-1$

        process(sql, null);
    }

    @Test public void testXmlTableSequenceArray() throws Exception {
        String sql = "select * from xmltable('/a' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string[] path 'b') as x"; //$NON-NLS-1$


        List<?>[] expected = new List<?>[] {
                Arrays.asList(new ArrayImpl("first", "second")),
        };
        process(sql, expected);
    }

    @Test public void testXmlTableSequenceArraySingle() throws Exception {
        String sql = "select * from xmltable('/a' passing convert('<a><b>first</b></a>', xml) columns x string[] path 'b') as x"; //$NON-NLS-1$


        List<?>[] expected = new List<?>[] {
                Arrays.asList(new ArrayImpl("first")),
        };
        process(sql, expected);
    }

    /**
     * TODO: we should allow explicit null on empty / empty on empty behavior with array values
     * @throws Exception
     */
    @Test public void testXmlTableSequenceArrayEmpty() throws Exception {
        String sql = "select * from xmltable('/a' passing convert('<a></a>', xml) columns x string[] path 'b') as x"; //$NON-NLS-1$


        List<?>[] expected = new List<?>[] {
                Collections.singletonList(null)
        };
        process(sql, expected);
    }

    @Test public void testXmlTableBinary() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><b xsi:type=\"xs:hexBinary\">0FAB</b><b>1F1C</b></a>', xml) columns val varbinary path '.') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(new BinaryType(new byte[] {0xf, (byte)0xab})),
                Arrays.asList(new BinaryType(new byte[] {0x1F, 0x1C})),
        };

        process(sql, expected);
    }

    @Test public void testXmlTableBOM() throws Exception {
        String sql = "select * from xmltable('/a' passing xmlparse(document X'EFBBBF" + PropertiesUtils.toHex("<a/>".getBytes("UTF-8")) + "' wellformed)) as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<a/>"),
        };

        process(sql, expected);
    }

    @Test public void testXsiNil() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><b xsi:nil=\"true\" xsi:type=\"xs:int\"/><b>1</b></a>', xml) columns val integer path '.') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Collections.singletonList(null),
                Arrays.asList(1),
        };

        process(sql, expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testXmlTableAsynchError() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x blob path '@x', val string path '/.') as x"; //$NON-NLS-1$

        process(sql, null);
    }

    @Test public void testXmlTableNumeric() throws Exception {
        String sql = "select * from xmltable('/a' passing convert('<a s=\"1\" d=\"2.0\" l=\"12345678901\"/>', xml) columns x short path '@s', x1 double path '@d', z long path '@l') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList((short)1, 2.0, 12345678901L)
        };

        process(sql, expected);
    }

    @Test public void testXmlTableDateTime() throws Exception {
        String sql = "select * from xmltable('/a' passing convert('<a dt=\"0001-11-17T07:38:49\" dtz=\"2011-11-17T07:38:49Z\" t=\"13:23:14\" d=\"2010-04-05\" />', xml) columns x timestamp path '@dt', x1 timestamp path '@dtz', y date path '@d', z time path '@t') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(TimestampUtil.createTimestamp(-1899, 10, 19, 7, 38, 49, 0), TimestampUtil.createTimestamp(111, 10, 17, 1, 38, 49, 0), TimestampUtil.createDate(110, 3, 5), TimestampUtil.createTime(13, 23, 14))
        };

        process(sql, expected);
    }

    @Test public void testXmlTableDateTimeInDST() throws Exception {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("PST")); //$NON-NLS-1$
        try {
            String sql = "select * from xmltable('/a' passing convert('<a dt=\"2011-11-01T09:38:49\" dtz=\"2011-11-01T07:38:49Z\"/>', xml) columns x timestamp path '@dt', x1 timestamp path '@dtz') as x"; //$NON-NLS-1$

            List<?>[] expected = new List<?>[] {
                    Arrays.asList(TimestampUtil.createTimestamp(111, 10, 1, 9, 38, 49, 0), TimestampUtil.createTimestamp(111, 10, 1, 0, 38, 49, 0))
            };

            process(sql, expected);
        } finally {
            TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
        }
    }

    @Test public void testXmlTablePassingSubquery() throws Exception {
        String sql = "select * from xmltable('/a/b' passing (SELECT xmlelement(name a, xmlAgg(xmlelement(name b, e1))) from pm1.g1) columns val string path '.') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("a"),
                Arrays.asList(""),
                Arrays.asList("a"),
                Arrays.asList("c"),
                Arrays.asList("b"),
                Arrays.asList("a")
        };

        process(sql, expected);
    }

    @Test public void testXmlTableInView() throws Exception {
        String sql = "select * from g1"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(null, "first"),
                Arrays.asList("attr", "second"),
        };

        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema vm1 = RealMetadataFactory.createVirtualModel("vm1", metadataStore);  //$NON-NLS-1$

        QueryNode vm1g1n1 = new QueryNode("select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string path '@x', val string path '.') as x"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = RealMetadataFactory.createVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        RealMetadataFactory.createElements(vm1g1,
                                    new String[] { "x", "val" }, //$NON-NLS-1$
                                    new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        // Create the facade from the store
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "example");

        ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(), createCommandContext());

        helpProcess(plan, createCommandContext(), dataManager, expected);

        plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(), createCommandContext());

        doProcess(plan, dataManager, expected, createCommandContext());
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
        String sql = "select * from xmltable('/*:a/*:b' passing convert('<a xmlns=\"http:foo\"><b>first</b><b xmlns=\"\" x=\"1\">second</b></a>', xml) columns val xml path '.') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<b xmlns=\"http:foo\">first</b>"),
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

        ProcessorPlan plan = process(sql, expected);
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, ((Expression)plan.getOutputElements().get(0)).getType());
    }

    @Test public void testXmlTableDescendantPath() throws Exception {
        String sql = "select * from xmltable('<a>{for $i in (1 to 5) return $i}</a>' columns x string path '//text()') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("1 2 3 4 5"),
        };

        process(sql, expected, true);
    }

    @Test public void testXmlQuery() throws Exception {
        String sql = "select xmlquery('for $i in (1 to 5) return $i')"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("1 2 3 4 5"),
        };

        process(sql, expected);
    }

    @Test public void testXmlExists() throws Exception {
        String sql = "select xmlexists('for $i in (1 to 1) return $i'), xmlexists('for $i in (1 to 0) return $i')"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(true, false),
        };

        process(sql, expected);
    }

    @Test public void testXmlQueryNull() throws Exception {
        String sql = "select xmlquery('/a' passing cast(null as xml))"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Collections.singletonList(null),
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

    @Test public void testXmlQueryEmptyNullString() throws Exception {
        String sql = "select xmlquery('/a/b' passing xmlparse(document '<x/>') null on empty)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList((String)null)
        };

        process(sql, expected);
    }

    @Test public void testXmlQueryStreaming() throws Exception {
        String sql = "select xmlquery('/a/b' passing xmlparse(document '<a><b x=''1''/><b x=''2''/></a>') null on empty)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<b x=\"1\"/><b x=\"2\"/>")
        };

        process(sql, expected);
    }

    @Test public void testXmlTableStreamingTiming() throws Throwable {
        String sql = "select xmlserialize(x.object_value as string), y.x from xmltable('/a/b' passing xmlparse(document '<a><b x=''1''/><b x=''2''/></a>')) as x, (select 1 as x) as y"; //$NON-NLS-1$

        final List<?>[] expected = new List<?>[] {
                Arrays.asList("<b x=\"1\"/>", 1),
                Arrays.asList("<b x=\"2\"/>", 1)
        };

        executeStreaming(sql, expected, -1);
    }

    @Test public void testXmlTableStreamingMultibatch() throws Throwable {
        String sql = "select t.* from (select xmlelement(a, xmlagg(xmlelement(b, e1))) doc from pm1.g1) as x, xmltable('/a/b' passing doc columns x string path '.') as t"; //$NON-NLS-1$

        final List<?>[] expected = new List<?>[] {
                Arrays.asList("a"),
                Arrays.asList(""),
                Arrays.asList("a"),
                Arrays.asList("c"),
                Arrays.asList("b"),
                Arrays.asList("a")
        };

        dataManager.setBlockOnce();
        executeStreaming(sql, expected, 2);
    }

    @Test(expected=TeiidProcessingException.class) public void testXmlTableStreamingTimingWithError() throws Throwable {
        String sql = "select x.x, y.x from xmltable('/a/b' passing xmlparse(document '<a><b x=''1''/><b x=''2''/></a>') columns x integer path '1 div (@x - 1)') as x, (select 1 as x) as y"; //$NON-NLS-1$

        final List<?>[] expected = new List<?>[] {
                Arrays.asList(1, 1),
                Arrays.asList(2, 1)
        };

        executeStreaming(sql, expected, -1);
    }

    private void executeStreaming(String sql, final List<?>[] expected, int batchSize)
            throws Throwable {
        final CommandContext cc = createCommandContext();
        if (batchSize != -1) {
            cc.setBufferManager(BufferManagerFactory.getTestBufferManager(0, 1));
        }
        final ResultsFuture<Runnable> r = new ResultsFuture<Runnable>();
        Executor ex = new Executor() {

            @Override
            public void execute(Runnable command) {
                r.getResultsReceiver().receiveResults(command);
            }
        };
        cc.setExecutor(ex);
        final ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), cc);
        final ResultsFuture<Void> result = new ResultsFuture<Void>();
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    doProcess(plan, dataManager, expected, cc);
                    result.getResultsReceiver().receiveResults(null);
                } catch (Throwable e) {
                    result.getResultsReceiver().exceptionOccurred(e);
                }
            }
        };
        t.start();
        Runnable runnable = r.get();
        runnable.run();
        try {
            result.get();
        } catch (ExecutionException e) {
            if (e.getCause() != null) {
                throw e.getCause();
            }
            throw e;
        }
    }

    @Test public void testXmlNameEscaping() throws Exception {
        String sql = "select xmlforest(\"xml\") from (select 1 as \"xml\") x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<_x0078_ml>1</_x0078_ml>")
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

    @Test public void testXmlParseBOM() throws Exception {
        String sql = "select xmlparse(content X'EFBBBF" + PropertiesUtils.toHex("<a/>".getBytes("UTF-8")) + "')"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("﻿<a/>")
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

    @Test public void testXmlTableStreamingParentAttributes() throws Exception {
        String sql = "select * from xmltable('/a/b' passing xmlparse(document '<a x=''1''><b>foo</b></a>') columns y string path '.', x integer path '../@x') as x"; //$NON-NLS-1$
        List<?>[] expected = new List<?>[] {
                Arrays.asList("foo", 1),
        };
        process(sql, expected);
    }

    /**
     * Highlights that the PathMapFilter needs to be selective in calling startContent
     * @throws Exception
     */
    @Test public void testXmlStreamingError() throws Exception {
        String sql = "select * from xmltable('/a/a' passing xmlparse(document '<a><a>2000-01-01T01:01:00.2-06:00<a></a></a></a>') columns x timestamp path 'xs:dateTime(./text())') as x"; //$NON-NLS-1$
        Timestamp ts = TimestampUtil.createTimestamp(100, 0, 1, 1, 1, 0, 200000000);
        List<?>[] expected = new List<?>[] {
                Arrays.asList(ts),
        };
        process(sql, expected);
    }

    @Test public void testXmlTableSubquery() throws Exception {
        String sql = "select * from xmltable('/a/b' passing convert('<a><b>first</b><b x=\"attr\">c</b></a>', xml) columns x string path '@x', val string path '.') as x where val = (select max(e1) from pm1.g1 as x)";

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

    private ProcessorPlan process(String sql, List<?>[] expected) throws Exception {
        return process(sql, expected, false);
    }

    private ProcessorPlan process(String sql, List<?>[] expected, boolean relativeXPath) throws Exception {
        CommandContext cc = createCommandContext();
        cc.getOptions().relativeXPath(relativeXPath);
        CommandContext.pushThreadLocalContext(cc);
        try {
            ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), cc);
            helpProcess(plan, cc, dataManager, expected);
            return plan;
        } finally {
            CommandContext.popThreadLocalContext();
        }
    }

    public static BlobType blobFromFile(final String file) {
        return new BlobType(new BlobImpl(new InputStreamFactory.FileInputStreamFactory(UnitTestUtil.getTestDataFile(file))));
    }

    @Test public void testXmlTableWithDefault() throws Exception {
        String sql = "select * from xmltable(XMLNAMESPACES(default 'http://x.y.com'), '/a/b' passing convert('<a xmlns=\"http://x.y.com\"><b>first</b><b x=\"attr\">second</b></a>', xml) columns x string path '@x', val string path '.') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(null, "first"),
                Arrays.asList("attr", "second"),
        };

        process(sql, expected);
    }

    @Test public void testXmlAggNested() throws Exception {
        String sql = "SELECT XMLELEMENT(NAME metadata, XMLFOREST(e1), (SELECT XMLAGG(XMLELEMENT(NAME subTypes, XMLFOREST(e1))) FROM pm1.g2 AS b WHERE b.e1 = a.e1)) FROM pm1.g1 AS a where e1 = 'a' GROUP BY e1"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("<metadata><e1>a</e1><subTypes><e1>a</e1></subTypes><subTypes><e1>a</e1></subTypes><subTypes><e1>a</e1></subTypes></metadata>"),
        };

        process(sql, expected);
    }

    @Test public void testJsonStreamingXmlTable() throws Exception {
        String sql = "select * from xmltable('/Person/phoneNumber' passing jsontoxml('Person', cast(? as blob)) columns x string path 'type', y string path 'number') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("home", "212 555-1234"),
                Arrays.asList("fax", "646 555-4567"),
        };

        Blob b = BlobType.createBlob(("{ \"firstName\": \"John\", \"lastName\": \"Smith\", \"age\": 25, \"address\": { \"streetAddress\": \"21 2nd Street\", \"city\": \"New York\", \"state\": \"NY\", "+
        "\"postalCode\": \"10021\" }, \"phoneNumber\": [ { \"type\": \"home\", \"number\": \"212 555-1234\" }, { \"type\": \"fax\", \"number\": \"646 555-4567\" } ] }").getBytes(Charset.forName("UTF-8")));

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(b));
    }

    @Test public void testLargeNumeric() throws Exception {
        String sql = "select * from xmltable('/num' passing jsontoxml('num', '{\"a\":12345678901234567890, \"b\":12345678901234567890.0}') columns x string path 'a', y string path 'b') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("12345678901234567890", "12345678901234567890.0"),
        };

        Blob b = BlobType.createBlob(("{ \"firstName\": \"John\", \"lastName\": \"Smith\", \"age\": 25, \"address\": { \"streetAddress\": \"21 2nd Street\", \"city\": \"New York\", \"state\": \"NY\", "+
        "\"postalCode\": \"10021\" }, \"phoneNumber\": [ { \"type\": \"home\", \"number\": \"212 555-1234\" }, { \"type\": \"fax\", \"number\": \"646 555-4567\" } ] }").getBytes(Charset.forName("UTF-8")));

        processPreparedStatement(sql, expected, dataManager, new DefaultCapabilitiesFinder(), RealMetadataFactory.example1Cached(), Arrays.asList(b));
    }

    @Test public void testStaxComment() throws Exception {
        String sql = "select * from xmltable('/*:Person/phoneNumber' passing cast(? as xml) columns x string path 'type', y string path 'number') as x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList(null, "8881112222"),
        };

        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLEventReader reader = factory.createXMLEventReader(new StringReader("<Person><!--hello--><phoneNumber><number>8881112222</number></phoneNumber></Person>"));
        XMLType value = new XMLType(new StAXSQLXML(new StAXSource(reader)));
        value.setType(Type.DOCUMENT);
        Command command = helpParse(sql);
        CommandContext context = createCommandContext();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        context.setMetadata(metadata);
        CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder();
        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder, context);
        setParameterValues(Arrays.asList(value), command, context);
        doProcess(plan, dataManager, expected, context);
    }

    @Test(expected=ExpressionEvaluationException.class) public void testExternalEntityResolving() throws Exception {
        String sql = "SELECT xmlelement(foo, '<bar>', convert('<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><bar1>&xxe;</bar1>', xml))"; //$NON-NLS-1$

        process(sql, null);
    }

    @Test public void testXmlText() throws Exception {
        String sql = "SELECT xmlserialize(xmltext('foo&bar') as string)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Arrays.asList("foo&amp;bar"),
        };

        process(sql, expected);
    }

    @Test(expected=TeiidProcessingException.class) public void testInvalidXmlComment() throws Exception {
        String sql = "SELECT xmlcomment('--')"; //$NON-NLS-1$

        process(sql, null);
    }

    @Test public void testXmlCast() throws Exception {
        String sql = "select xmlcast(xmlquery('/a/b' passing convert('<a><b>1</b></a>', xml)) as integer)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Collections.singletonList(1),
        };

        assertEquals("SELECT XMLCAST(XMLQUERY('/a/b' PASSING convert('<a><b>1</b></a>', xml)) AS integer)", TestProcessor.helpParse(sql).toString());

        process(sql, expected);
    }

    @Test public void testXmlCast1() throws Exception {
        String sql = "select xmlcast(cast('2000-01-01 00:00:00' as timestamp) as xml)"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Collections.singletonList("2000-01-01T06:00:00Z"),
        };

        process(sql, expected);
    }

    @Test public void testAtomizedPredicate() throws Exception {
        String sql = "Select valOption2 From\n" +
                "    (select '<root>\n" +
                "        <item>\n" +
                "            <id>id1</id>\n" +
                "            <val>val1</val>\n" +
                "        </item>\n" +
                "    </root>' as resp) w,\n" +
                "    XMLTABLE(\n" +
                "        '/root' passing XMLPARSE(document w.resp) columns\n" +
                "        valOption2 string PATH 'item[id = \"id1\"]/val'\n" +
                "    ) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
                Collections.singletonList("val1"),
        };

        process(sql, expected);
    }

    @Test public void testTextPredicate() throws Exception {
        String sql = "Select valOption2 From\n" +
                "    (select '<root>\n" +
                "        <item>\n" +
                "            <id>id1</id>\n" +
                "            <val>val1</val>\n" +
                "        </item>\n" +
                "    </root>' as resp) w,\n" +
                "    XMLTABLE(\n" +
                "        '/root' passing XMLPARSE(document w.resp) columns\n" +
                "        valOption2 string PATH 'item[id/text() = \"id1\"]/val'\n" +
                "    ) x"; //$NON-NLS-1$

        List<?>[] expected = new List<?>[] {
            Collections.singletonList("val1"),
        };

        process(sql, expected);
    }

    @Test public void testNotNullParameterValidation() throws Exception {
        String sql = "call v0(\n" +
                "    soapBody => XmlElement(root)\n" +
                ")";

        String ddl = "create virtual procedure v0(IN soapBody xml not null)\n" +
                "             returns (response xml) as\n" +
                "          begin\n" +
                "             select XmlElement(root, 'Hi!');\n" +
                "          end";

        List<?>[] expected = new List[] {Arrays.asList("<root>Hi!</root>")};

        CommandContext cc = createCommandContext();
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.fromDDL(ddl, "x", "y"), new DefaultCapabilitiesFinder(), cc);
        helpProcess(plan, cc, dataManager, expected);
    }

    @Test public void testPathSubtree() throws Exception {
        String sql = "Select * From XmlTable ('/root' Passing cast('<root><def><test1>10</test1><test1>20</test1></def><abc>22</abc></root>' as xml) \n" +
                "Columns b string Path 'def')xx";

        List<?>[] expected = new List<?>[] {
            Arrays.asList("1020"),
        };

        process(sql, expected);
    }

    @Test public void testRootFunction() throws Exception {
        String xml = "'<root xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" type=\"decimal\">\n" +
                "        <waitSeconds xsi:type=\"decimal\">13</waitSeconds>\n" +
                "         </root>'";

        String sqlPrefix = "Select *\n" +
                        "        From \n" +
                        "            XmlTable(\n" +
                        "                '/root' \n" +
                        "                PASSING convert(" + xml +
                        "                , xml) Columns \n";

        String sql = sqlPrefix + " waitSeconds integer Path 'waitSeconds[@*:type=root()/root/@type]'\n)a";

        List<?>[] expected = new List<?>[] {
            Arrays.asList(13),
        };

        process(sql, expected);

        sql = sqlPrefix + " waitSeconds integer Path 'root()/root/waitSeconds'\n)a";

        process(sql, expected);
    }

    @Test public void testJustRoot() throws Exception {
        String sql = "Select * From XmlTable (\n" +
                "        '/root/abc'\n" +
                "        Passing convert('<root><def><test1>10</test1><test1>20</test1></def><abc>22</abc></root>', xml)\n" +
                "        Columns\n" +
                "            b string Path 'root()'\n" +
                "    )xx;";

        List<?>[] expected = new List<?>[] {
            Arrays.asList("102022"),
        };

        process(sql, expected);
    }

    @Test public void testRootPath() throws Exception {
        String sql = "Select * From XmlTable (\n" +
                "        '/root'\n" +
                "        Passing convert('<root><def><test1>10</test1><test1>20</test1></def><abc>22</abc></root>', xml)\n" +
                "        Columns\n" +
                "            b integer Path '//root',\n" +
                "            c string Path '/root'\n" +
                "    )xx;";

        List<?>[] expected = new List<?>[] {
            Arrays.asList(102022, "102022"),
        };

        process(sql, expected);
    }

    @Test public void testLateralJoinMixedFromClause() throws Exception {
        String ddl = "create view v as select 1 a, 2 b;";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "x", "views");

        String sql = "select x4.*,x3.*,x2.*,x1.* from v x1, table(select x1.a a) x2 join v x3 on x2.a=x3.a join v x4 on x4.a=x3.a";
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(caps));
        HardcodedDataManager dataManager = new HardcodedDataManager();
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1,2,1,2,1,1,2)});

        sql = "select x4.*,x3.*,x2.*,x1.* from views.v x1, xmltable('/a' PASSING xmlparse(document '<a id=\"' || x1.a || '\"/>') COLUMNS a integer PATH '@id') x2 join views.v x3 on x2.a=x3.a join views.v x4 on x4.a=x3.a";
        plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(caps));
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1,2,1,2,1,1,2)});
    }

    @Test public void testXmlTableStreamingWithLimit() throws Exception {
        TestDQPCore tester = new TestDQPCore();
        tester.setUp();
        String sql = "select * from xmltable('/a/b' passing xmlparse(document '<a x=''1''><b>foo</b><b>bar</b><b>zed</b></a>') columns y string path '.') as x limit 2"; //$NON-NLS-1$

        ResultsMessage rm = tester.execute("A", 1, tester.exampleRequestMessage(sql));
        assertNull(rm.getException());
        assertEquals(2, rm.getResultsList().size());
        tester.tearDown();
    }

    @Test public void testXmlExistsCost() throws Exception {
        String critString = "xmlexists('/a/b' passing pm1.g1.e1)"; //$NON-NLS-1$
        TestCalculateCostUtil.helpTestEstimateCost(critString, 1000, 1000, RealMetadataFactory.example1Cached());
    }

}
