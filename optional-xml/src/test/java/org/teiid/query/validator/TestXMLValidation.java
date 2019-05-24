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

package org.teiid.query.validator;

import static org.teiid.query.validator.TestValidator.*;

import org.junit.Test;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestXMLValidation {

    @Test public void testXMLQueryPassingContextType() {
        helpValidate("select xmlquery('/' passing 2)", new String[] {"XMLQUERY('/' PASSING 2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXpathValueValid_defect15088() {
        String userSql = "SELECT xpathValue('<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>', 'a/b/c')"; //$NON-NLS-1$
        helpValidate(userSql, new String[] {}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testXpathValueInvalid_defect15088() throws Exception {
        String userSql = "SELECT xpathValue('<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>', '//*[local-name()=''bookName\"]')"; //$NON-NLS-1$
        helpValidate(userSql, new String[] {"xpathValue('<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>', '//*[local-name()=''bookName\"]')"}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testXMLTablePassingMultipleContext() {
        helpValidate("select * from pm1.g1, xmltable('/' passing xmlparse(DOCUMENT '<a/>'), xmlparse(DOCUMENT '<b/>')) as x", new String[] {"XMLTABLE('/' PASSING XMLPARSE(DOCUMENT '<a/>'), XMLPARSE(DOCUMENT '<b/>')) AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testValidateNoExpressionName() {
        helpValidate("SELECT xmlelement(name a, xmlattributes('1'))", new String[] {"XMLATTRIBUTES('1')"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidCorrelation() {
        helpValidate("SELECT XMLELEMENT(NAME metadata, XMLFOREST(e1 AS objectName), (SELECT XMLAGG(XMLELEMENT(NAME subTypes, XMLFOREST(e1))) FROM pm1.g2 AS b WHERE b.e2 = a.e2)) FROM pm1.g1 AS a GROUP BY e1", new String[] {"a.e2"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXMLNamespacesReserved() {
        helpValidate("select xmlforest(xmlnamespaces('http://foo' as xmlns), e1 as \"table\") from pm1.g1", new String[] {"XMLNAMESPACES('http://foo' AS xmlns)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXmlNameValidation() throws Exception {
        helpValidate("select xmlelement(\":\")", new String[] {"XMLELEMENT(NAME \":\")"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionWithNestedOrdering() {
        helpValidate("SELECT xmlagg(xmlelement(name x, e1) order by e2) over () from pm1.g1", new String[] {"XMLAGG(XMLELEMENT(NAME x, e1) ORDER BY e2) OVER ()"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXMLTableContextRequired() {
        helpValidate("select * from xmltable('/a/b' passing convert('<a/>', xml) as a columns x for ordinality, c integer path '.') as x", new String[] {"XMLTABLE('/a/b' PASSING convert('<a/>', xml) AS a COLUMNS x FOR ORDINALITY, c integer PATH '.') AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testInvalidDefault() {
        helpValidate("select * from pm1.g1, xmltable('/' passing xmlparse(DOCUMENT '<a/>') columns y string default 'a', x string default (select e1 from pm1.g1)) as x", new String[] {"XMLTABLE('/' PASSING XMLPARSE(DOCUMENT '<a/>') COLUMNS y string DEFAULT 'a', x string DEFAULT (SELECT e1 FROM pm1.g1)) AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXMLTableMultipleOrdinals() {
        helpValidate("select * from pm1.g1, xmltable('/' passing XMLPARSE(DOCUMENT '<a/>') columns x for ordinality, y for ordinality) as x", new String[] {"XMLTABLE('/' PASSING XMLPARSE(DOCUMENT '<a/>') COLUMNS x FOR ORDINALITY, y FOR ORDINALITY) AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXMLTablePassingContextType() {
        helpValidate("select * from pm1.g1, xmltable('/' passing 2) as x", new String[] {"XMLTABLE('/' PASSING 2) AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXMLNamespaces() {
        helpValidate("select xmlforest(xmlnamespaces(no default, default 'http://foo'), e1 as \"table\") from pm1.g1", new String[] {"XMLNAMESPACES(NO DEFAULT, DEFAULT 'http://foo')"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXMLNamespacesInvalid() {
        helpValidate("select xmlforest(xmlnamespaces('http://foo' as \"1\"), e1 as \"table\") from pm1.g1", new String[] {"XMLNAMESPACES('http://foo' AS \"1\")"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testValidateAlterViewDeep() {
        TestValidator.helpValidate("alter view Defect15355 as select xpathvalue('a', ':'), cast(1 as biginteger)", new String[] {"xpathvalue('a', ':')"}, RealMetadataFactory.exampleBQTCached());
    }

}
