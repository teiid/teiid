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

package org.teiid.query.function;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestXMLFunctionLibrary {

    private TestFunctionLibrary tester = new TestFunctionLibrary();

    @Before public void before() {
        tester.setUp();
    }

    @After public void after() {
        tester.tearDown();
    }

    @Test public void testInvokeXpath1() {
        tester.helpInvokeMethod("xpathValue",  //$NON-NLS-1$
                         new Object[] {
                                       "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>", //$NON-NLS-1$
                                       "a/b/c"}, //$NON-NLS-1$
                         "test"); //$NON-NLS-1$
    }

    @Test public void testInvokeXpathWithNill() {
        tester.helpInvokeMethod("xpathValue",  //$NON-NLS-1$
                         new Object[] {
                                       "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><b xsi:nil=\"true\"/></a>", //$NON-NLS-1$
                                       "//*[local-name()='b' and not(@*[local-name()='nil' and string()='true'])]"}, //$NON-NLS-1$
                         null);
    }

    @Test public void testInvokeXpathWithNill1() {
        tester.helpInvokeMethod("xpathValue",  //$NON-NLS-1$
                         new Object[] {
                                       "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b>value</b></a>", //$NON-NLS-1$
                                       "//*[local-name()='b' and not(@*[local-name()='nil' and string()='true'])]"}, //$NON-NLS-1$
                         "value"); //$NON-NLS-1$
    }

    @Test public void testInvokeXslTransform() throws Exception {
        CommandContext c = new CommandContext();
        c.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
        ClobType result = (ClobType)tester.helpInvokeMethod("xsltransform", new Class<?>[] {DataTypeManager.DefaultDataClasses.XML, DataTypeManager.DefaultDataClasses.XML},
                new Object[] {DataTypeManager.transformValue("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Catalog><Items><Item ItemID=\"001\"><Name>Lamp</Name><Quantity>5</Quantity></Item></Items></Catalog></Catalogs>", DataTypeManager.DefaultDataClasses.XML),
                DataTypeManager.transformValue("<?xml version=\"1.0\" encoding=\"UTF-8\"?><xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"><xsl:template match=\"@*|node()\"><xsl:copy><xsl:apply-templates select=\"@*|node()\"/></xsl:copy></xsl:template><xsl:template match=\"Quantity\"/></xsl:stylesheet>", DataTypeManager.DefaultDataClasses.XML)}, c);

        String xml = ObjectConverterUtil.convertToString(result.getCharacterStream());
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Catalog><Items><Item ItemID=\"001\"><Name>Lamp</Name></Item></Items></Catalog></Catalogs>", xml);
    }

    @Test public void testInvokeXmlConcat() throws Exception {
        CommandContext c = new CommandContext();
        c.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
        XMLType result = (XMLType)tester.helpInvokeMethod("xmlconcat", new Class<?>[] {DataTypeManager.DefaultDataClasses.XML, DataTypeManager.DefaultDataClasses.XML},
                new Object[] {DataTypeManager.transformValue("<bar/>", DataTypeManager.DefaultDataClasses.XML), DataTypeManager.transformValue("<Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Catalog><Items><Item ItemID=\"001\"><Name>Lamp</Name><Quantity>5</Quantity></Item></Items></Catalog></Catalogs>", DataTypeManager.DefaultDataClasses.XML)}, c);

        String xml = ObjectConverterUtil.convertToString(result.getCharacterStream());
        assertEquals("<bar/><Catalogs xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Catalog><Items><Item ItemID=\"001\"><Name>Lamp</Name><Quantity>5</Quantity></Item></Items></Catalog></Catalogs>", xml);
    }

    @Test public void testInvokeXmlComment() throws Exception {
        CommandContext c = new CommandContext();
        c.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
        XMLType result = (XMLType)tester.helpInvokeMethod("xmlcomment", new Class<?>[] {DataTypeManager.DefaultDataClasses.STRING},
                new Object[] {"comment"}, c);

        String xml = ObjectConverterUtil.convertToString(result.getCharacterStream());
        assertEquals("<!--comment-->", xml);
    }



}
