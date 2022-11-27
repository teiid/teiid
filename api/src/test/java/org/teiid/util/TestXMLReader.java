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

package org.teiid.util;

import static org.junit.Assert.*;

import java.io.StringReader;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.stax.StAXSource;

import org.junit.Test;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ObjectConverterUtil;

@SuppressWarnings("nls")
public class TestXMLReader {

    @Test public void testStreaming() throws Exception {
        StringBuilder xmlBuilder = new StringBuilder();
        xmlBuilder.append("<root>");
        for (int i = 0; i < 1000; i++) {
            xmlBuilder.append("<a></a>");
            xmlBuilder.append("<b></b>");
        }
        xmlBuilder.append("</root>");
        String xml = xmlBuilder.toString();

        StAXSource source = new StAXSource(XMLType.getXmlInputFactory().createXMLEventReader(new StringReader(xml)));
        XMLReader is = new XMLReader(source, XMLOutputFactory.newFactory());
        String str = ObjectConverterUtil.convertToString(is);
        assertTrue(str, str.startsWith("<?xml version=\"1.0\""));
        //omit document declaration
        assertEquals(xml, str.substring(str.indexOf("><") + 1));
    }

}
