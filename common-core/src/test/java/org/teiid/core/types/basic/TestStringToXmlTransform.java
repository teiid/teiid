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

package org.teiid.core.types.basic;

import static org.junit.Assert.*;

import java.sql.SQLXML;

import org.junit.Test;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;


@SuppressWarnings("nls")
public class TestStringToXmlTransform {

    @Test public void testGoodXML() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><customer>\n" + //$NON-NLS-1$
                        "<name>ABC</name>" + //$NON-NLS-1$
                        "<age>32</age>" + //$NON-NLS-1$
                     "</customer>"; //$NON-NLS-1$

       StringToSQLXMLTransform transform = new StringToSQLXMLTransform();

       SQLXML xmlValue = (SQLXML)transform.transformDirect(xml);
       assertEquals(xml.replaceAll("[\r]", ""), xmlValue.getString().replaceAll("[\r]", ""));
    }

    @Test public void testGoodElement() throws Exception {
        String xml = "<customer>\n" + //$NON-NLS-1$
                        "<name>ABC</name>" + //$NON-NLS-1$
                        "<age>32</age>" + //$NON-NLS-1$
                     "</customer>"; //$NON-NLS-1$

       StringToSQLXMLTransform transform = new StringToSQLXMLTransform();

       XMLType xmlValue = (XMLType)transform.transformDirect(xml);
       assertEquals(xml.replaceAll("[\r]", ""), xmlValue.getString().replaceAll("[\r]", ""));
       assertEquals(XMLType.Type.ELEMENT, xmlValue.getType());
    }

    @Test(expected=TransformationException.class) public void testBadXML() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><customer>\n" + //$NON-NLS-1$
                        "<name>ABC</name>" + //$NON-NLS-1$
                        "<age>32</age>" + //$NON-NLS-1$
                     "<customer>"; //$NON-NLS-1$ (********no ending)

       StringToSQLXMLTransform transform = new StringToSQLXMLTransform();

       transform.transformDirect(xml);
    }

}
