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

package org.teiid.core.types;

import java.io.StringReader;
import java.util.StringTokenizer;

import javax.xml.transform.stream.StreamSource;

import org.teiid.core.types.StandardXMLTranslator;

import junit.framework.TestCase;


/**
 * XML StreamSource Translator.
 */
public class TestXMLStreamSourceTranslator extends TestCase {

    private static final String sourceXML =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +  //$NON-NLS-1$
        "<Books:bookCollection xmlns:Books=\"http://www.metamatrix.com/XMLSchema/DataSets/Books\">\r\n" +  //$NON-NLS-1$
        "   <book isbn=\"0-7356-0877-6\">\r\n" +  //$NON-NLS-1$
        "      <title>After the Gold Rush</title>\r\n" +  //$NON-NLS-1$
        "      <subtitle>Creating a True Profession of Software Engineering</subtitle>\r\n" +  //$NON-NLS-1$
        "      <edition>1</edition>\r\n" +  //$NON-NLS-1$
        "      <authors>\r\n" +  //$NON-NLS-1$
        "         <author>McConnell</author>\r\n" +  //$NON-NLS-1$
        "      </authors>\r\n" +  //$NON-NLS-1$
        "      <publishingInformation>\r\n" +  //$NON-NLS-1$
        "         <publisher>Microsoft Press</publisher>\r\n" +  //$NON-NLS-1$
        "         <publishDate>1999</publishDate>\r\n" +  //$NON-NLS-1$
        "      </publishingInformation>\r\n" +  //$NON-NLS-1$
        "   </book>\r\n" +  //$NON-NLS-1$
        "   <book isbn=\"1-55615-484-4\">\r\n" +  //$NON-NLS-1$
        "      <title>Code Complete</title>\r\n" +  //$NON-NLS-1$
        "      <subtitle>A Practical Handbook of Software Construction</subtitle>\r\n" +  //$NON-NLS-1$
        "      <edition>1</edition>\r\n" +  //$NON-NLS-1$
        "      <authors>\r\n" +  //$NON-NLS-1$
        "         <author>McConnell</author>\r\n" +  //$NON-NLS-1$
        "      </authors>\r\n" +  //$NON-NLS-1$
        "      <publishingInformation>\r\n" +  //$NON-NLS-1$
        "         <publisher>Microsoft Press</publisher>\r\n" +  //$NON-NLS-1$
        "         <publishDate>1993</publishDate>\r\n" +  //$NON-NLS-1$
        "      </publishingInformation>\r\n" +  //$NON-NLS-1$
        "   </book>\r\n" +  //$NON-NLS-1$
        "   <book isbn=\"1-556-15900-5\">\r\n" +  //$NON-NLS-1$
        "      <title>Rapid Development</title>\r\n" +  //$NON-NLS-1$
        "      <subtitle>Taming Wild Software Schedules</subtitle>\r\n" +  //$NON-NLS-1$
        "      <edition>1</edition>\r\n" +  //$NON-NLS-1$
        "      <authors>\r\n" +  //$NON-NLS-1$
        "         <author>McConnell</author>\r\n" +  //$NON-NLS-1$
        "      </authors>\r\n" +  //$NON-NLS-1$
        "      <publishingInformation>\r\n" +  //$NON-NLS-1$
        "         <publisher>Microsoft Press</publisher>\r\n" +  //$NON-NLS-1$
        "         <publishDate>1996</publishDate>\r\n" +  //$NON-NLS-1$
        "      </publishingInformation>\r\n" +  //$NON-NLS-1$
        "   </book>\r\n" + //$NON-NLS-1$
        "</Books:bookCollection>"; //$NON-NLS-1$

    public void testStreamSourceWithStream() throws Exception {
        StandardXMLTranslator translator = new StandardXMLTranslator(new StreamSource(new StringReader(sourceXML)));
        compareDocuments(sourceXML, translator.getString());
    }

    private void compareDocuments(String expectedDoc, String actualDoc) {
        StringTokenizer tokens1 = new StringTokenizer(expectedDoc, "\r\n"); //$NON-NLS-1$
        StringTokenizer tokens2 = new StringTokenizer(actualDoc, "\n");//$NON-NLS-1$
        while(tokens1.hasMoreTokens()){
            String token1 = tokens1.nextToken().trim();
            if(!tokens2.hasMoreTokens()){
                fail("XML doc mismatch: expected=" + token1 + "\nactual=none");//$NON-NLS-1$ //$NON-NLS-2$
            }
            String token2 = tokens2.nextToken().trim();
            assertEquals("XML doc mismatch: ", token1, token2); //$NON-NLS-1$
        }
        if(tokens2.hasMoreTokens()){
            fail("XML doc mismatch: expected=none\nactual=" + tokens2.nextToken().trim());//$NON-NLS-1$
        }
    }
}
