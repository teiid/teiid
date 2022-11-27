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
package org.teiid.translator.odata;
import java.io.Reader;
import java.io.StringWriter;

import org.odata4j.core.OError;
import org.odata4j.core.OErrors;
import org.odata4j.format.FormatParser;
import org.odata4j.format.xml.XmlFormatParser;
import org.odata4j.stax2.QName2;
import org.odata4j.stax2.XMLEvent2;
import org.odata4j.stax2.XMLEventReader2;
import org.odata4j.stax2.XMLWriter2;
import org.odata4j.stax2.util.StaxUtil;

@SuppressWarnings("nls")
// TEIID-2759 - allowing to wrap the innererror with root element
public class AtomErrorFormatParser extends XmlFormatParser implements FormatParser<OError> {

  private static final QName2 ERROR = new QName2(NS_METADATA, "error");
  private static final QName2 CODE = new QName2(NS_METADATA, "code");
  private static final QName2 MESSAGE = new QName2(NS_METADATA, "message");
  private static final QName2 INNER_ERROR = new QName2(NS_METADATA, "innererror");

  @Override
  public OError parse(Reader reader) {
    String code = null;
    String message = null;
    String innerError = null;
    XMLEventReader2 xmlReader = StaxUtil.newXMLEventReader(reader);
    XMLEvent2 event = xmlReader.nextEvent();
    while (!event.isStartElement())
      event = xmlReader.nextEvent();
    if (!isStartElement(event, ERROR))
      throw new RuntimeException("Bad error response: <" + ERROR.getLocalPart() + "> not found");
    while (!isEndElement(event = xmlReader.nextEvent(), ERROR)) {
      if (isStartElement(event, CODE))
        code = xmlReader.getElementText();
      else if (isStartElement(event, MESSAGE))
        message = xmlReader.getElementText();
      else if (isStartElement(event, INNER_ERROR))
        innerError = StaxUtil.outerXml(event, xmlReader);
      else if (!event.isStartElement() || !event.isEndElement())
        continue;
      else
        throw new RuntimeException("Bad error response: Unexpected structure");
    }
    if (!isEndElement(event, ERROR))
      throw new RuntimeException("Bad error response: Expected </" + ERROR.getLocalPart() + ">");
    if (code == null && message == null && innerError == null)
      throw new RuntimeException("Bad error response: Unknown elements");
    return OErrors.error(code, message, innerError);
  }
}