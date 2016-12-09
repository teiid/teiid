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