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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;


public class StringToSQLXMLTransform extends Transform {

    /**
     * This method transforms a value of the source type into a value
     * of the target type.
     * @param value Incoming value of source type
     * @return Outgoing value of target type
     * @throws TransformationException if value is an incorrect input type or
     * the transformation fails
     */
    public Object transformDirect(Object value) throws TransformationException {
        String xml = (String)value;
        Reader reader = new StringReader(xml);
        Type type = isXml(reader);
        XMLType result = new XMLType(new SQLXMLImpl(xml));
        result.setType(type);
        return result;
    }

    public static Type isXml(Reader reader) throws TransformationException {
        Type type = Type.ELEMENT;
        XMLInputFactory inputFactory = XMLType.getXmlInputFactory();
        try{
             XMLStreamReader xmlReader = inputFactory.createXMLStreamReader(reader);
             int event = xmlReader.getEventType();
             if  (event == XMLEvent.START_DOCUMENT && xmlReader.getLocation().getColumnNumber() != 1) {
                 type = Type.DOCUMENT;
             }
             while (xmlReader.hasNext()) {
                 xmlReader.next();
             }
        } catch (Exception e){
              throw new TransformationException(CorePlugin.Event.TEIID10070, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10070));
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
            }
        }
        return type;
    }

    /**
     * Type of the incoming value.
     * @return Source type
     */
    public Class<?> getSourceType() {
        return DataTypeManager.DefaultDataClasses.STRING;
    }

    /**
     * Type of the outgoing value.
     * @return Target type
     */
    public Class<?> getTargetType() {
        return DataTypeManager.DefaultDataClasses.XML;
    }

    @Override
    public boolean isExplicit() {
        return true;
    }

}
