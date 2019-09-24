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
package org.teiid.translator.swagger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.Expression;
import org.teiid.language.Literal;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.Document;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

public class JsonSerializer implements SwaggerSerializer {

    @Override
    public List<Document> deserialize(InputStream stream)
            throws TranslatorException {
        try {
            JsonParser parser = new JsonFactory().createParser(stream);
            List<Document> list = null;
            Document current = null;
            Stack<String> fieldName = new Stack<String>();
            int arrayLevel = 0;
            int objectLevel = 0;

            while (parser.nextToken() != null) {
                switch (parser.getCurrentToken()) {
                case START_OBJECT:
                    objectLevel++;
                    if (current == null) {
                        current = new Document(); // this is root
                    } else {
                        Document child = new Document(fieldName.peek(), (arrayLevel >= objectLevel), current);
                        current.addChildDocument(fieldName.peek(), child);
                        current = child;
                    }
                    break;
                case END_OBJECT:
                    objectLevel--;
                    if (list != null && current.getParent() == null) {
                        list.add(current);
                        current = null;
                    } else {
                        Document parent = current.getParent();
                        if (parent != null) {
                            current = parent;
                        }
                    }
                    break;
                case START_ARRAY:
                    arrayLevel++;
                    if (current == null && list == null) {
                        list = new ArrayList<Document>(); // root document is a list
                    }
                    break;
                case END_ARRAY:
                    if (arrayLevel > objectLevel && !fieldName.empty()) {
                        fieldName.pop();
                    }
                    arrayLevel--;
                    break;
                case FIELD_NAME:
                    fieldName.push(parser.getCurrentName());
                    break;
                case VALUE_STRING:
                    if ((list != null && arrayLevel > objectLevel)
                            || (list == null && arrayLevel >= objectLevel)) {
                        current.addArrayProperty(fieldName.peek(), parser.getValueAsString());
                    } else {
                        current.addProperty(fieldName.pop(), parser.getValueAsString());
                    }
                    break;
                case VALUE_NUMBER_INT:
                    if ((list != null && arrayLevel > objectLevel)
                            || (list == null && arrayLevel >= objectLevel)) {
                        current.addArrayProperty(fieldName.peek(), parser.getValueAsLong());
                    } else {
                    current.addProperty(fieldName.pop(), parser.getValueAsLong());
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    if ((list != null && arrayLevel > objectLevel)
                            || (list == null && arrayLevel >= objectLevel)) {
                        current.addArrayProperty(fieldName.peek(), parser.getValueAsDouble());
                    } else {
                        current.addProperty(fieldName.pop(), parser.getValueAsDouble());
                    }
                    break;
                case VALUE_TRUE:
                    if ((list != null && arrayLevel > objectLevel)
                            || (list == null && arrayLevel >= objectLevel)) {
                        current.addArrayProperty(fieldName.peek(), parser.getValueAsBoolean());
                    } else {
                        current.addProperty(fieldName.pop(), parser.getValueAsBoolean());
                    }
                    break;
                case VALUE_FALSE:
                    if ((list != null && arrayLevel > objectLevel)
                            || (list == null && arrayLevel >= objectLevel)) {
                        current.addArrayProperty(fieldName.peek(), parser.getValueAsBoolean());
                    } else {
                        current.addProperty(fieldName.pop(), parser.getValueAsBoolean());
                    }
                    break;
                case VALUE_NULL:
                    if ((list != null && arrayLevel > objectLevel)
                            || (list == null && arrayLevel >= objectLevel)) {
                        current.addArrayProperty(fieldName.peek(), null);
                    } else {
                        current.addProperty(fieldName.pop(), null);
                    }
                    break;

                default:
                    break;
                }
            }
            return list == null?Arrays.asList(current):list;
        } catch (JsonParseException e) {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28007,
                    SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28007, e));
        } catch (IOException e) {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28007,
                    SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28007, e));
        }
    }

    @Override
    public InputStream serialize(Document doc) throws TranslatorException {
        ByteArrayOutputStream outputStream = null;
        try {
            outputStream = new ByteArrayOutputStream(1024*10);
            JsonGenerator json = new JsonFactory().createGenerator(outputStream);
            writeDocument(doc, null, json, false);
            json.close();
            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (IOException | SQLException e) {
            throw new TranslatorException(e);
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    // ignore.
                }
            }
        }
    }

    private void writeDocument(Document doc, String name, JsonGenerator json, boolean writeName)
            throws IOException, SQLException {
        if (doc.getProperties().isEmpty() && doc.getChildren().isEmpty()) {
            return;
        }

        if (writeName) {
            json.writeObjectFieldStart(name);
        } else {
            json.writeStartObject();
        }

        for (Map.Entry<String, Object> entry:doc.getProperties().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.startsWith(name+"/")) {
                key = key.substring(name.length()+1);
            }

            if (value instanceof org.teiid.language.Array) {
                json.writeArrayFieldStart(key);
                org.teiid.language.Array array = (org.teiid.language.Array)value;
                for (Expression expr:array.getExpressions()) {
                    writeProperty(json, ((Literal)expr).getValue());
                }
                json.writeEndArray();
            } else {
                writeProperty(json, key, value);
            }
        }
        if (!doc.getChildren().isEmpty()) {
            for (Map.Entry<String, List<Document>> entry:doc.getChildren().entrySet()) {
                String docName = entry.getKey();
                List<Document> children = entry.getValue();
                boolean array = children.get(0).isArray();
                if (array) {
                    json.writeArrayFieldStart(docName);
                    for (Document child:children) {
                        writeDocument(child, docName, json, false);
                    }
                    json.writeEndArray();
                } else {
                    writeDocument(children.get(0), docName, json, true);
                }
            }
        }
        json.writeEndObject();
    }

    private void writeProperty(JsonGenerator json, Object value)
            throws IOException, SQLException {
        if (value instanceof Integer) {
            json.writeNumber((Integer)value);
        } else if (value instanceof Long) {
            json.writeNumber((Long)value);
        } else if (value instanceof Float) {
            json.writeNumber((Float)value);
        } else if (value instanceof Double) {
            json.writeNumber((Double)value);
        } else if (value instanceof Byte) {
            json.writeNumber((Byte)value);
        } else if (value instanceof Boolean) {
            json.writeBoolean((Boolean)value);
        } else if (value instanceof Timestamp) {
            json.writeString(SwaggerTypeManager.timestampToString((Timestamp)value));
        } else if (value instanceof Date) {
            json.writeString(SwaggerTypeManager.dateToString((Date)value));
        } else if (value instanceof ClobType)  {
            // no LOB types in swagger spec ???
            json.writeString(ClobType.getString((Clob) value));
        } else if (value instanceof BlobType) {
            json.writeString(Base64.encodeBytes(
                    ObjectConverterUtil.convertToByteArray(((Blob) value).getBinaryStream())));
        } else if (value instanceof SQLXML) {
            json.writeString(ObjectConverterUtil.convertToString(((SQLXML) value).getCharacterStream()));
        } else if (value instanceof byte[] ) {
            json.writeString(Base64.encodeBytes((byte[])value));
        } else {
            json.writeString(value.toString());
        }
    }

    private void writeProperty(JsonGenerator json, String key, Object value)
            throws IOException, SQLException {
        if (value instanceof Integer) {
            json.writeNumberField(key,(Integer)value);
        } else if (value instanceof Long) {
            json.writeNumberField(key,(Long)value);
        } else if (value instanceof Float) {
            json.writeNumberField(key,(Float)value);
        } else if (value instanceof Double) {
            json.writeNumberField(key,(Double)value);
        } else if (value instanceof Byte) {
            json.writeNumberField(key,(Byte)value);
        } else if (value instanceof Boolean) {
            json.writeBooleanField(key,(Boolean)value);
        } else if (value instanceof Timestamp) {
            json.writeStringField(key, SwaggerTypeManager.timestampToString((Timestamp)value));
        } else if (value instanceof Date) {
            json.writeStringField(key, SwaggerTypeManager.dateToString((Date)value));
        } else if (value instanceof ClobType)  {
            // no LOB types in swagger spec ???
            json.writeStringField(key, ClobType.getString((Clob) value));
        } else if (value instanceof BlobType) {
            json.writeStringField(key, Base64.encodeBytes(
                    ObjectConverterUtil.convertToByteArray(((Blob) value).getBinaryStream())));
        } else if (value instanceof SQLXML) {
            json.writeStringField(key,
                    ObjectConverterUtil.convertToString(((SQLXML) value).getCharacterStream()));
        } else if (value instanceof byte[] ) {
            json.writeStringField(key, Base64.encodeBytes((byte[])value));
        } else {
            json.writeStringField(key, value.toString());
        }
    }
}
