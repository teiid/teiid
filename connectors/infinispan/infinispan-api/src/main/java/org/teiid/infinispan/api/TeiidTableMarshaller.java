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
package org.teiid.infinispan.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.RawProtobufMarshaller;
import org.infinispan.protostream.impl.ByteArrayOutputStreamEx;
import org.infinispan.protostream.impl.RawProtoStreamWriterImpl;
import org.teiid.translator.document.Document;

public class TeiidTableMarshaller implements RawProtobufMarshaller<InfinispanDocument>, Serializable {
    private static final long serialVersionUID = 6540991524742624955L;
    private String documentName;
    private TreeMap<Integer, TableWireFormat> wireMap = new TreeMap<>();

    public TeiidTableMarshaller(String docName, TreeMap<Integer, TableWireFormat> wireMap) {
        this.documentName= docName;
        this.wireMap = wireMap;
    }

    @Override
    public String getTypeName() {
        return this.documentName;
    }

    // Read from ISPN Types >> Teiid Types
    @Override
    public InfinispanDocument readFrom(ImmutableSerializationContext ctx, RawProtoStreamReader in) throws IOException {
        InfinispanDocument row = new InfinispanDocument(this.documentName, this.wireMap, null);
        readDocument(in, row, this.wireMap);
        return row;
    }

    // Write from Teiid Types >> ISPN Types
    @Override
    public void writeTo(ImmutableSerializationContext ctx, RawProtoStreamWriter out, InfinispanDocument document)
            throws IOException {
        TreeMap<Integer, TableWireFormat> wireMap = document.getWireMap();
        for (Entry<Integer, TableWireFormat> entry : wireMap.entrySet()) {
            TableWireFormat twf = entry.getValue();

            if (twf == null) {
                throw new IOException("Error in wireformat");
            }

            int tag = twf.getWriteTag();

            if (twf.isNested()) {
                List<? extends Document> children = document.getChildDocuments(twf.getAttributeName());
                if (children != null) {
                    for (Document d : children) {
                        ByteArrayOutputStreamEx baos = new ByteArrayOutputStreamEx();
                        RawProtoStreamWriter rpsw = RawProtoStreamWriterImpl.newInstance(baos);
                        writeTo(ctx, rpsw, (InfinispanDocument)d);
                        rpsw.flush();
                        baos.flush();
                        // here readtag because this is inner object, even other one uses write tag but calculated
                        // based on the write operation used.
                        out.writeBytes(tag, baos.getByteBuffer());
                    }
                }
                continue;
            }

            Object value = document.getProperties().get(twf.getAttributeName());
            if (value == null) {
                continue;
            }

            ArrayList<Object> values = null;
            boolean array = twf.isArrayType();
            if (array) {
                values = (ArrayList<Object>)value;
            }

            switch (twf.getProtobufType()) {
            case DOUBLE:
                if (array) {
                    for (Object o:values) {
                        out.writeDouble(tag, ProtobufDataManager.convertToInfinispan(Double.class, o));
                    }
                } else {
                    out.writeDouble(tag, ProtobufDataManager.convertToInfinispan(Double.class, value));
                }
                break;
            case FLOAT:
                if (array) {
                    for (Object o:values) {
                        out.writeFloat(tag, ProtobufDataManager.convertToInfinispan(Float.class, o));
                    }
                } else {
                    out.writeFloat(tag, ProtobufDataManager.convertToInfinispan(Float.class, value));
                }
                break;
            case BOOL:
                if (array) {
                    for (Object o:values) {
                        out.writeBool(tag, ProtobufDataManager.convertToInfinispan(Boolean.class, o));
                    }
                } else {
                    out.writeBool(tag, ProtobufDataManager.convertToInfinispan(Boolean.class, value));
                }
                break;
            case STRING:
                if (array) {
                    for (Object o:values) {
                        out.writeString(tag, ProtobufDataManager.convertToInfinispan(String.class, o));
                    }
                } else {
                    out.writeString(tag, ProtobufDataManager.convertToInfinispan(String.class, value));
                }
                break;
            case BYTES:
                if (array) {
                    for (Object o:values) {
                        out.writeBytes(tag, ProtobufDataManager.convertToInfinispan(byte[].class, o));
                    }
                } else {
                    out.writeBytes(tag, ProtobufDataManager.convertToInfinispan(byte[].class, value));
                }
                break;
            case INT32:
                if (array) {
                    for (Object o:values) {
                        out.writeInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case SFIXED32:
                if (array) {
                    for (Object o:values) {
                        out.writeSFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeSFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case FIXED32:
                if (array) {
                    for (Object o:values) {
                        out.writeFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeFixed32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case UINT32:
                if (array) {
                    for (Object o:values) {
                        out.writeUInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeUInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case SINT32:
                if (array) {
                    for (Object o:values) {
                        out.writeSInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, o));
                    }
                } else {
                    out.writeSInt32(tag, ProtobufDataManager.convertToInfinispan(Integer.class, value));
                }
                break;
            case INT64:
                if (array) {
                    for (Object o:values) {
                        out.writeInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case UINT64:
                if (array) {
                    for (Object o:values) {
                        out.writeUInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeUInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case FIXED64:
                if (array) {
                    for (Object o:values) {
                        out.writeFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case SFIXED64:
                if (array) {
                    for (Object o:values) {
                        out.writeSFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeSFixed64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            case SINT64:
                if (array) {
                    for (Object o:values) {
                        out.writeSInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, o));
                    }
                } else {
                    out.writeSInt64(tag, ProtobufDataManager.convertToInfinispan(Long.class, value));
                }
                break;
            default:
                throw new IOException("Unexpected field type : " + twf.getProtobufType());
            }
        }
    }

    static void readDocument(RawProtoStreamReader in, InfinispanDocument document,
            TreeMap<Integer, TableWireFormat> columnMap) throws IOException {

        while (true) {
            int tag = in.readTag();
            if (tag == 0) {
                break;
            }
            TableWireFormat twf = columnMap.get(tag);
            if (twf == null) {
                throw new IOException("Error in wireformat");
            }

            if (twf.isNested()) {
                InfinispanDocument child = new InfinispanDocument(twf.getAttributeName(), twf.getNestedWireMap(), document);
                int length = in.readRawVarint32();
                int oldLimit = in.pushLimit(length);
                readDocument(in, child, twf.getNestedWireMap());
                document.addChildDocument(twf.getAttributeName(), child);
                in.checkLastTagWas(0);
                in.popLimit(oldLimit);
                continue;
            }

            switch (twf.getProtobufType()) {
            case DOUBLE:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readDouble()));
                break;
            case FLOAT:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readFloat()));
                break;
            case BOOL:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(),in.readBool()));
                break;
            case STRING:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readString()));
                break;
            case BYTES:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readByteArray()));
                break;
            case INT32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readInt32()));
                break;
            case SFIXED32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSFixed32()));
                break;
            case FIXED32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readFixed32()));
                break;
            case UINT32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readUInt32()));
                break;
            case SINT32:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSInt32()));
                break;
            case INT64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readInt64()));
                break;
            case UINT64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readUInt64()));
                break;
            case FIXED64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readFixed64()));
                break;
            case SFIXED64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSFixed64()));
                break;
            case SINT64:
                document.addProperty(tag, ProtobufDataManager.convertToRuntime(twf.expectedType(), in.readSInt64()));
                break;
            default:
                throw new IOException("Unexpected field type : " + twf.getProtobufType());
            }
        }
    }

    @Override
    public Class getJavaClass() {
        return InfinispanDocument.class;
    }

}
